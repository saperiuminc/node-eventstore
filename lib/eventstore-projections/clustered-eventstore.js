const murmurhash = require('murmurhash');
const Bluebird = require('bluebird');
const _ = require('lodash');
const debug = require('debug')('eventstore:clustered');
const TaskAssignmentGroup = require('./task-assignment').TaskAssignmentGroup;
const EventstoreWithProjection = require('./eventstore-projection');

class ClusteredEventStore extends EventstoreWithProjection {
    constructor(opts, store, distributedSignal, distributedLock, playbackListStore, playbackListViewStore, projectionStore, stateListStore) {
        super(opts, store, distributedSignal, distributedLock, playbackListStore, playbackListViewStore, projectionStore, stateListStore);

        const defaults = {
            partitions: 25
        };

        this._options = _.defaults({}, opts, defaults);
        this._options.numberOfShards = this._options.clusters.length;

        this._eventstoreCount = 0;
        this._eventstores = [];
        this._distributedLock = distributedLock;
    }

    addEventstoreToCluster(eventstoreNode) {
        this._eventstores.push(eventstoreNode);
        this._eventstoreCount++;
        Bluebird.promisifyAll(eventstoreNode);
    }

    /**
     * Call this function to initialize the eventstore.
     * If an event publisher function was injected it will additionally initialize an event dispatcher.
     * @param {Function} callback the function that will be called when this action has finished [optional]
     */
    init(callback) {
        this._doOnAllEventstores('init', callback);
        this._listenAndForwardEmitOfAllEventstores();
    }

    /**
     * @param {DoneCallbackFunction} callback success/error callback
     * @returns {void} - returns void
     */
    startAllProjections(callback) {
        const self = this;
        const promises = [];

        for (const eventstore of self._eventstores) {
            promises.push(eventstore['startAllProjectionsAsync'].apply(eventstore, {}));
        }

        // Note: Convert to Async with _startAllProjections
        Promise.all(promises)
        .then(function() {
            const projectionTaskPromises = [];
            for (const eventstore of self._eventstores) {
                projectionTaskPromises.push(eventstore.getProjectionTasksAsync());
            }
            return Promise.all(projectionTaskPromises);
        })
        .then((promiseAllResponse) => {
            let allProjectionTasksInAllShards = [];
            for (const projectionTasksOfAShard of promiseAllResponse) {
              allProjectionTasksInAllShards = allProjectionTasksInAllShards.concat(projectionTasksOfAShard);
            }

            const filterByDictionary = {};
            let allProjectionTaskIdsInAllShards = [];
            for(const projectionTask of allProjectionTasksInAllShards) {
                filterByDictionary[projectionTask.projectionTaskId] = projectionTask.projectionTaskId;
            }
            allProjectionTaskIdsInAllShards = Object.values(filterByDictionary);

            return self._doTaskAssignment(self._options.projectionGroup, allProjectionTaskIdsInAllShards);
        })
        .then(function(...data) {
            callback(null, ...data);
        })
        .catch((err) => {
            console.error(err);
            callback();
        });
    }

    async _doTaskAssignment(projectionGroup, allProjectionTaskIdsInAllShards) {
        const self = this;
        const tasks = [];

        for (let projectionTaskId of allProjectionTaskIdsInAllShards) {
            tasks.push(projectionTaskId);
        }

        // create TaskAssignmentGroup
        console.log('Clustered _doTaskAssignment');
        self._taskGroup = new TaskAssignmentGroup({
            initialTasks: tasks,
            createRedisClient: self._options.redisCreateClient,
            groupId: projectionGroup,
            membershipPollingTimeout: self._options.membershipPollingTimeout,
            distributedLock: self._distributedLock
        });
    
        await self._taskGroup.join();
        self._taskGroup.on('rebalance', async (updatedAssignments, rebalanceId) => {
            console.log('REBALANCE CALLED', updatedAssignments);
            // distribute to shards
            const distributedAssignments = {};
            for(const projectionTaskId of updatedAssignments) {
                const splitProjectionTaskId = projectionTaskId.split(':');
                if (splitProjectionTaskId.length > 1 && splitProjectionTaskId[1]) {
                    const shardString = splitProjectionTaskId[1];
                    const stringIndex = shardString.replace('shard', '');
                    const shard = parseInt(stringIndex);
                    if (!distributedAssignments[shard]) {
                        distributedAssignments[shard] = [];
                    }
                    distributedAssignments[shard].push(projectionTaskId);
                }
            }
            const distributedAssignmentsLength = Object.keys(distributedAssignments).length;
            if(distributedAssignmentsLength > 0) {
                for (const eventstore of this._eventstores) {
                    const updatedEventStoreAssignment = distributedAssignments[eventstore.options.shard];
                    if (updatedEventStoreAssignment) {
                        console.log('ES REBALANCE CALLED', updatedEventStoreAssignment);
                        eventstore.rebalance(updatedEventStoreAssignment, rebalanceId);
                    } else {
                        // NOTE: still update but with empty assignment
                        eventstore.rebalance([], rebalanceId);
                    }
                }
            }
        });
    };

    /**
     * @param {ProjectionConfiguration} projectionConfig parameters for the projection
     * @param {Function} callback success/error callback
     * @returns {void} - returns void
     */
    project(projectionConfig, callback) {
        // this._project(projectionConfig).then((data) => {
        //     callback(null, data);
        // }).catch(callback);
        this._doOnAllEventstores('project', projectionConfig, callback);
    }

    // _getPartitionsAsync(eventstore) {
    //     return new Promise((resolve, reject) => {
    //         eventstore.store.getPartitions(function(err, partitions) {
    //             if (err) {
    //                 reject(err)
    //             } else {
    //                 resolve(partitions);
    //             }
    //         })
    //     });
    // }

    // async _project(projectionConfig) {
    //     const promises = [];
    //     for (const eventstore of this._eventstores) {
    //         promises.push(eventstore.projectAsync(projectionConfig));
    //     }
    //     await Promise.all(promises);
    // }

    /**
     * @param {DoneCallbackFunction} callback success/error callback
     * @returns {void} - returns void
     */
    stopAllProjections(callback) {
        const self = this;
        const promises = [];
        for (const eventstore of this._eventstores) {
            promises.push(eventstore.stopAllProjectionsAsync());
        }

        Promise.all(promises)
        .then(() => {
            if (self._taskGroup) {
                self._taskGroup.leave();
            }
        })
        .then((data) => {
            callback();
        })
        .catch((err) => {
            console.error(err);
            callback();
        });
    }

    /**
     * @param {string} projectionId
     * @returns {Promise}
     */
    runProjection(projectionId, forced, done) {
        this._runProjection(projectionId, forced).then((data) => {
            done(null, data);
        }).catch(done);
    }

    async _runProjection(projectionId, forced) {
        const self = this;
        const promises = [];

        for (const eventstore of this._eventstores) {
            promises.push(eventstore.runProjectionAsync(projectionId, forced));
            // const partitions = await this._getPartitionsAsync(eventstore);
            // for (const partition of partitions) {
            //     let clonedProjectionId = _.clone(projectionId);
            //     clonedProjectionId = `${clonedProjectionId}:shard${eventstore.options.shard}:partition${partition}`;
            //     newTasks.push(clonedProjectionId);
            // }
        }

        const allProjectionTaskIdsInAllShards = await Promise.all(promises);

        console.log('_runProjection: allProjectionTaskIdsInAllShards:', allProjectionTaskIdsInAllShards);

        if (self._taskGroup) {
            for(const allProjectionTaskIdsOfAShard of allProjectionTaskIdsInAllShards) {
                console.log('_runProjection: allProjectionTaskIdsOfAShard:', allProjectionTaskIdsOfAShard);
                self._taskGroup.addTasks(allProjectionTaskIdsOfAShard);
            }
        }
    }

    /**
     * 
     * @param {string} projectionId 
     * @returns {Promise<Projection>}
     */
    // getProjection(projectionId, done) {
    //     this._getProjection(projectionId).then((data) => {
    //         done(null, data);
    //     }).catch(done);
    // }

    // async _getProjection(projectionId) {
    //     const promises = [];

    //     if (this._eventstores && this._eventstores.length > 0) {
    //         return await this._eventstores[0]
    //     }
    //     for (const eventstore of this._eventstores) {
    //         const partitions = await this._getPartitionsAsync(eventstore);
    //         for (const partition of partitions) {
    //             let clonedProjectionId = _.clone(projectionId);
    //             clonedProjectionId = `${clonedProjectionId}:shard${eventstore.options.shard}:partition${partition}`;
    //             promises.push(eventstore.getProjectionAsync(clonedProjectionId));
    //         }
    //     }
        
    //     return await Promise.all(promises);
    // }

    /**
     * @returns {Promise<Array<Projection>} returns Promise of type void
     */
    // getProjections() {
    //     const self = this;
    //     const promises = [];
    //     let args = arguments;
    //     const done = args[args.length - 1];

    //     // NOTE: for review, getting projections of one shard gets all projections of all shards
    //     const eventstore = this._eventstores[0];
    //     promises.push(eventstore['getProjectionsAsync'].apply(eventstore, {}));

    //     Promise.all(promises)
    //     .then((data) => done(null, data))
    //     .catch((err) => {
    //         console.error(err);
    //         done();
    //     });
    // }

    /**
     * @param {string} projectionId
     * @returns {Promise}
     */
    deleteProjection(projectionId, done) {
        this._deleteProjection(projectionId).then((data) => {
            done(null, data);
        }).catch(done);
    }

    async _deleteProjection(projectionId) {
        const self = this;
        const promises = [];

        for (const eventstore of this._eventstores) {
            promises.push(eventstore.deleteProjectionAsync(projectionId));
            // const partitions = await this._getPartitionsAsync(eventstore);
            // for (const partition of partitions) {
            //     let clonedProjectionId = _.clone(projectionId);
            //     clonedProjectionId = `${clonedProjectionId}:shard${eventstore.options.shard}:partition${partition}`;
            //     if(self._taskGroup) {
            //         self._taskGroup.removeTasks([clonedProjectionId]);
            //     }
            //     promises.push(eventstore.deleteProjectionAsync(clonedProjectionId));
            // }
        }
        
        const allProjectionTaskIdsInAllShards = await Promise.all(promises);

        if (self._taskGroup) {
            for(const allProjectionTaskIdsOfAShard of allProjectionTaskIdsInAllShards) {
                console.log('_deleteProjection: allProjectionTaskIdsOfAShard:', allProjectionTaskIdsOfAShard);
                self._taskGroup.removeTasks(allProjectionTaskIdsOfAShard);
            }
        }
    }

    /**
     * @param {string} projectionId
     * @returns {Promise}
     */
    pauseProjection(projectionId, done) {
        this._pauseProjection(projectionId).then((data) => {
            done(null, data);
        }).catch(done);
    }

    async _pauseProjection(projectionId) {
        const promises = [];

        // TODO: Do on All
        for (const eventstore of this._eventstores) {
            promises.push(eventstore.pauseProjectionAsync(projectionId));
            // const partitions = await this._getPartitionsAsync(eventstore);
            // for (const partition of partitions) {
            //     let clonedProjectionId = _.clone(projectionId);
            //     clonedProjectionId = `${clonedProjectionId}:shard${eventstore.options.shard}:partition${partition}`;
            //     promises.push(eventstore.pauseProjectionAsync(clonedProjectionId));
            // }
        }
        
        return await Promise.all(promises);
    }

    /**
     * @param {string} projectionId
     * @returns {Promise}
     */
    resetProjection(projectionId, done) {
        this._resetProjection(projectionId).then((data) => {
            done(null, data);
        }).catch(done);
    }

    async _resetProjection(projectionId) {
        const promises = [];

        // TODO: Do on All
        for (const eventstore of this._eventstores) {
            promises.push(eventstore.resetProjectionAsync(projectionId));
            // const partitions = await this._getPartitionsAsync(eventstore);
            // for (const partition of partitions) {
            //     let clonedProjectionId = _.clone(projectionId);
            //     clonedProjectionId = `${clonedProjectionId}:shard${eventstore.options.shard}:partition${partition}`;
            //     promises.push(eventstore.resetProjectionAsync(clonedProjectionId));
            // }
        }
        
        return await Promise.all(promises);
    }

    /**
     * @param {AggregateQuery} query the query for the aggregate/category that we like to get
     * @param {Number} revision the revision from where we should start the subscription
     * @param {EventCallback} onEventCallback the callback to be called whenever an event is available for the subscription
     * @param {ErrorCallback} onErrorCallback the callback to be called whenever an error is thrown
     * @returns {String} - returns token
     */
    subscribe(query, revision, onEventCallback, onErrorCallback) {
        const aggregateId = query.aggregateId;
        return this._doOnShardedEventstore(aggregateId, 'subscribe', query, revision, onEventCallback, onErrorCallback);
    }

    /**
     * Define which values should be mapped/copied to the payload event. [optional]
     * @param {Object} mappings the mappings in dotty notation
     *                          {
     *                            id: 'id',
     *                            commitId: 'commitId',
     *                            commitSequence: 'commitSequence',
     *                            commitStamp: 'commitStamp',
     *                            streamRevision: 'streamRevision'
     *                          }
     * @returns {Eventstore}  to be able to chain...
     */
    defineEventMappings(mappings) {
        this._doOnAllEventstoresNoCallback('defineEventMappings', mappings);
    }

    /**
     * Inject function for event publishing.
     * @param {Function} fn the function to be injected
     * @returns {Eventstore}  to be able to chain...
     */
    useEventPublisher(fn) {
        // NOTE: callback of useEventPublisher is not a callback when useEventPublisher is finished being called
        // it is a callback for when an event is to be published. 
        // hence we are using the no callback method
        this._doOnAllEventstoresNoCallback('useEventPublisher', fn);
    }

    /**
     * loads the last event
     * @param {Object || String} query    the query object [optional]
     * @param {Function}         callback the function that will be called when this action has finished
     *                                    `function(err, event){}`
     */
    getLastEvent(query, callback) {
        if (typeof query === 'string') {
            query = {
                aggregateId: query
            };
        }

        const aggregateId = query.aggregateId;
        this._doOnShardedEventstore(aggregateId, 'getLastEvent', query, callback);
    }

    /**
     * loads the events
     * @param {Object || String} query    the query object [optional]
     * @param {Number}           skip     how many events should be skipped? [optional]
     * @param {Number}           limit    how many events do you want in the result? [optional]
     * @param {Function}         callback the function that will be called when this action has finished
     *                                    `function(err, events){}`
     */
    getEvents(query, skip, limit, callback) {
        // TODO: Review, possible contract breach
        if (!query) {
            throw new Error('query should be defined');
        }

        if (isNaN(parseInt(query.shard))) {
            throw new Error('shard should be a number');
        }

        if (!query.partition) {
            throw new Error('partition should be defined');
        }

        if (typeof query === 'function') {
            callback = query;
            skip = 0;
            limit = -1;
            query = {};
        } else if (typeof skip === 'function') {
            callback = skip;
            skip = 0;
            limit = -1;
            if (typeof query === 'number') {
              skip = query;
              query = {};
            }
        } else if (typeof limit === 'function') {
            callback = limit;
            limit = -1;
            if (typeof query === 'number') {
              limit = skip;
              skip = query;
              query = {};
            }
        }

        try {
            let shard = query.shard;
            const eventstore = this._eventstores[shard];
            eventstore.getEventsAsync(query, skip, limit).then((data) => {
                callback(null, data);
            }).catch(callback);
        } catch (error) {
            callback(error);
        }
    }

    /**
     * loads the last event in a stream
     * @param {Object || String} query    the query object [optional]
     * @param {Function}         callback the function that will be called when this action has finished
     *                                    `function(err, eventstream){}`
     */
    getLastEventAsStream(query, callback) {
        if (typeof query === 'string') {
            query = {
                aggregateId: query
            };
        }

        const aggregateId = query.aggregateId;
        this._doOnShardedEventstore(aggregateId, 'getLastEventAsStream', query, callback);
    }

    /**
     * loads the event stream
     * @param {Object || String} query    the query object
     * @param {Number}           revMin   revision start point [optional]
     * @param {Number}           revMax   revision end point (hint: -1 = to end) [optional]
     * @param {Function}         callback the function that will be called when this action has finished
     *                                    `function(err, eventstream){}`
     */
    getEventStream(query, revMin, revMax, callback) {
        if (typeof query === 'string') {
            query = {
                aggregateId: query
            };
        }

        const aggregateId = query.aggregateId;
        this._doOnShardedEventstore(aggregateId, 'getEventStream', query, revMin, revMax, callback);
    }

    /**
     * loads the next snapshot back from given max revision
     * @param {Object || String} query    the query object
     * @param {Number}           revMax   revision end point (hint: -1 = to end) [optional]
     * @param {Function}         callback the function that will be called when this action has finished
     *                                    `function(err, snapshot, eventstream){}`
     */
    getFromSnapshot(query, revMax, callback) {
        if (typeof query === 'string') {
            query = {
                aggregateId: query
            };
        }

        const aggregateId = query.aggregateId;
        this._doOnShardedEventstore(aggregateId, 'getFromSnapshot', query, revMax, callback);
    }

    /**
     * stores a new snapshot
     * @param {Object}   obj      the snapshot data
     * @param {Function} callback the function that will be called when this action has finished [optional]
     */
    createSnapshot(obj, callback) {
        const aggregateId = obj.aggregateId;
        this._doOnShardedEventstore(aggregateId, 'createSnapshot', obj, callback);
    }

    /**
     * loads all undispatched events
     * @param {Object || String} query    the query object [optional]
     * @param {Function}         callback the function that will be called when this action has finished
     *                                    `function(err, events){}`
     */
    getUndispatchedEvents(query, callback) {
        if (typeof query === 'string') {
            query = {
                aggregateId: query
            };
        }

        const aggregateId = query.aggregateId;
        this._doOnShardedEventstore(aggregateId, 'getUndispatchedEvents', query, callback);
    }

    /**
     * Sets the given event to dispatched.
     * @param {Object || String} evtOrId  the event object or its id
     * @param {Function}         callback the function that will be called when this action has finished [optional]
     */
    setEventToDispatched(evtOrId, callback) {
        if (typeof evtOrId === 'object') {
            let aggregateId = evtOrId.aggregateId;
            this._doOnShardedEventstore(aggregateId, 'setEventToDispatched', evtOrId.id, callback);
        } else {
            this._doOnAllEventstores('setEventToDispatched', evtOrId, callback);
        }
    }

    /**
     * @param {String} listName the name of the playback list view
     * @param {DoneCallbackFunction} done the last event that built this projection state
     * @returns {void} Returns void. Use the callback to the get playbacklist
     */
    getPlaybackListView(listName, done) {
        const index = _.random(0, this._options.numberOfShards - 1);
        const eventstore = this._eventstores[index];
        eventstore.getPlaybackListViewAsync(listName)
        .then((data) => {
            done(null, data);
        })
        .catch((err) => {
            console.error(err);
            done();
        });
    }

    /**
     * @param {String} listName the name of the playback list view
     * @param {String} listQuery the list query for the playback list view
     * @param {String} totalCountQuery the total count query for the playback list view
     * @param {Alias} alias the query for the playback list view
     * @param {DoneCallbackFunction} done the last event that built this projection state
     * @returns {void} Returns void. Use the callback to the get playbacklist
     */
    registerPlaybackListView(listName, listQuery, totalCountQuery, opts, done) {
        let promises = [];
        for (const eventstore of this._eventstores) {
            promises.push(eventstore.registerPlaybackListViewAsync(listName, listQuery, totalCountQuery, opts));
        }

        Promise.all(promises)
        .then(() => {
            done();
        })
        .catch((err) => {
            console.error(err);
            done();
        });
    }

    /**
     * @param {String} functionName the name of the function to register
     * @param {Function} theFunction the function to call
     * @returns {void} Returns void.
     */
    registerFunction(functionName, theFunction) {
        this._doOnAllEventstoresNoCallback('registerFunction', functionName, theFunction);
    }

    /**
     * @param {String} token the position/offset from where we should start the subscription
     * @returns {Boolean} returns true if subscription token is existing and it got unsubscribed. false if it token does not exist
     */
    unsubscribe(token) {
        this._doOnAllEventstoresNoCallback('unsubscribe', token);
    }

    /**
     * Closes all Subscription Event StreamBuffers
     * @param {DoneCallbackFunction} callback success/error callback
     * @returns {void} - returns void
     */
    closeSubscriptionEventStreamBuffers(callback) {
        this._doOnAllEventstores('closeSubscriptionEventStreamBuffers', callback);
    }

    /**
     * Closes all Projection Event StreamBuffers
     * @param {DoneCallbackFunction} callback success/error callback
     * @returns {void} - returns void
     */
    closeProjectionEventStreamBuffers(callback) {
        this._doOnAllEventstores('closeProjectionEventStreamBuffers', callback);
    }

    /**
     * @returns {void} - returns void
     */
    deactivatePolling() {
        this._doOnAllEventstoresNoCallback('deactivatePolling', arguments);
    }

    /**
     * @returns {void} - returns void
     */
    activatePolling() {
        this._doOnAllEventstoresNoCallback('activatePolling', arguments);
    }

    /**
     * @param {DoneCallbackFunction} callback success/error callback
     * @returns {void} - returns void
     */
    close(callback) {
        this._doOnAllEventstores('close', callback);
    }

    _listenAndForwardEmitOfAllEventstores() {
        const self = this;
        for (const eventstore of self._eventstores) {
            eventstore.on('playbackError', (errorFault) => {
                self.emit('playbackError', errorFault);
            })

            eventstore.on('playbackSuccess', (successParams) => {
                self.emit('playbackSuccess', successParams);
            })

            eventstore.on('rebalance', (updatedAssignments) => {
                self.emit('rebalance', updatedAssignments);
            })
        }
    }

    _doOnAllEventstores(methodName, ...args) {
        const promises = [];
        const callback = args[args.length - 1];
        for (const eventstore of this._eventstores) {
            promises.push(eventstore[methodName + 'Async'](...args));
        }
        Promise.all(promises).then(function(...data) {
            callback(null, ...data);
        }).catch(callback);
    }

    _doOnAllEventstoresNoCallback(methodName, ...args) {
        for (const eventstore of this._eventstores) {
            eventstore[methodName](...args);
        }
    }

    _doOnShardedEventstore(aggregateId, methodName, ...args) {
        // NOTE: our usage always have aggregateId in query
        let shard = this._getShard(aggregateId);
        const eventstore = this._eventstores[shard];
        return eventstore[methodName](...args);
    }

    _getShard(aggregateId) {
        let shard = murmurhash(aggregateId) % this._options.numberOfShards;
        return shard;
    }
}
module.exports = ClusteredEventStore;