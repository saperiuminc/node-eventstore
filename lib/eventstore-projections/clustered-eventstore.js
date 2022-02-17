const Bluebird = require('bluebird');
const _ = require('lodash');
const debug = require('debug')('eventstore:clustered');
const TaskAssignmentGroup = require('./task-assignment').TaskAssignmentGroup;
const EventstoreWithProjection = require('./eventstore-projection');
const helpers = require('../helpers');
const debugC = require('debug')('eventstore:clustered');

class ClusteredEventStore extends EventstoreWithProjection {
    constructor(opts, store, distributedSignal, distributedLock, playbackListStore, playbackListViewStore, projectionStore, stateListStore) {
        super(opts, store, distributedSignal, distributedLock, playbackListStore, playbackListViewStore, projectionStore, stateListStore);

        const defaultOptions = {
            partitions: 25,
            membershipPollingTimeout: 10000
        };

        this._options = Object.assign(_.clone(defaultOptions), opts);
        this._options.numberOfShards = this._options.clusters.length;

        this._eventstoreCount = 0;
        this._eventstores = [];
        this._distributedLock = distributedLock;
        this._taskGroup = null;

        this.shard = -1;
    }

    async createProjectionTasks(projection) {
        let projectionTasks;
        const storedProjectionTasks = await this._projectionStore.getProjectionTasks(projection.projectionId);

        if (storedProjectionTasks.length === 0) {
            const createProjectionTaskPromises = [];
            const partitions = this.store.getPartitions();
            const hasPartitions = !partitions || !partitions.length ? false : true;
            const createProjectionTaskFunc = async (partition) => {
                let projectionTaskOffset = 0;
                if ((projection && projection.configuration && projection.configuration.fromOffset === 'latest') || !hasPartitions) {
                    projectionTaskOffset = await this._getLatestOffsetAsync(partition);
                }
                const newProjectionTask = {
                    projectionTaskId: this._getProjectionTaskId(projection.projectionId, hasPartitions ? partition : null),
                    projectionId: projection.projectionId,
                    shard: this.shard,
                    partition: hasPartitions ? partition : '__unpartitioned',
                    offset: this._serializeProjectionOffset(projectionTaskOffset)
                };
                debugC('ES-SHARD', this.shard, 'CREATING PROJECTION TASK', newProjectionTask);
                createProjectionTaskPromises.push(this._projectionStore.createProjectionTask(newProjectionTask));
            }
            if (!hasPartitions) {
                await createProjectionTaskFunc(null)
            } else {
                for (const partition of partitions) {
                    await createProjectionTaskFunc(partition)
                }
            }
            
            await Promise.all(createProjectionTaskPromises);
        } else {
            debugC('ES-SHARD', this.shard, 'TASKS ALREADY EXISTING FOR PROJECTION', projectionConfig.projectionId);
            projectionTasks = storedProjectionTasks;
        }

        return projectionTasks;
    }

    async doTaskAssignment(projectionGroup, projectionTasks) {
        // TODO: Ryan: Need to only get tasks for a specific context. This is because the projection store can be used by other microservices

        const projectionTaskIds = _.map(projectionTasks, (task) => {
            return task.projectionTaskId;
        })
        this._taskGroup = new TaskAssignmentGroup({
            initialTasks: projectionTaskIds,
            createRedisClient: this._options.redisCreateClient,
            groupId: projectionGroup,
            membershipPollingTimeout: this._options.membershipPollingTimeout,
            distributedLock: this._distributedLock
        });

        this._taskGroup.on('rebalance', async (updatedAssignments, rebalanceId) => {
            debug('CLUSTERED MULTI REBALANCE CALLED', updatedAssignments);
            // distribute to shards
            await this.processProjectionTasks(updatedAssignments, rebalanceId);
        })
    };

    /**
     * @param {ProjectionConfiguration} projectionConfig parameters for the projection
     * @param {Function} callback success/error callback
     * @returns {void} - returns void
     */
    project(projectionConfig, callback) {
    //   if (projectionConfig.partitionBy == 'stream' || _.isFunction(projectionConfig.partitionBy)) {
    //     debug('CLUSTERED: project called with multi-concurrent configuration', projectionConfig.projectionId);
    //     this._doOnAllEventstores('project', projectionConfig, callback);
    //   } else {
    //     debug('CLUSTERED: project called with single-concurrent configuration', projectionConfig.projectionId);
    //     super.project(projectionConfig, callback);
    //   }
    }

    /**
     * @param {DoneCallbackFunction} callback success/error callback
     * @returns {void} - returns void
     */
    stopAllProjections(callback) {
        const self = this;
        super.stopAllProjections(function(err, ...data) {
            if (self._taskGroup) {
                self._taskGroup.leave();
            }

            callback(err, ...data);
        })
    }

    /**
     * @param {string} projectionId
     * @returns {Promise}
     */
    deleteProjection(projectionId, callback) {
        const self = this;

        super.deleteProjection(function(err, projectionTaskIds) {
            if (err) {
                callback(err);
            } else {
                if (self._taskGroup) {
                    self._taskGroup.removeTasks(projectionTaskIds).then((data) => callback(null, data)).catch(callback);
                }
            }
        });
    };

    /**
     * @param {string} projectionId
     * @returns {Promise}
     */
    runProjection(projectionId, forced, done) {
        const self = this;
        const promises = [];
        for (const eventstore of self._eventstores) {
            promises.push(eventstore.runProjectionAsync(projectionId, forced));
        }

        Promise.all(promises)
        .then((projectionTasksIds) => {
            return self.getProjectionAsync(projectionId).then((projection) => {
                return {
                    projectionTasksIds: projectionTasksIds,
                    projectionConfig: projection.configuration
                }
            });
        }).then((result) => {
            const projectionTasksIds = result.projectionTasksIds;
            const projectionConfig = result.projectionConfig;
            projectionTasksIds.forEach((projectionTaskIds) => {
                if (projectionConfig.partitionBy == 'stream' || _.isFunction(projectionConfig.partitionBy)) {
                    debug('CLUSTERED: adding tasks to multi-concurrent group', projectionConfig.projectionId);
                    if (self._multiTaskGroup) { 
                        self._multiTaskGroup.addTasks(projectionTaskIds);
                    }
                } else {
                    debug('CLUSTERED: adding tasks to single-concurrent group', projectionConfig.projectionId);
                    if (self._singleTaskGroup) {
                        self._singleTaskGroup.addTasks(projectionTaskIds);
                    }
                }
            });
            done(null, projectionTasksIds);
        }).catch((err) => {
            console.error(err);
            done(err);
        });
    };

    /**
     * @param {AggregateQuery} query the query for the aggregate/category that we like to get
     * @param {Number} revision the revision from where we should start the subscription
     * @param {EventCallback} onEventCallback the callback to be called whenever an event is available for the subscription
     * @param {ErrorCallback} onErrorCallback the callback to be called whenever an error is thrown
     * @returns {String} - returns token
     */
    subscribe(query, revision, onEventCallback, onErrorCallback) {
        // TODO: Ryan: I think we can just use the super method
        return super.subscribe(query, revision, onEventCallback, onErrorCallback);
        // const aggregateId = query.aggregateId;
        // return this._doOnShardedEventstore(aggregateId, 'subscribe', query, revision, onEventCallback, onErrorCallback);
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
        // TODO: Ryan: I think we can just use the super method. I think we dont need this anymore
        return super.defineEventMappings(mappings);
        // this._doOnAllEventstoresNoCallback('defineEventMappings', mappings);
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
        // TODO: Ryan: I think we can just use the super method. I think we dont need this anymore
        return super.useEventPublisher(fn);
        // this._doOnAllEventstoresNoCallback('useEventPublisher', fn);
        // super.useEventPublisher(fn);
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
    getEvents(queryInput, skip, limit, callback) {
        let query = _.clone(queryInput);
        if (Array.isArray(query)) {
            query = query[0];
        }
        // TODO: Review, possible contract breach
        if (!query) {
            throw new Error('query should be defined');
        }

        if (isNaN(parseInt(query.shard))) {
            throw new Error('shard should be a number');
        }
        // if (!query.partition) {
        //     throw new Error('partition should be defined');
        // }
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

        if (query.partition) { // multi-concurrency
          try {
              let shard = query.shard;
              const eventstore = this._eventstores[shard];
              eventstore.getEventsAsync(query, skip, limit).then((data) => {
                  callback(null, data);
              }).catch(callback);
          } catch (error) {
              callback(error);
          }
        } else { // single concurrency
          this.store.getEvents(query, skip, limit, callback);
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
        super.registerFunction(functionName, theFunction);
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
     * 
     * @param {Number|Number[]} rawOffset 
     * @return {Number|Number[]} 
     */
    formatOffset(rawOffset) {
      const finalOffset = [];
      this._eventstores.forEach((eventstore, index) => {
        if (!Array.isArray(rawOffset) || index >= rawOffset.length || isNaN(rawOffset[index])) {
          finalOffset.push(0)
        } else {
          finalOffset.push(rawOffset[index])
        }
      });
      return finalOffset;
    }
    /**
     * 
     * @param {Object} event 
     * @param {Number|Number[]} oldOffset 
     * @return {Number|Number[]} 
     */
    calculateNewOffset(event, oldOffset) {
      let finalOffset;
      if (Array.isArray(oldOffset)) {
        const partition = helpers.getShard(event.aggregateId, this._options.numberOfShards);
        finalOffset = oldOffset;
        finalOffset[partition] = event.eventSequence;
      } else {
        return oldOffset;
      }
      return finalOffset;
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
            });

            eventstore.on('playbackSuccess', (successParams) => {
                self.emit('playbackSuccess', successParams);
            });
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
        let shard = helpers.getShard(aggregateId, this._options.numberOfShards);
        const eventstore = this._eventstores[shard];
        return eventstore[methodName](...args);
    }
}

module.exports = ClusteredEventStore;
