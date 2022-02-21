const _ = require('lodash');
const debug = require('debug')('eventstore:clustered');
const TaskAssignmentGroup = require('./task-assignment').TaskAssignmentGroup;
const EventstoreWithProjection = require('./eventstore-projection');
const helpers = require('../helpers');

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
    }
    
    async createProjectionTasks(projection) {
        let projectionTasks;
        const projectionConfig = projection.configuration;
        let concurrencyType = '';
        if (projectionConfig.partitionBy == 'stream' || _.isFunction(projectionConfig.partitionBy)) {
            concurrencyType = 'multi';
        } else {
            concurrencyType = 'single';
        }
        const storedProjectionTasks = await this._projectionStore.getProjectionTasks(projection.projectionId);
        if (storedProjectionTasks.length === 0) {
            const createProjectionTaskPromises = [];
            const partitions = this.store.getPartitions(concurrencyType);
            const hasPartitions = !partitions || !partitions.length ? false : true;
            const createProjectionTaskFunc = async (shard, partition) => {
                let projectionTaskOffset = helpers.serializeProjectionOffset(0);
                if ((projection && projectionConfig && projectionConfig.fromOffset === 'latest') || !hasPartitions) {
                    projectionTaskOffset = await this._getLatestOffsetAsync(shard > -1 ? shard : null, hasPartitions ? partition : null);
                }
                const newProjectionTask = {
                    projectionTaskId: this._getProjectionTaskId(projection.projectionId, shard, hasPartitions ? partition : null),
                    projectionId: projection.projectionId,
                    shard: shard,
                    partition: hasPartitions ? partition : '__unpartitioned',
                    offset: projectionTaskOffset
                };
                debug('CLUSTERED-ES: CREATING PROJECTION TASK', newProjectionTask);
                createProjectionTaskPromises.push(this._projectionStore.createProjectionTask(newProjectionTask));
            }
            if (!hasPartitions) {
                await createProjectionTaskFunc(-1, null);
            } else {
                for (let i = 0; i < this._options.numberOfShards; i++) {
                    for (const partition of partitions) {
                        await createProjectionTaskFunc(i, partition);
                    }
                }
            }
            
            await Promise.all(createProjectionTaskPromises);
        } else {
            debug('CLUSTERED-ES: TASKS ALREADY EXISTING FOR PROJECTION', projection.projectionId);
            projectionTasks = storedProjectionTasks;
        }
        
        return projectionTasks;
    }
    
    async doTaskAssignment(projectionGroup, projectionTasks) {
        
        const projectionTaskIds = _.map(projectionTasks, (task) => {
            return task.projectionTaskId;
        });
        debug('CLUSTERED-ES: doTaskAssignment CALLED', projectionTaskIds);
        this._taskGroup = new TaskAssignmentGroup({
            initialTasks: projectionTaskIds,
            createRedisClient: this._options.redisCreateClient,
            groupId: projectionGroup,
            membershipPollingTimeout: this._options.membershipPollingTimeout,
            distributedLock: this._distributedLock
        });
        
        await this._taskGroup.join();
        
        this._taskGroup.on('rebalance', async (updatedAssignments, rebalanceId) => {
            debug('CLUSTERED-ES: REBALANCE CALLED', updatedAssignments);
            // distribute to shards
            await this.processProjectionTasks(updatedAssignments, rebalanceId);
        })
    };
    
    /**
    * 
    * @param {String} partition 
    * @return {Number|Number[]} 
    */
    resetOffset(partition) {
        if (partition === '__unpartitioned') { // single concurrency
            let index = 0;
            const finalOffsetArray = [];
            while (index < this._options.numberOfShards) {
                finalOffsetArray.push(helpers.serializeProjectionOffset({
                    commitStamp: 0,
                    eventId: '',
                    streamRevision: -1
                }));
                index++;
            }
            return helpers.serializeProjectionOffset(finalOffsetArray);
        } else {
            return helpers.serializeProjectionOffset({
                commitStamp: 0,
                eventId: '',
                streamRevision: -1
            });
        }
    }
    
    /**
    * 
    * @param {Object} aggregateId 
    */
    sendSignal(query, done) {
        const partition = helpers.getShard(query.aggregateId, this._options.numberOfShards);
        let index = 0;
        while (index < this._options.numberOfShards) {
            const shard = _.clone(index);
            const channels = super._queryToChannels(query, shard, partition);
            
            for (const channel of channels) {
                this._waitableProducer.signal(channel);
            }
            index++;
        }
        done(null, partition);
    };
    
    _getProjectionTaskId(projectionId, shard, partition) {
        if (!_.isNil(partition)) {
            return `${projectionId}:shard${shard}:partition${partition}`;
        } else {
            return projectionId;
        }
    }
    
    // /**
    //  * loads the last event
    //  * @param {Object || String} query    the query object [optional]
    //  * @param {Function}         callback the function that will be called when this action has finished
    //  *                                    `function(err, event){}`
    //  */
    // getLastEvent(query, callback) {
    //     if (typeof query === 'string') {
    //         query = {
    //             aggregateId: query
    //         };
    //     }
    
    //     const aggregateId = query.aggregateId;
    //     this._doOnShardedEventstore(aggregateId, 'getLastEvent', query, callback);
    // }
    
    // /**
    //  * loads the events
    //  * @param {Object || String} query    the query object [optional]
    //  * @param {Number}           skip     how many events should be skipped? [optional]
    //  * @param {Number}           limit    how many events do you want in the result? [optional]
    //  * @param {Function}         callback the function that will be called when this action has finished
    //  *                                    `function(err, events){}`
    //  */
    // getEvents(queryInput, skip, limit, callback) {
    //     let query = _.clone(queryInput);
    //     if (Array.isArray(query)) {
    //         query = query[0];
    //     }
    //     // TODO: Review, possible contract breach
    //     if (!query) {
    //         throw new Error('query should be defined');
    //     }
    
    //     if (isNaN(parseInt(query.shard))) {
    //         throw new Error('shard should be a number');
    //     }
    //     // if (!query.partition) {
    //     //     throw new Error('partition should be defined');
    //     // }
    //     if (typeof query === 'function') {
    //       callback = query;
    //       skip = 0;
    //       limit = -1;
    //       query = {};
    //     } else if (typeof skip === 'function') {
    //         callback = skip;
    //         skip = 0;
    //         limit = -1;
    //         if (typeof query === 'number') {
    //           skip = query;
    //           query = {};
    //         }
    //     } else if (typeof limit === 'function') {
    //         callback = limit;
    //         limit = -1;
    //         if (typeof query === 'number') {
    //           limit = skip;
    //           skip = query;
    //           query = {};
    //         }
    //     }
    
    //     if (query.partition) { // multi-concurrency
    //       try {
    //           let shard = query.shard;
    //           const eventstore = this._eventstores[shard];
    //           eventstore.getEventsAsync(query, skip, limit).then((data) => {
    //               callback(null, data);
    //           }).catch(callback);
    //       } catch (error) {
    //           callback(error);
    //       }
    //     } else { // single concurrency
    //       this.store.getEvents(query, skip, limit, callback);
    //     }
    // }
    
    // /**
    //  * loads the last event in a stream
    //  * @param {Object || String} query    the query object [optional]
    //  * @param {Function}         callback the function that will be called when this action has finished
    //  *                                    `function(err, eventstream){}`
    //  */
    // getLastEventAsStream(query, callback) {
    //     if (typeof query === 'string') {
    //         query = {
    //             aggregateId: query
    //         };
    //     }
    
    //     const aggregateId = query.aggregateId;
    //     this._doOnShardedEventstore(aggregateId, 'getLastEventAsStream', query, callback);
    // }
    
    // /**
    //  * loads the event stream
    //  * @param {Object || String} query    the query object
    //  * @param {Number}           revMin   revision start point [optional]
    //  * @param {Number}           revMax   revision end point (hint: -1 = to end) [optional]
    //  * @param {Function}         callback the function that will be called when this action has finished
    //  *                                    `function(err, eventstream){}`
    //  */
    // getEventStream(query, revMin, revMax, callback) {
    //     if (typeof query === 'string') {
    //         query = {
    //             aggregateId: query
    //         };
    //     }
    
    //     const aggregateId = query.aggregateId;
    //     this._doOnShardedEventstore(aggregateId, 'getEventStream', query, revMin, revMax, callback);
    // }
    
    // /**
    //  * loads the next snapshot back from given max revision
    //  * @param {Object || String} query    the query object
    //  * @param {Number}           revMax   revision end point (hint: -1 = to end) [optional]
    //  * @param {Function}         callback the function that will be called when this action has finished
    //  *                                    `function(err, snapshot, eventstream){}`
    //  */
    // getFromSnapshot(query, revMax, callback) {
    //     if (typeof query === 'string') {
    //         query = {
    //             aggregateId: query
    //         };
    //     }
    
    //     const aggregateId = query.aggregateId;
    //     this._doOnShardedEventstore(aggregateId, 'getFromSnapshot', query, revMax, callback);
    // }
    
    // /**
    //  * stores a new snapshot
    //  * @param {Object}   obj      the snapshot data
    //  * @param {Function} callback the function that will be called when this action has finished [optional]
    //  */
    // createSnapshot(obj, callback) {
    //     const aggregateId = obj.aggregateId;
    //     this._doOnShardedEventstore(aggregateId, 'createSnapshot', obj, callback);
    // }
    
    // /**
    //  * loads all undispatched events
    //  * @param {Object || String} query    the query object [optional]
    //  * @param {Function}         callback the function that will be called when this action has finished
    //  *                                    `function(err, events){}`
    //  */
    // getUndispatchedEvents(query, callback) {
    //     if (typeof query === 'string') {
    //         query = {
    //             aggregateId: query
    //         };
    //     }
    
    //     const aggregateId = query.aggregateId;
    //     this._doOnShardedEventstore(aggregateId, 'getUndispatchedEvents', query, callback);
    // }
    
    // /**
    //  * Sets the given event to dispatched.
    //  * @param {Object || String} evtOrId  the event object or its id
    //  * @param {Function}         callback the function that will be called when this action has finished [optional]
    //  */
    // setEventToDispatched(evtOrId, callback) {
    //     if (typeof evtOrId === 'object') {
    //         let aggregateId = evtOrId.aggregateId;
    //         this._doOnShardedEventstore(aggregateId, 'setEventToDispatched', evtOrId.id, callback);
    //     } else {
    //         this._doOnAllEventstores('setEventToDispatched', evtOrId, callback);
    //     }
    // }
    
    // /**
    //  * @param {String} listName the name of the playback list view
    //  * @param {DoneCallbackFunction} done the last event that built this projection state
    //  * @returns {void} Returns void. Use the callback to the get playbacklist
    //  */
    // getPlaybackListView(listName, done) {
    //     const index = _.random(0, this._options.numberOfShards - 1);
    //     const eventstore = this._eventstores[index];
    //     eventstore.getPlaybackListViewAsync(listName)
    //     .then((data) => {
    //         done(null, data);
    //     })
    //     .catch((err) => {
    //         console.error(err);
    //         done();
    //     });
    // }
    
    // /**
    //  * @param {String} listName the name of the playback list view
    //  * @param {String} listQuery the list query for the playback list view
    //  * @param {String} totalCountQuery the total count query for the playback list view
    //  * @param {Alias} alias the query for the playback list view
    //  * @param {DoneCallbackFunction} done the last event that built this projection state
    //  * @returns {void} Returns void. Use the callback to the get playbacklist
    //  */
    // registerPlaybackListView(listName, listQuery, totalCountQuery, opts, done) {
    //     let promises = [];
    //     for (const eventstore of this._eventstores) {
    //         promises.push(eventstore.registerPlaybackListViewAsync(listName, listQuery, totalCountQuery, opts));
    //     }
    
    //     Promise.all(promises)
    //     .then(() => {
    //         done();
    //     })
    //     .catch((err) => {
    //         console.error(err);
    //         done();
    //     });
    // }
    
    // /**
    //  * @param {String} functionName the name of the function to register
    //  * @param {Function} theFunction the function to call
    //  * @returns {void} Returns void.
    //  */
    // registerFunction(functionName, theFunction) {
    //     super.registerFunction(functionName, theFunction);
    //     this._doOnAllEventstoresNoCallback('registerFunction', functionName, theFunction);
    // }
    
    // /**
    //  * @param {String} token the position/offset from where we should start the subscription
    //  * @returns {Boolean} returns true if subscription token is existing and it got unsubscribed. false if it token does not exist
    //  */
    // unsubscribe(token) {
    //     this._doOnAllEventstoresNoCallback('unsubscribe', token);
    // }
    
    
    // /**
    //  * @returns {void} - returns void
    //  */
    // deactivatePolling() {
    //     this._doOnAllEventstoresNoCallback('deactivatePolling', arguments);
    // }
    
    // /**
    //  * @returns {void} - returns void
    //  */
    // activatePolling() {
    //     this._doOnAllEventstoresNoCallback('activatePolling', arguments);
    // }
    
    // /**
    //  * @param {DoneCallbackFunction} callback success/error callback
    //  * @returns {void} - returns void
    //  */
    // close(callback) {
    //     this._doOnAllEventstores('close', callback);
    // }
    
    // _listenAndForwardEmitOfAllEventstores() {
    //     const self = this;
    //     for (const eventstore of self._eventstores) {
    //         eventstore.on('playbackError', (errorFault) => {
    //             self.emit('playbackError', errorFault);
    //         });
    
    //         eventstore.on('playbackSuccess', (successParams) => {
    //             self.emit('playbackSuccess', successParams);
    //         });
    //     }
    // }
    
    // _doOnAllEventstores(methodName, ...args) {
    //     const promises = [];
    //     const callback = args[args.length - 1];
    //     for (const eventstore of this._eventstores) {
    //         promises.push(eventstore[methodName + 'Async'](...args));
    //     }
    //     Promise.all(promises).then(function(...data) {
    //         callback(null, ...data);
    //     }).catch(callback);
    // }
    
    // _doOnAllEventstoresNoCallback(methodName, ...args) {
    //     for (const eventstore of this._eventstores) {
    //         eventstore[methodName](...args);
    //     }
    // }
    
    // _doOnShardedEventstore(aggregateId, methodName, ...args) {
    //     // NOTE: our usage always have aggregateId in query
    //     let shard = helpers.getShard(aggregateId, this._options.numberOfShards);
    //     const eventstore = this._eventstores[shard];
    //     return eventstore[methodName](...args);
    // }
}

module.exports = ClusteredEventStore;
