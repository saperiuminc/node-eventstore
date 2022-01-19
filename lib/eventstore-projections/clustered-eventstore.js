const murmurhash = require('murmurhash');
const Bluebird = require('bluebird');
const _ = require('lodash');
const debug = require('debug')('eventstore:clustered');
const TaskAssignmentGroup = require('./task-assignment').TaskAssignmentGroup;
const { partition } = require('lodash');
const EventEmitter = require('events').EventEmitter;
const EventstoreWithProjection = require('./eventstore-projection');

class ClusteredEventStore extends EventstoreWithProjection {
    constructor(opts, store, distributedSignal, distributedLock, playbackListStore, playbackListViewStore, projectionStore, stateListStore) {
        super(opts, store, distributedSignal, distributedLock, playbackListStore, playbackListViewStore, projectionStore, stateListStore);

        const defaults = {
            partitions: 25
        };

        this._options = _.defaults(opts, defaults);
        this._options.numberOfShards = this._options.clusters.length;

        this._eventstoreCount = 0;
        this._eventstores = [];
    }

    addEventstoreToCluster(eventstoreNode) {
        this._eventstores.push(eventstoreNode);
        this._eventstoreCount++;
        Bluebird.promisifyAll(eventstoreNode);
    }

    init() {
        this._doOnAllEventstores('init', arguments);
        this._listenAndForwardEmitOfAllEventstores();
    }

    startAllProjections() {
        const self = this;
        const promises = [];
        let args = arguments;
        const callback = args[args.length - 1];

        for (const eventstore of self._eventstores) {
            promises.push(eventstore['startAllProjectionsAsync'].apply(eventstore, {}));
        }

        Promise.all(promises)
        .then(function(data) {
            const projectionPromises = [];
            for (const eventstore of self._eventstores) {
                projectionPromises.push(eventstore.getProjectionsAsync());
            }
            return Promise.all(projectionPromises);
        })
        .then((promiseAllResponse) => {
            let allProjectionsInAllShards = [];
            for (const projectionsOfAShard of promiseAllResponse) {
              allProjectionsInAllShards = allProjectionsInAllShards.concat(projectionsOfAShard);
            }

            const filterByDictionary = {};
            let allProjectionIdsInAllShards = [];
            for(const projConfig of allProjectionsInAllShards) {
                filterByDictionary[projConfig.configuration.projectionId] = projConfig.configuration.projectionId;
            }
            allProjectionIdsInAllShards = Object.values(filterByDictionary);

            return self._doTaskAssignment(self._options.projectionGroup, allProjectionIdsInAllShards);
        })
        .then(function(...data) {
            callback(null, ...data);
        })
        .catch((err) => {
            console.error(err);
            callback();
        });
    }

    async _doTaskAssignment(projectionGroup, allProjectionsInAllShards) {
        const self = this;
        const tasks = [];

        for (let projectionId of allProjectionsInAllShards) {
            tasks.push(projectionId);
        }

        // create distributed lock
        const redisCreateClient = self._options.redisCreateClient;
        const redisClient = redisCreateClient('client');
        const DistributedLock = require('./distributed-lock');
        self._distributedLock = new DistributedLock({
            redis: redisClient,
            lock: self._options.lock
        });

        self._taskGroup = new TaskAssignmentGroup({
            initialTasks: tasks,
            createRedisClient: self._options.redisCreateClient,
            groupId: projectionGroup,
            membershipPollingTimeout: self._options.membershipPollingTimeout,
            distributedLock: self._distributedLock
        });
    
        await self._taskGroup.join();
        self._taskGroup.on('rebalance', async (updatedAssignments, rebalanceId) => {
            // distribute to shards
            const distributedAssignments = {};
            for(const projectionId of updatedAssignments) {
                const splitProjectionId = projectionId.split(':');
                if (splitProjectionId.length > 1 && splitProjectionId[1]) {
                    const shardString = splitProjectionId[1];
                    const stringIndex = shardString.replace('shard', '');
                    const shard = parseInt(stringIndex);
                    if (!distributedAssignments[shard]) {
                        distributedAssignments[shard] = [];
                    }
                    distributedAssignments[shard].push(projectionId);
                }
            }
            const distributedAssignmentsLength = Object.keys(distributedAssignments).length;
            if(distributedAssignmentsLength > 0) {
                for (const eventstore of this._eventstores) {
                    const updatedEventStoreAssignment = distributedAssignments[eventstore.options.shard];
                    if (updatedEventStoreAssignment) {
                        eventstore.rebalance(updatedEventStoreAssignment, rebalanceId);
                    } else {
                        // NOTE: still update but with empty assignment
                        eventstore.rebalance([], rebalanceId);
                    }
                }
            }
        });
    };

    project(projectionConfig, callback) {
        this._project(projectionConfig).then((data) => {
            callback(null, data);
        }).catch(callback);
    }

    _getPartitionsAsync(eventstore) {
        return new Promise((resolve, reject) => {
            eventstore.store.getPartitions(function(err, partitions) {
                if (err) {
                    reject(err)
                } else {
                    resolve(partitions);
                }
            })
        });
    }

    async _project(projectionConfig) {
        // TODO: review projectionId, config
        // also review adding partitioning here

        const promises = [];
        for (const eventstore of this._eventstores) {
            const partitions = await this._getPartitionsAsync(eventstore);
            for (const partition of partitions) {
                const projectionConfigClone = _.cloneDeep(projectionConfig);
                projectionConfigClone.projectionId = `${projectionConfigClone.projectionId}:shard${eventstore.options.shard}:partition${partition}`;
                projectionConfigClone.shard = eventstore.options.shard;
                projectionConfigClone.partition = partition;
                promises.push(eventstore.projectAsync(projectionConfigClone));
            }
        }

        await Promise.all(promises);
    }

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

    runProjection(projectionId, forced, done) {
        this._runProjection(projectionId, forced).then((data) => {
            done(null, data);
        }).catch(done);
    }

    async _runProjection(projectionId, forced) {
        const self = this;
        const promises = [];
        const newTasks = [];

        for (const eventstore of this._eventstores) {
            const partitions = await this._getPartitionsAsync(eventstore);
            for (const partition of partitions) {
                let clonedProjectionId = _.clone(projectionId);
                clonedProjectionId = `${clonedProjectionId}:shard${eventstore.options.shard}:partition${partition}`;
                newTasks.push(clonedProjectionId);
                promises.push(eventstore.runProjectionAsync(clonedProjectionId, forced));
            }
        }

        await Promise.all(promises);

        if (self._taskGroup) {
            self._taskGroup.addTasks(newTasks);
        }
    }

    getProjection(projectionId, done) {
        this._getProjection(projectionId).then((data) => {
            done(null, data);
        }).catch(done);
    }

    async _getProjection(projectionId) {
        const promises = [];

        for (const eventstore of this._eventstores) {
            const partitions = await this._getPartitionsAsync(eventstore);
            for (const partition of partitions) {
                let clonedProjectionId = _.clone(projectionId);
                clonedProjectionId = `${clonedProjectionId}:shard${eventstore.options.shard}:partition${partition}`;
                promises.push(eventstore.getProjectionAsync(clonedProjectionId));
            }
        }
        
        return await Promise.all(promises);
    }

    getProjections() {
        const self = this;
        const promises = [];
        let args = arguments;
        const done = args[args.length - 1];

        // NOTE: for review, getting projections of one shard gets all projections of all shards
        const eventstore = this._eventstores[0];
        promises.push(eventstore['getProjectionsAsync'].apply(eventstore, {}));

        Promise.all(promises)
        .then((data) => done(null, data))
        .catch((err) => {
            console.error(err);
            done();
        });
    }

    deleteProjection(projectionId, done) {
        this._deleteProjection(projectionId).then((data) => {
            done(null, data);
        }).catch(done);
    }

    async _deleteProjection(projectionId) {
        const self = this;
        const promises = [];

        for (const eventstore of this._eventstores) {
            const partitions = await this._getPartitionsAsync(eventstore);
            for (const partition of partitions) {
                let clonedProjectionId = _.clone(projectionId);
                clonedProjectionId = `${clonedProjectionId}:shard${eventstore.options.shard}:partition${partition}`;
                if(self._taskGroup) {
                    self._taskGroup.removeTasks([clonedProjectionId]);
                }
                promises.push(eventstore.deleteProjectionAsync(clonedProjectionId));
            }
        }
        
        return await Promise.all(promises);
    }

    pauseProjection(projectionId, done) {
        this._pauseProjection(projectionId).then((data) => {
            done(null, data);
        }).catch(done);
    }

    async _pauseProjection(projectionId) {
        const promises = [];

        for (const eventstore of this._eventstores) {
            const partitions = await this._getPartitionsAsync(eventstore);
            for (const partition of partitions) {
                let clonedProjectionId = _.clone(projectionId);
                clonedProjectionId = `${clonedProjectionId}:shard${eventstore.options.shard}:partition${partition}`;
                promises.push(eventstore.pauseProjectionAsync(clonedProjectionId));
            }
        }
        
        return await Promise.all(promises);
    }

    resetProjection(projectionId, done) {
        this._resetProjection(projectionId).then((data) => {
            done(null, data);
        }).catch(done);
    }

    async _resetProjection(projectionId) {
        const promises = [];

        for (const eventstore of this._eventstores) {
            const partitions = await this._getPartitionsAsync(eventstore);
            for (const partition of partitions) {
                let clonedProjectionId = _.clone(projectionId);
                clonedProjectionId = `${clonedProjectionId}:shard${eventstore.options.shard}:partition${partition}`;
                promises.push(eventstore.resetProjectionAsync(clonedProjectionId));
            }
        }
        
        return await Promise.all(promises);
    }

    subscribe(query, revision, onEventCallback, onErrorCallback) {
        const aggregateId = query.aggregateId;
        this._doOnShardedEventstore(aggregateId, 'subscribe', arguments);
    }

    defineEventMappings() {
        this._doOnAllEventstoresNoCallback('defineEventMappings', arguments);
    }

    useEventPublisher() {
        // NOTE: callback of useEventPublisher is not a callback when useEventPublisher is finished being called
        // it is a callback for when an event is to be published. 
        // hence we are using the no callback method
        this._doOnAllEventstoresNoCallback('useEventPublisher', arguments);
    }

    getLastEvent(query) {
        if (typeof query === 'string') {
            query = {
                aggregateId: query
            };
        }

        const aggregateId = query.aggregateId;
        this._doOnShardedEventstore(aggregateId, 'getLastEvent', arguments);
    }

    getEvents(query, callback) {
        if (!query) {
            throw new Error('query should be defined');
        }

        if (isNaN(parseInt(query.shard))) {
            throw new Error('shard should be a number');
        }

        if (!query.partition) {
            throw new Error('partition should be defined');
        }

        try {
            let shard = query.shard;
            const eventstore = this._eventstores[shard];
            eventstore.getEventsAsync(query).then((data) => {
                callback(null, data);
            }).catch(callback);
        } catch (error) {
            callback(error);
        }
    }

    getLastEventAsStream(query, callback) {
        if (typeof query === 'string') {
            query = {
                aggregateId: query
            };
        }

        const aggregateId = query.aggregateId;
        this._doOnShardedEventstore(aggregateId, 'getLastEventAsStream', arguments);
    }

    getEventStream(query) {
        if (typeof query === 'string') {
            query = {
                aggregateId: query
            };
        }

        const aggregateId = query.aggregateId;
        this._doOnShardedEventstore(aggregateId, 'getEventStream', arguments);
    }

    getFromSnapshot(query, revMax, callback) {
        if (typeof query === 'string') {
            query = {
                aggregateId: query
            };
        }

        const aggregateId = query.aggregateId;
        this._doOnShardedEventstore(aggregateId, 'getFromSnapshot', arguments);
    }

    createSnapshot(query, callback) {
        const aggregateId = query.aggregateId;
        this._doOnShardedEventstore(aggregateId, 'createSnapshot', arguments);
    }

    getUndispatchedEvents(query, callback) {
        if (typeof query === 'string') {
            query = {
                aggregateId: query
            };
        }

        const aggregateId = query.aggregateId;
        this._doOnShardedEventstore(aggregateId, 'getUndispatchedEvents', arguments);
    }

    setEventToDispatched(event) {
        const aggregateId = event.aggregateId;
        this._doOnShardedEventstore(aggregateId, 'setEventToDispatched', arguments);
    }

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

    registerFunction(functionName, theFunction) {
        this._doOnAllEventstoresNoCallback('registerFunction', arguments);
    }

    unsubscribe(token) {
        this._doOnAllEventstoresNoCallback('unsubscribe', arguments);
    }

    closeSubscriptionEventStreamBuffers(done) {
        this._doOnAllEventstores('closeSubscriptionEventStreamBuffers', arguments);
    }

    closeProjectionEventStreamBuffers(done) {
        this._doOnAllEventstores('closeProjectionEventStreamBuffers', arguments);
    }

    deactivatePolling() {
        this._doOnAllEventstoresNoCallback('deactivatePolling', arguments);
    }

    activatePolling() {
        this._doOnAllEventstoresNoCallback('activatePolling', arguments);
    }

    close(callback) {
        this._doOnAllEventstores('close', arguments);
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

    _doOnAnyEventstore(methodName, args) {
        // const index = Math.random(this._options.numberOfShards);
        const index = _.random(0, this._options.numberOfShards - 1);
        const eventstore = this._eventstores[index];
        return eventstore[methodName].apply(eventstore, args)
    }

    _doOnAllEventstores(methodname, args) {
        const promises = [];
        const callback = args[args.length - 1];
        for (const eventstore of this._eventstores) {
            promises.push(eventstore[methodname + 'Async'].apply(eventstore, args));
        }
        Promise.all(promises).then(function(...data) {
            callback(null, ...data);
        }).catch(callback);
    }

    _doOnAllEventstoresNoCallback(methodName, args) {
        for (const eventstore of this._eventstores) {
            eventstore[methodName].apply(eventstore, args)
        }
    }

    _doOnShardedEventstore(aggregateId, methodName, args) {
        // NOTE: our usage always have aggregateId in query
        let shard = this._getShard(aggregateId);
        const eventstore = this._eventstores[shard];
        return eventstore[methodName].apply(eventstore, args);
    }

    _getShard(aggregateId) {
        let shard = murmurhash(aggregateId) % this._options.numberOfShards;
        return shard;
    }

}
module.exports = ClusteredEventStore;