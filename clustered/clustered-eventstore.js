const util = require('util');
const murmurhash = require('murmurhash');
const Bluebird = require('bluebird');
const _ = require('lodash');
const debug = require('debug')('eventstore:clustered');

class ClusteredEventStore {
    constructor(options) {
        const defaults = {
            clusters: []
        };

        this._options = _.defaults(options, defaults);
        this._options.numberOfShards = this._options.clusters.length;

        this._eventstores = [];

        this._options.clusters.forEach((storeConfig, index) => {
            let config = {
                type: storeConfig.type,
                host: storeConfig.host,
                port: storeConfig.port,
                user: storeConfig.user,
                password: storeConfig.password,
                database: storeConfig.database,
                connectionPoolLimit: storeConfig.connectionPoolLimit,
                shard: index,
                shouldDoTaskAssignment: false
            };

            let esConfig = _.defaults(config, _.cloneDeep(this._options));
            delete esConfig.clusters;

            const eventstore = require('../index')(esConfig);
            Bluebird.promisifyAll(eventstore);
            this._eventstores[index] = eventstore;
        });
    }

    init() {
        this.#doOnAllEventstores('init', arguments);
    }

    startAllProjections(callback) {
        const promises = [];
        const callback = args[args.length - 1];
        const arg = args.splice(0, args.length - 1);
        for (const eventstore in this._eventstores) {
            promises.push(eventstore[asyncMethodName].apply(arg));
        }

        Promise.all(promises)
        .then(() => {
            const projectionPromises = [];
            for (const eventstore in this._eventstores) {
                projectionPromises.push(eventstore.getProjectionsAsync());
            }
            return Promise.all(projectionPromises);
        })
        .then((promiseAllResponse) => {
            let allProjectionsInAllShards = [];
            for (const projectionsOfAShard of promiseAllResponse) {
              allProjectionsInAllShards = allProjectionsInAllShards.concat(projectionsOfAShard);
            }
            return this.#doTaskAssignment(this._options.projectionGroup, allProjectionsInAllShards);
        })
        .then(callback)
        .catch(callback);
    }

    #doTaskAssignment = async function(projectionGroup, allProjectionsInAllShards) {
        const self = this;
        const tasks = [];
    
        for (let projection in allProjectionsInAllShards) {
            tasks.push(projection.projectionId);
        }

        // create distributed lock
        const redisCreateClient = self._options.redisCreateClient;
        const redisClient = redisCreateClient('client');
        const DistributedLock = require('../lib/eventstore-projections/distributed-lock');
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
            for(const projectionId in updatedAssignments) {
                const splitProjectionId = projectionId.split(':');
                if(splitProjectionId.length > 1 && splitProjectionId[1]) {
                    const shardString = splitProjectionId[1];
                    const stringIndex = shardString.replace('shard', '');
                    const shard = parseInt(stringIndex);
                    if (!distributedAssignments[shard]) {
                        distributedAssignments[shard] = [];
                    }
                    distributedAssignments[shard].push(projectionId);
                }
            }

            if(distributedAssignments.length > 0) {
                for (const eventstore in this._eventstores) {
                    const updatedEventStoreAssignment = distributedAssignments[eventstore.shard];
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
        // TODO: review projectionId, config
        // also review adding partitioning here
        const promises = [];
        for (const eventstore in this._eventstores) {
            const projectionConfigClone = _.cloneDeep(projectionConfig);
            projectionConfigClone.projectionId = `${projectionConfigClone.projectionId}:shard${eventstore.shard}`
            promises.push(eventstore.projectAsync(projectionConfigClone));
        }
        Promise.all(promises).then(callback).catch(callback);
    }

    stopAllProjections(callback) {
        const self = this;
        const promises = [];
        for (const eventstore in this._eventstores) {
            promises.push(eventstore.stopAllProjectionsAsync());
        }
        
        Promise.all(promises)
        .then(() => {
            if (self._taskGroup) {
                self._taskGroup.leave();
            }
        })
        .then(callback)
        .catch(callback);
    }

    runProjection(projectionId, forced, done) {
        const self = this;
        const promises = [];
        const newTasks = [];

        for (const eventstore in this._eventstores) {
            const clonedProjectionId = _.clone(projectionId);
            clonedProjectionId = `${clonedProjectionId}:shard${eventstore.shard}`
            newTasks.push(clonedProjectionId);
            promises.push(eventstore.runProjectionAsync(clonedProjectionId, forced));
        }
        
        Promise.all(promises)
        .then(() => {
            if (self._taskGroup) {
                self._taskGroup.addTasks(newTasks);
            }
        })
        .then((data) => done(null, data))
        .catch(callback);
    }

    deleteProjection(projectionId, done) {
        const self = this;
        const promises = [];

        for (const eventstore in this._eventstores) {
            const clonedProjectionId = _.clone(projectionId);
            clonedProjectionId = `${clonedProjectionId}:shard${eventstore.shard}`
 
            self._taskGroup.removeTasks([clonedProjectionId]);
            promises.push(eventstore.deleteProjectionAsync(clonedProjectionId));
        }
        
        Promise.all(promises).then((data) => done(null, data)).catch(callback);
    }

    subscribe(query, revision, onEventCallback, onErrorCallback) {
        const aggregateId = query.aggregateId;
        this.#doOnShardedEventstore(aggregateId, 'subscribe', arguments);
    }

    defineEventMappings() {
        this.#doOnAllEventstoresNoCallback('defineEventMappings', arguments);
    }

    useEventPublisher() {
        // NOTE: callback of useEventPublisher is not a callback when useEventPublisher is finished being called
        // it is a callback for when an event is to be published. 
        // hence we are using the no callback method
        this.#doOnAllEventstoresNoCallback('useEventPublisher', arguments);
    }

    getLastEvent(query, callback) {
        if (typeof query === 'string') {
            query = {
                aggregateId: query
            };
        }

        const aggregateId = query.aggregateId;
        this.#doOnShardedEventstore(aggregateId, 'getLastEvent', arguments);
    }


    getLastEventAsStream(query, callback) {
        if (typeof query === 'string') {
            query = {
                aggregateId: query
            };
        }

        const aggregateId = query.aggregateId;
        this.#doOnShardedEventstore(aggregateId, 'getLastEventAsStream', arguments);
    }


    getEventStream(query) {
        if (typeof query === 'string') {
            query = {
                aggregateId: query
            };
        }

        const aggregateId = query.aggregateId;
        this.#doOnShardedEventstore(aggregateId, 'getEventStream', arguments);
    }

    getFromSnapshot(query, revMax, callback) {
        if (typeof query === 'string') {
            query = {
                aggregateId: query
            };
        }

        const aggregateId = query.aggregateId;
        this.#doOnShardedEventstore(aggregateId, 'getFromSnapshot', arguments);
    }

    createSnapshot(obj, callback) {
        const aggregateId = obj.aggregateId;
        this.#doOnShardedEventstore(aggregateId, 'createSnapshot', arguments);
    }

    getUndispatchedEvents(query, revMax, callback) {
        if (typeof query === 'string') {
            query = {
                aggregateId: query
            };
        }

        const aggregateId = query.aggregateId;
        this.#doOnShardedEventstore(aggregateId, 'getUndispatchedEvents', arguments);
    }

    setEventToDispatched() {
        this.#doOnAllEventstores('setEventToDispatched', arguments);
    }

    #doOnAnyEventstore(methodName, args) {
        const eventstore = this._eventstores[Math.random(this._options.numberOfShards)];
        eventstore[methodName].apply(eventstore, args)
    }

    #doOnAllEventstores(methodname, args) {
        const promises = [];
        const callback = args[args.length - 1];
        for (const eventstore of this._eventstores) {
            promises.push(eventstore[methodname].apply(eventstore, args));
        }
        Promise.all(promises).then(function(...data) {
            callback(null, ...data);
        }).catch(callback);
    }

    #doOnAllEventstoresNoCallback(asyncMethodName, args) {
        for (const eventstore of this._eventstores) {
            eventstore[asyncMethodName].apply(eventstore, args)
        }
    }

    #doOnShardedEventstore(aggregateId, methodName, args) {
        // NOTE: our usage always have aggregateId in query
        let shard = this.#getShard(aggregateId);
        const eventstore = this._eventstores[shard];
        return eventstore[methodName].apply(eventstore, args);
    }

    async #getShardAndPartition(aggregateId) {
        let shard = await this._mappingStore.getShard(aggregateId);
        return shard;
    }

    async #getShard(aggregateId) {
        let shard = murmurhash(aggregateId) % this._options.numberOfShards;
        return shard;
    }

    #getPartition(aggregateId, numberOfPartitions) {
        const partition = murmurhash(aggregateId) % numberOfPartitions;
        return partition;
    }

    #getShard(aggregateId) {
        let shard = murmurhash(aggregateId) % this._options.numberOfShards;
        return shard;
    }
}
module.exports = ClusteredEventStore;