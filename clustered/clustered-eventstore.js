const murmurhash = require('murmurhash');
const Bluebird = require('bluebird');
const _ = require('lodash');
const debug = require('debug')('eventstore:clustered');
const TaskAssignmentGroup = require('../lib/eventstore-projections/task-assignment').TaskAssignmentGroup;

class ClusteredEventStore {
    constructor(options) {
        const defaults = {
            clusters: [],
            partitions: 25
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
                partitions: this._options.partitions,
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

            return self.#doTaskAssignment(self._options.projectionGroup, allProjectionIdsInAllShards);
        })
        .then(function(...data) {
            callback(null, ...data);
        })
        .catch((err) => {
            console.error(err);
            callback();
        });
    }

    #doTaskAssignment = async function(projectionGroup, allProjectionsInAllShards) {
        const self = this;
        const tasks = [];

        for (let projectionId of allProjectionsInAllShards) {
            tasks.push(projectionId);
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
        // TODO: review projectionId, config
        // also review adding partitioning here
        const promises = [];
        for (const eventstore of this._eventstores) {
            for (let i = 0; i < eventstore.options.partitions; i++) {
                const projectionConfigClone = _.cloneDeep(projectionConfig);
                projectionConfigClone.projectionId = `${projectionConfigClone.projectionId}:shard${eventstore.options.shard}:partition${i}`;
                projectionConfigClone.shard = eventstore.options.shard;
                projectionConfigClone.partition = i;
                promises.push(eventstore.projectAsync(projectionConfigClone));
            }
        }

        Promise.all(promises)
        .then((data) => {
            callback();
        })
        .catch((err) => {
            console.error(err);
            callback();
        });
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
        const self = this;
        const promises = [];
        const newTasks = [];

        for (const eventstore of this._eventstores) {
            for (let i = 0; i < eventstore.options.partitions; i++) {
                let clonedProjectionId = _.clone(projectionId);
                clonedProjectionId = `${clonedProjectionId}:shard${eventstore.options.shard}:partition${i}`;
                newTasks.push(clonedProjectionId);
                promises.push(eventstore.runProjectionAsync(clonedProjectionId, forced));
            }
        }

        Promise.all(promises)
        .then(() => {
            if (self._taskGroup) {
                self._taskGroup.addTasks(newTasks);
            }
        })
        .then((data) => done(null, data))
        .catch((err) => {
            console.error(err);
            done();
        });
    }

    getProjection(projectionId, done) {
        const promises = [];

        for (const eventstore of this._eventstores) {
            for (let i = 0; i < eventstore.options.partitions; i++) {
                let clonedProjectionId = _.clone(projectionId);
                clonedProjectionId = `${clonedProjectionId}:shard${eventstore.options.shard}:partition${i}`;
                promises.push(eventstore.getProjectionAsync(clonedProjectionId));
            }
        }
        
        Promise.all(promises)
        .then((data) => done(null, data))
        .catch((err) => {
            console.error(err);
            done();
        });
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
        const self = this;
        const promises = [];

        for (const eventstore of this._eventstores) {
            for (let i = 0; i < eventstore.options.partitions; i++) {
                let clonedProjectionId = _.clone(projectionId);
                clonedProjectionId = `${clonedProjectionId}:shard${eventstore.options.shard}:partition${i}`;
                if(self._taskGroup) {
                    self._taskGroup.removeTasks([clonedProjectionId]);
                }
                promises.push(eventstore.deleteProjectionAsync(clonedProjectionId));
            }
        }
        
        Promise.all(promises)
        .then((data) => done(null, data))
        .catch((err) => {
            console.error(err);
            callback();
        });
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

    getLastEvent(query) {
        if (typeof query === 'string') {
            query = {
                aggregateId: query
            };
        }

        const aggregateId = query.aggregateId;
        this.#doOnShardedEventstore(aggregateId, 'getLastEvent', arguments);
    }

    getEvents(query, callback) {
        if (!query) {
            throw new Error('query should be defined');
        }

        if (isNaN(parseInt(query.shard))) {
            throw new Error('shard should be a number');
        }

        if (isNaN(parseInt(query.partition))) {
            throw new Error('partition should be a number');
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

    createSnapshot(query, callback) {
        const aggregateId = query.aggregateId;
        this.#doOnShardedEventstore(aggregateId, 'createSnapshot', arguments);
    }

    getUndispatchedEvents(query, callback) {
        if (typeof query === 'string') {
            query = {
                aggregateId: query
            };
        }

        const aggregateId = query.aggregateId;
        this.#doOnShardedEventstore(aggregateId, 'getUndispatchedEvents', arguments);
    }

    setEventToDispatched(event) {
        const aggregateId = event.aggregateId;
        this.#doOnShardedEventstore(aggregateId, 'setEventToDispatched', arguments);
    }

    getPlaybackList(listName) {
        this.#doOnAnyEventstore('getPlaybacklist', arguments)
    }

    getStateList(listName, state) {
        this.#doOnAnyEventstore('getStateList', arguments)
    }

    getPlaybackListView(listName, done) {
        this.#doOnAnyEventstore('getPlaybackListView', arguments)
    }

    registerPlaybackListView(listName, listQuery, totalCountQuery, opts, done) {
        this.#doOnAllEventstores('registerPlaybackListView', arguments);
    }

    registerFunction(functionName, theFunction) {
        this.#doOnAllEventstoresNoCallback('registerFunction', arguments);
    }

    unsubscribe(token) {
        this.#doOnAllEventstoresNoCallback('unsubscribe', arguments);
    }

    closeSubscriptionEventStreamBuffers(done) {
        this.#doOnAllEventstores('closeSubscriptionEventStreamBuffers', arguments);
    }

    closeProjectionEventStreamBuffers(done) {
        this.#doOnAllEventstores('closeProjectionEventStreamBuffers', arguments);
    }

    deactivatePolling() {
        this.#doOnAllEventstoresNoCallback('deactivatePolling', arguments);
    }

    activatePolling() {
        this.#doOnAllEventstoresNoCallback('activatePolling', arguments);
    }

    close(callback) {
        this.#doOnAllEventstores('close', arguments);
    }

    #doOnAnyEventstore(methodName, args) {
        const eventstore = this._eventstores[Math.random(this._options.numberOfShards)];
        eventstore[methodName].apply(eventstore, args)
    }

    #doOnAllEventstores(methodname, args) {
        const promises = [];
        const callback = args[args.length - 1];
        for (const eventstore of this._eventstores) {
            promises.push(eventstore[methodname + 'Async'].apply(eventstore, args));
        }
        Promise.all(promises).then(function(...data) {
            callback(null, ...data);
        }).catch(callback);
    }

    #doOnAllEventstoresNoCallback(methodName, args) {
        for (const eventstore of this._eventstores) {
            eventstore[methodName].apply(eventstore, args)
        }
    }

    #doOnShardedEventstore(aggregateId, methodName, args) {
        // NOTE: our usage always have aggregateId in query
        let shard = this.#getShard(aggregateId);
        const eventstore = this._eventstores[shard];
        return eventstore[methodName].apply(eventstore, args);
    }

    #getShard(aggregateId) {
        let shard = murmurhash(aggregateId) % this._options.numberOfShards;
        return shard;
    }

}
module.exports = ClusteredEventStore;