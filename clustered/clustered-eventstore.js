const util = require('util');
const murmurhash = require('murmurhash');
const Bluebird = require('bluebird');
const _ = require('lodash');

class ClusteredEventStore {
    constructor(options, mappingStore) {
        this._mappingStore = mappingStore;

        const defaults = {
            type: 'clusteredMysql',
            clusteredStores: [],
            numberOfPartitions: 80
        };
    
        this._options = this.options = _.defaults(options, defaults);
        this._options.numberOfShards = this._options.clusteredStores.length;

        this._eventstores = [];
    }

    init(callback) {
        try {
            const promises = [];
            this.options.clusteredStores.forEach((storeConfig, index) => {
                let config = {
                    host: storeConfig.host,
                    port: storeConfig.port,
                    user: storeConfig.user,
                    password: storeConfig.password,
                    database: storeConfig.database,
                    connectionPoolLimit: storeConfig.connectionPoolLimit,
                };
                let esConfig = _.defaults(config, _.cloneDeep(this._options));
                delete esConfig.clusteredStores;

                const eventstore = require('../index')(esConfig);
                Bluebird.promisifyAll(eventstore);
                this._eventstores[`shard_${index}`] = eventstore;

                promises.push(eventstore.initAsync());
            });

            Promise.all(promises).then(callback).catch(callback);
        } catch (error) {
            callback(error);
        }
    }

    startAllProjections(callback) {
        this._doOnAllEventstore('startAllProjectionAsync', arguments);
    }

    project() {
        this._doOnAllEventstore('projectAsync', arguments);
    }

    subscribe(query, revision, onEventCallback, onErrorCallback) {
        const aggregateId = query.aggregateId;

        this._doOnShardedEventstore(aggregateId, 'subscribe', arguments);
    }

    defineEventMappings() {
        this._doOnAllEventstoreNoCallback('defineEventMappings', arguments);
    }

    useEventPublisher() {
        this._doOnAllEventstoreNoCallback('useEventPublisher', arguments);
    }

    getLastEvent(query, callback) {
        if (typeof query === 'string') {
            query = { aggregateId: query };
        }

        const aggregateId = query.aggregateId;
        this._doOnShardedEventstore(aggregateId, 'getLastEvent', arguments);
    }

    getLastEventAsStream(query, callback) {
        if (typeof query === 'string') {
            query = { aggregateId: query };
        }

        const aggregateId = query.aggregateId;
        this._doOnShardedEventstore(aggregateId, 'getLastEventAsStream', arguments);
    }

    getEventStream(query, revMin, revMax, callback) {
        if (typeof query === 'string') {
            query = { aggregateId: query };
        }

        const aggregateId = query.aggregateId;
        this._doOnShardedEventstore(aggregateId, 'getEventStream', arguments);
    }

    getFromSnapshot(query, revMax, callback) {
        if (typeof query === 'string') {
            query = { aggregateId: query };
        }

        const aggregateId = query.aggregateId;
        this._doOnShardedEventstore(aggregateId, 'getFromSnapshot', arguments);
    }

    createSnapshot(obj, callback) {
        const aggregateId = obj.aggregateId;
        this._doOnShardedEventstore(aggregateId, 'createSnapshot', arguments);
    }

    getUndispatchedEvents(query, revMax, callback) {
        if (typeof query === 'string') {
            query = { aggregateId: query };
        }

        const aggregateId = query.aggregateId;
        this._doOnShardedEventstore(aggregateId, 'getUndispatchedEvents', arguments);
    }

    setEventToDispatched() {
        this._doOnAllEventstore('setEventToDispatched', arguments);
    }

    _doOnAnyEventstore(methodName, args) {
        const eventstore = this._eventstores[`shard_${Math.random(this.options.numberOfShards)}`];
        eventstore[methodName].apply(args)
    }

    _doOnAllEventstore(asyncMethodName, args) {
        const promises = [];
        const callback = args[args.length-1];
        const arg = args.splice(0, args.length-1);
        for (const eventstore in this._eventstores) {
            promises.push(eventstore[asyncMethodName].apply(arg));
        }
        Promise.all(promises).then(callback).catch(callback);
    }

    _doOnAllEventstoreNoCallback(asyncMethodName, args) {
        for (const eventstore in this._eventstores) {
            eventstore[asyncMethodName].apply(args)
        }
    }

    _doOnShardedEventstore(aggregateId, methodName, args) {
        // NOTE: our usage always have aggregateId in query
        let shardPartition = await this.getShardAndPartition(aggregateId);

        if (!shardPartition) {
            // numberOfPartitions = 80
            // 0 - 79
            const partition = this.getPartition(aggregateId, numberOfPartitions);

            // 10 % 4 == 2
            const shard = partition % this._options.numberOfShards;

            shardPartition = {
                shard: shard,
                partition: partition
            }

            await this._mappingStore.addShardPartition(aggregateId, shardPartition);
        }

        const eventstore = this._eventstores[shardId];
        return eventstore[methodName].apply(args);
    }

    /* EVENTSTORE.JS */

    // // for all shards
    // defineEventMappings - DONE
    // useEventPublisher - DONE
    // setEventToDispatched - DONE

    // // for specific shard
    // getLastEvent - DONE
    // getLastEventAsStream - DONE
    // getEventStream - DONE
    // getFromSnapshot - DONE
    // createSnapshot - DONE
    // getUndispatchedEvents - DONE

    /* EVENTSTORE-PROJECTION.JS */

    // deactivatePolling
    // activatePolling
    // close
    // stopAllProjections
    // closeSubscriptionEventStreamBuffers
    // closeProjectionEventStreamBuffers
    // // startAllProjections
    // // project
    // unsubscribe // unique token
    // registerPlaybackListView
    // registerFunction

    // // for first/any shard
    // getPlaybackList
    // getStateList
    // getPlaybackListView
    // getProjections
    // getProjection
    // getProjectionByName
    // pauseProjection
    // resetProjection
    // deleteProjection

    // // for specific shard
    // subscribe

    // // undecided
    // runProjection // taskGroup


    async getShardAndPartition(aggregateId) {
        let shard = await this._mappingStore.getShard(aggregateId);
        return shard;
    }

    getPartition(aggregateId, numberOfPartitions) {
        const partition = murmurhash(aggregateId) % numberOfPartitions;
        return partition;
    }

    getEventstream(query, revMin, revMax, callback) {
        this._getEventStream(query, revMin, revMax).then((data) => callback(data)).catch(callback);
    }

    async _getEventStream(query, revMin, revMax) {
        const aggregateId = query.aggregateId;

        // NOTE: our usage always have aggregateId in query
        let shardPartition = await this.getShardAndPartition(aggregateId);

        if (!shardPartition) {
            // numberOfPartitions = 80
            // 0 - 79
            const partition = this.getPartition(aggregateId, numberOfPartitions);

            // 10 % 4 == 2
            const shard = partition % this._options.numberOfShards;

            shardPartition = {
                shard: shard,
                partition: partition
            }

            await this._mappingStore.addShardPartition(aggregateId, shardPartition);
        }

        const getEventStream = util.promisify(this._eventstores[shardPartition.shard].getEventstream);

        return getEventStream(query, revMin, revMax);
    }

}

module.exports = ClusteredEventStore;