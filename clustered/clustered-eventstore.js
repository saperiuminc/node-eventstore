const util = require('util');
const murmurhash = require('murmurhash');
const Bluebird = require('bluebird');
const _ = require('lodash');

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
                connectionPoolLimit: storeConfig.connectionPoolLimit
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
        this.#doOnAllEventstores('startAllProjection', arguments);
    }

    project() {
        this.#doOnAllEventstores('project', arguments);
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

    # doOnAnyEventstore(methodName, args) {
        const eventstore = this._eventstores[Math.random(this._options.numberOfShards)];
        eventstore[methodName].apply(eventstore, args)
    }

    # doOnAllEventstores(methodname, args) {
        const promises = [];
        const callback = args[args.length - 1];
        for (const eventstore of this._eventstores) {
            promises.push(eventstore[methodname].apply(eventstore, args));
        }
        Promise.all(promises).then(function(...data) {
            callback(null, ...data);
        }).catch(callback);
    }

    # doOnAllEventstoresNoCallback(asyncMethodName, args) {
        for (const eventstore of this._eventstores) {
            eventstore[asyncMethodName].apply(eventstore, args)
        }
    }

    # doOnShardedEventstore(aggregateId, methodName, args) {
        // NOTE: our usage always have aggregateId in query
        let shard = this.#getShard(aggregateId);
        const eventstore = this._eventstores[shard];
        return eventstore[methodName].apply(eventstore, args);
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


    # getShard(aggregateId) {
        let shard = murmurhash(aggregateId) % this._options.numberOfShards;
        return shard;
    }
}
module.exports = ClusteredEventStore;