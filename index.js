/**
 * PlaybackListStoreConfig
 * @typedef {import('./lib/eventstore-projections/eventstore-projection').PlaybackListStoreConfig} PlaybackListStoreConfig
 */

 /**
 * ProjectionStoreConfig
 * @typedef {import('./lib/eventstore-projections/eventstore-projection').ProjectionStoreConfig} ProjectionStoreConfig
 */

/**
 * EventStore
 * @typedef {Object} EventstoreOptions
 * @property {Number} pollingMaxRevisions maximum number of revisions to get for every polling interval
 * @property {Number} pollingTimeout timeout in milliseconds for the polling interval
 * @property {String} projectionGroup name of the projectionGroup if using projection
 * @property {Number} concurrentProjectionGroup number of concurrent running projections per projectionGroup
 * @property {PlaybackListStoreConfig} listStore
 * @property {ProjectionStoreConfig} projectionStore
 * @property {String} eventNameFieldName the field name of the event's name in the payload. Default is "name"
 * @property {String} stateContextName the name of the context when a state is created by a projection. default is 'default'
 * @property {Number} concurrentAggregatesInProjection number of concurrent aggregates running in the projection job
 * @property {Boolean} shouldExhaustAllEvents tells the projection if it should exhaust all the events when a projection job is triggered.
 * @property {String} context the context name of this eventstore. default context name is "default"
 * @property {Number} lockTimeToLive the ttlDuration of the lock. used by leader election code
 * @property {Eventstore} outputsTo the eventstore where emits and states are outputted to. default is itself
 */

var Eventstore = require('./lib/eventstore-projections/eventstore-projection'),
    Base = require('./lib/base'),
    _ = require('lodash'),
    debug = require('debug')('eventstore'),
    StoreEventEmitter = require('./lib/storeEventEmitter');

// add tracing
require('./lib/eventstore-projections/tracing/eventstore-tracing');

function exists(toCheck) {
    var _exists = require('fs').existsSync || require('path').existsSync;
    if (require('fs').accessSync) {
        _exists = function(toCheck) {
            try {
                require('fs').accessSync(toCheck);
                return true;
            } catch (e) {
                return false;
            }
        };
    }
    return _exists(toCheck);
}

function getSpecificStore(options) {
    options = options || {};

    options.type = options.type || 'inmemory';

    if (_.isFunction(options.type)) {
        return options.type;
    }

    options.type = options.type.toLowerCase();

    var dbPath = __dirname + "/lib/databases/" + options.type + ".js";

    if (!exists(dbPath)) {
        var errMsg = 'Implementation for db "' + options.type + '" does not exist!';
        debug(errMsg);
        throw new Error(errMsg);
    }

    try {
        debug('dbPath:', dbPath);
        var db = require(dbPath);
        return db;
    } catch (err) {

        console.error('error in requiring store');
        console.error(err);
        if (err.message.indexOf('Cannot find module') >= 0 &&
            err.message.indexOf("'") > 0 &&
            err.message.lastIndexOf("'") !== err.message.indexOf("'")) {

            var moduleName = err.message.substring(err.message.indexOf("'") + 1, err.message.lastIndexOf("'"));
            var msg = 'Please install module "' + moduleName +
                '" to work with db implementation "' + options.type + '"!';
            debug(msg);
        }

        throw err;
    }
}

/**
 * @param {EventstoreOptions} options - The options
 * @returns {Eventstore} - eventstore with Projection
 */
const esFunction = function(options) {
    options = options || {};

    var Store;

    // eslint-disable-next-line no-useless-catch
    try {
        Store = getSpecificStore(options);
    } catch (err) {
        throw err;
    }

    // TODO: move dependencies out of the options and move them as a parameter in the constructor of Eventstore
    // let jobsManager;
    let distributedLock;
    let distributedSignal;
    let playbackListStore;
    let playbackListViewStore;
    let projectionStore;
    let stateListStore;

    if (options.enableProjection) {
        // TO DO: Find the best way to pass options params
        // options.redisConfig
        if (options.redisCreateClient) {
            const redisCreateClient = options.redisCreateClient;
            const redisClient = redisCreateClient('client');
            const DistributedLock = require('./lib/eventstore-projections/distributed-lock');
            distributedLock = new DistributedLock({
                redis: redisClient,
                lock: options.lock
            });

            // TO DO: Handle redis config input instead of createClient
            const DistributedSignal = require('./lib/eventstore-projections/distributed-signal');
            distributedSignal = new DistributedSignal({
                createClient: redisCreateClient,
                ttlDuration: options.lockTimeToLive,
                processingTimeout: options.eventCallbackTimeout
            });
        } else {
            throw new Error('redisCreateClient is required when enableProjection is true');
        }

        const defaultDataStore = 'mysql';
        if (options.listStore) {
            // NOTE: we only have one store as of the moment. we can add more playbacklist stores in the future and pass it to the eventstore later
            // based on the listStore configuration
            // TODO: add base class for playbackliststore when there is a need to create another store in the future
            const type = options.listStore.type || defaultDataStore;
            const EventstorePlaybackListStore = require(`./lib/eventstore-projections/playbacklist/eventstore-playbacklist-${type}-store`);
            playbackListStore = new EventstorePlaybackListStore(options.listStore);
            playbackListStore.init();

            const EventstoreStateListStore = require(`./lib/eventstore-projections/state-list/databases/eventstore-statelist-${type}-store`);
            stateListStore = new EventstoreStateListStore(options.listStore);
            stateListStore.init();
        }
    
        if (options.projectionStore) {
            const type = options.projectionStore.type || defaultDataStore;
            const EventstoreProjectionStore = require(`./lib/eventstore-projections/projection/eventstore-projection-${type}-store`);
            projectionStore = new EventstoreProjectionStore(options.projectionStore);
            projectionStore.init();
        }
    }

    var eventstore = new Eventstore(options, new Store(options), distributedSignal, distributedLock, playbackListStore, playbackListViewStore, projectionStore, stateListStore);

    if (options.emitStoreEvents) {
        var storeEventEmitter = new StoreEventEmitter(eventstore);
        storeEventEmitter.addEventEmitter();
    }

    return eventstore;
};

module.exports = esFunction;

module.exports.Store = Base;
