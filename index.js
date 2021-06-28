/**
 * PlaybackListStoreConfig
 * @typedef {import('./lib/eventstore-projections/eventstore-projection').PlaybackListStoreConfig} PlaybackListStoreConfig
 */

const EventstoreWithProjection = require('./lib/eventstore-projections/eventstore-projection');

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

    try {
        Store = getSpecificStore(options);
    } catch (err) {
        throw err;
    }

    // TODO: move dependencies out of the options and move them as a parameter in the constructor of Eventstore
    let jobsManager;
    let distributedLock;
    let distributedSignal;
    let playbackListStore;
    let playbackListViewStore;
    let projectionStore;
    let stateListStore;

    if (options.enableProjection) {
        if (options.redisConfig) {
            const DistributedLock = require('./lib/eventstore-projections/distributed-lock');
            distributedLock = new DistributedLock({
                redis: options.redisConfig,
                lock: options.lock
            });

            const Redis = require('ioredis');
            const redisClient = new Redis(options.redisConfig);
            const redisSubscriber = new Redis(options.redisConfig);
            const createClient = function(type) {
                switch (type) {
                    case 'client':
                        return redisClient;
                    case 'bclient':
                        return new Redis(options.redisConfig); // always create a new one
                    case 'subscriber':
                        return redisSubscriber;
                    default:
                        return new Redis(options.redisConfig);
                }
            }
            const DistributedSignal = require('./lib/eventstore-projections/distributed-signal');
            distributedSignal = new DistributedSignal({
                createClient: createClient
            });

        } else {
            throw new Error('redisConfig is required when enabeProjection is true');
        }
    
        if (options.listStore) {
            // NOTE: we only have one store as of the moment. we can add more playbacklist stores in the future and pass it to the eventstore later
            // based on the listStore configuration
            // TODO: add base class for playbackliststore when there is a need to create another store in the future
            const EventstorePlaybackListMySqlStore = require('./lib/eventstore-projections/eventstore-playbacklist-mysql-store');
            playbackListStore = new EventstorePlaybackListMySqlStore(options.listStore);
            playbackListStore.init();

            const EventstoreStateListMySqlStore = require('./lib/eventstore-projections/state-list/databases/eventstore-statelist-mysql-store');
            stateListStore = new EventstoreStateListMySqlStore(options.listStore);
            stateListStore.init();
        }
    
        if (options.projectionStore) {
            const EventstoreProjectionStore = require('./lib/eventstore-projections/eventstore-projection-store');
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
