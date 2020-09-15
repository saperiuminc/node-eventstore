/**
 * PlaybackListStoreConfig
 * @typedef {import('./lib/eventstore-projections/eventstore-projection').PlaybackListStoreConfig} PlaybackListStoreConfig
 */

/**
 * EventStore
 * @typedef {Object} EventstoreOptions
 * @property {Number} pollingMaxRevisions maximum number of revisions to get for every polling interval
 * @property {Number} pollingTimeout timeout in milliseconds for the polling interval
 * @property {String} projectionGroup name of the projectionGroup if using projection
 * @property {PlaybackListStoreConfig} listStore
 */

var Eventstore = require('./lib/eventstore-projections/eventstore-projection'),
    Base = require('./lib/base'),
    _ = require('lodash'),
    debug = require('debug')('eventstore'),
    StoreEventEmitter = require('./lib/storeEventEmitter');

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
    let playbackListStore;
    let playbackListViewStore;
    if (options.redisConfig) {
        var Redis = require("ioredis");
        const redis = new Redis({
            host: options.redisConfig.host,
            password: options.redisConfig.password,
            port: options.redisConfig.port
        });

        const RedisLock = require('redlock');
        const redLock = new RedisLock([redis], {
            driftFactor: 0.01, // time in ms
            retryCount: 10,
            retryDelay: 200, // time in ms
            retryJitter: 200 // time in ms
        });

        const DistributedLock = require('./lib/eventstore-projections/distributed-lock');
        distributedLock = new DistributedLock({
            redis: redLock
        });

        const JobsManager = require('./lib/eventstore-projections/jobs-manager');
        jobsManager = new JobsManager({
            BullQueue: require('bull'),
            redis: redis
        });

        options.redis = redis;
    }

    if (options.listStore) {
        // NOTE: we only have one store as of the moment. we can add more playbacklist stores in the future and pass it to the eventstore later
        // based on the listStore configuration
        // TODO: add base class for playbackliststore when there is a need to create another store in the future
        const EventstorePlaybackListMySqlStore = require('./lib/eventstore-projections/eventstore-playbacklist-mysql-store');
        playbackListStore = new EventstorePlaybackListMySqlStore(options.listStore);
        playbackListStore.init(options.listStore);
    }

    var eventstore = new Eventstore(options, new Store(options), jobsManager, distributedLock, playbackListStore, playbackListViewStore);

    if (options.enableProjection === true) {
        eventstore.setupNotifyPublish();
    }

    if (options.emitStoreEvents) {
        var storeEventEmitter = new StoreEventEmitter(eventstore);
        storeEventEmitter.addEventEmitter();
    }

    return eventstore;
};

module.exports = esFunction;

module.exports.Store = Base;