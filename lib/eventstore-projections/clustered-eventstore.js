const murmurhash = require('murmurhash');
const Bluebird = require('bluebird');
const _ = require('lodash');
const debug = require('debug')('eventstore:clustered');
const TaskAssignmentGroup = require('./task-assignment').TaskAssignmentGroup;
const EventstoreWithProjection = require('./eventstore-projection');


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

class ClusteredEventStore extends EventstoreWithProjection {
    constructor(opts, store, distributedSignal, distributedLock, playbackListStore, playbackListViewStore, projectionStore, stateListStore) {
        super(opts, store, distributedSignal, distributedLock, playbackListStore, playbackListViewStore, projectionStore, stateListStore);

        const defaultOptions = {
            partitions: 25
        };

        this._options = Object.assign(defaultOptions, opts);
        this._options.numberOfShards = this._options.clusters.length;

        this._eventstoreCount = 0;
        this._eventstores = [];
        this._distributedLock = distributedLock;
    }

    addEventstoreToCluster(eventstoreNode) {
        this._eventstores.push(eventstoreNode);
        this._eventstoreCount++;
        Bluebird.promisifyAll(eventstoreNode);
    }

    /**
     * Call this function to initialize the eventstore.
     * If an event publisher function was injected it will additionally initialize an event dispatcher.
     * @param {Function} callback the function that will be called when this action has finished [optional]
     */
    init(callback) {
        this._doOnAllEventstores('init', callback);
        this._listenAndForwardEmitOfAllEventstores();
    }

    /**
     * @param {DoneCallbackFunction} callback success/error callback
     * @returns {void} - returns void
     */
    startAllProjections(callback) {
        const self = this;
        const promises = [];

        for (const eventstore of self._eventstores) {
            promises.push(eventstore['startAllProjectionsAsync'].apply(eventstore, {}));
        }

        // Note: Convert to Async with _startAllProjections
        Promise.all(promises)
        .then(function() {
            return new Promise((resolve, reject) => {
                self.getProjectionTasks((error, projectionTasks) => {
                    if (error) {
                        reject(error);
                    }
                    resolve(projectionTasks);
                });
            })
        })
        .then((projectionTasks) => {
            const allProjectionTaskIdsInAllShards = [];
            for(const pj of projectionTasks) {
                allProjectionTaskIdsInAllShards.push(pj.projectionTaskId);
            }

            return self._doTaskAssignment(self._options.projectionGroup, allProjectionTaskIdsInAllShards);
        })
        .then(function(...data) {
            callback(null, ...data);
        })
        .catch((err) => {
            console.error(err);
            callback();
        });
    }

    async _doTaskAssignment(projectionGroup, allProjectionTaskIdsInAllShards) {
        const self = this;
        const tasks = [];

        for (let projectionTaskId of allProjectionTaskIdsInAllShards) {
            tasks.push(projectionTaskId);
        }

        // create TaskAssignmentGroup
        debug('CLUSTERED _doTaskAssignment', allProjectionTaskIdsInAllShards);
        if (self._taskGroup) {
            debug('CLUSTERED _doTaskAssignment: Has a prior task group');
            // await self._taskGroup.leave();
            await self._taskGroup.addTasks(tasks);
        } else {
            debug('CLUSTERED _doTaskAssignment: Has no prior task group');
            self._taskGroup = new TaskAssignmentGroup({
                initialTasks: tasks,
                createRedisClient: self._options.redisCreateClient,
                groupId: projectionGroup,
                membershipPollingTimeout: self._options.membershipPollingTimeout,
                distributedLock: self._distributedLock
            });

            debug('CLUSTERED _doTaskAssignment: Creating new task group');

            await self._taskGroup.join();
            debug('CLUSTERED _doTaskAssignmentL joined to group', projectionGroup);
            self._taskGroup.on('rebalance', async (updatedAssignments, rebalanceId) => {
                debug('CLUSTERED REBALANCE CALLED', updatedAssignments);
                // distribute to shards
                const distributedAssignments = {};
                for(const projectionTaskId of updatedAssignments) {
                    const splitProjectionTaskId = projectionTaskId.split(':');
                    if (splitProjectionTaskId.length > 1 && splitProjectionTaskId[1]) {
                        const shardString = splitProjectionTaskId[1];
                        const stringIndex = shardString.replace('shard', '');
                        const shard = parseInt(stringIndex);
                        if (!distributedAssignments[shard]) {
                            distributedAssignments[shard] = [];
                        }
                        distributedAssignments[shard].push(projectionTaskId);
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
                debug('CLUSTERED EMITTING REBALANCE', updatedAssignments);
                self.emit('rebalance', updatedAssignments);
            });
        }
    };

    /**
     * @param {ProjectionConfiguration} projectionConfig parameters for the projection
     * @param {Function} callback success/error callback
     * @returns {void} - returns void
     */
    project(projectionConfig, callback) {
        this._doOnAllEventstores('project', projectionConfig, callback);
    }

    /**
     * @param {DoneCallbackFunction} callback success/error callback
     * @returns {void} - returns void
     */
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
            callback();
        }).catch((err) => {
            console.error(err);
            callback();
        });
    }

    /**
     * 
     * @param {string} projectionId 
     * @returns {Promise<Projection>}
     */
    // getProjection(projectionId, done) {
    //     this._getProjection(projectionId).then((data) => {
    //         done(null, data);
    //     }).catch(done);
    // }

    // async _getProjection(projectionId) {
    //     const promises = [];

    //     if (this._eventstores && this._eventstores.length > 0) {
    //         return await this._eventstores[0]
    //     }
    //     for (const eventstore of this._eventstores) {
    //         const partitions = await this._getPartitionsAsync(eventstore);
    //         for (const partition of partitions) {
    //             let clonedProjectionId = _.clone(projectionId);
    //             clonedProjectionId = `${clonedProjectionId}:shard${eventstore.options.shard}:partition${partition}`;
    //             promises.push(eventstore.getProjectionAsync(clonedProjectionId));
    //         }
    //     }
        
    //     return await Promise.all(promises);
    // }

    /**
     * @returns {Promise<Array<Projection>} returns Promise of type void
     */
    // getProjections() {
    //     const self = this;
    //     const promises = [];
    //     let args = arguments;
    //     const done = args[args.length - 1];

    //     // NOTE: for review, getting projections of one shard gets all projections of all shards
    //     const eventstore = this._eventstores[0];
    //     promises.push(eventstore['getProjectionsAsync'].apply(eventstore, {}));

    //     Promise.all(promises)
    //     .then((data) => done(null, data))
    //     .catch((err) => {
    //         console.error(err);
    //         done();
    //     });
    // }

    /**
     * @param {AggregateQuery} query the query for the aggregate/category that we like to get
     * @param {Number} revision the revision from where we should start the subscription
     * @param {EventCallback} onEventCallback the callback to be called whenever an event is available for the subscription
     * @param {ErrorCallback} onErrorCallback the callback to be called whenever an error is thrown
     * @returns {String} - returns token
     */
    subscribe(query, revision, onEventCallback, onErrorCallback) {
        const aggregateId = query.aggregateId;
        return this._doOnShardedEventstore(aggregateId, 'subscribe', query, revision, onEventCallback, onErrorCallback);
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
        this._doOnAllEventstoresNoCallback('defineEventMappings', mappings);
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
        this._doOnAllEventstoresNoCallback('useEventPublisher', fn);
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
    getEvents(query, skip, limit, callback) {
        // TODO: Review, possible contract breach
        if (!query) {
            throw new Error('query should be defined');
        }

        if (isNaN(parseInt(query.shard))) {
            throw new Error('shard should be a number');
        }

        if (!query.partition) {
            throw new Error('partition should be defined');
        }

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

        try {
            let shard = query.shard;
            const eventstore = this._eventstores[shard];
            eventstore.getEventsAsync(query, skip, limit).then((data) => {
                callback(null, data);
            }).catch(callback);
        } catch (error) {
            callback(error);
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
        let shard = this._getShard(aggregateId);
        const eventstore = this._eventstores[shard];
        return eventstore[methodName](...args);
    }

    _getShard(aggregateId) {
        let shard = murmurhash(aggregateId, 1) % this._options.numberOfShards;
        return shard;
    }
}

var Eventstore = ClusteredEventStore,
    StoreEventEmitter = require('../../lib/storeEventEmitter');

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

    var dbPath = __dirname + "/../../lib/databases/" + options.type + ".js";

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
 * @param {EventstoreOptions} opts - The options
 * @returns {Eventstore} - eventstore with Projection
 */
const esFunction = function(opts) {
    let options = opts ? _.cloneDeep(opts) : {};

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
            const DistributedLock = require('../../lib/eventstore-projections/distributed-lock');
            distributedLock = new DistributedLock({
                redis: redisClient,
                lock: options.lock
            });

            // TO DO: Handle redis config input instead of createClient
            const DistributedSignal = require('../../lib/eventstore-projections/distributed-signal');
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
            const EventstorePlaybackListStore = require(`../../lib/eventstore-projections/playbacklist/eventstore-playbacklist-${type}-store`);
            playbackListStore = new EventstorePlaybackListStore(options.listStore);
            playbackListStore.init();

            const EventstoreStateListStore = require(`../../lib/eventstore-projections/state-list/databases/eventstore-statelist-${type}-store`);
            stateListStore = new EventstoreStateListStore(options.listStore);
            stateListStore.init();
        }

        if (options.projectionStore) {
            const type = options.projectionStore.type || defaultDataStore;
            const EventstoreProjectionStore = require(`../../lib/eventstore-projections/projection/eventstore-projection-${type}-store`);
            projectionStore = new EventstoreProjectionStore(options.projectionStore);
            projectionStore.init();
        }
    }

    options.shouldSkipSignalOverride = true;
    options.shouldDoTaskAssignment = false;
    var eventstore = new Eventstore(options, new Store(options), distributedSignal, distributedLock, playbackListStore, playbackListViewStore, projectionStore, stateListStore);

    if (options.emitStoreEvents) {
        var storeEventEmitter = new StoreEventEmitter(eventstore);
        storeEventEmitter.addEventEmitter();
    }

    options.clusters.forEach((storeConfig, index) => {
        let config = {
            type: storeConfig.type,
            host: storeConfig.host,
            port: storeConfig.port,
            user: storeConfig.user,
            password: storeConfig.password,
            database: storeConfig.database,
            connectionPoolLimit: storeConfig.connectionPoolLimit,
            shard: index,
            partitions: options.partitions,
            shouldDoTaskAssignment: false,
            shouldSkipSignalOverride: false
        };

        let esConfig = Object.assign(options, config);
        delete esConfig.clusters;

        if (!esConfig.outputsTo) {
            // NOTE: if no outputsTo is defined, use the cluster to output the events to
            esConfig.outputsTo = eventstore;
        }

        const Store = getSpecificStore(config);

        const EventstoreWithProjection = require('../../lib/eventstore-projections/eventstore-projection');
        const eventstoreNode = new EventstoreWithProjection(esConfig, new Store(config), distributedSignal, distributedLock, playbackListStore, playbackListViewStore, projectionStore, stateListStore);

        eventstore.addEventstoreToCluster(eventstoreNode);
    });

    return eventstore;
};

module.exports = esFunction;