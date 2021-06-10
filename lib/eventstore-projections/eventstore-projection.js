const Eventstore = require('../eventstore');
const _ = require('lodash');
const util = require('util');
const shortid = require('shortid');
const debug = require('debug')('eventstore:projection');
const async = require('async');
const StreamBuffer = require('../eventStreamBuffer');
const BufferedEventSubscription = require('../bufferedEventSubscription');
const v8 = require('v8');
const EventstoreStateList = require('./eventstore-state-list');

const STREAM_BUFFER_MEMORY_THRESHOLD = process.env.STREAM_BUFFER_MEMORY_THRESHOLD || 0.8; // Defaults to 80%
const STREAM_BUFFER_MAX_CAPACITY = process.env.STREAM_BUFFER_MAX_CAPACITY || 10;
const STREAM_BUFFER_POOL_MAX_CAPACITY = process.env.STREAM_BUFFER_POOL_MAX_CAPACITY || 5;
const STREAM_BUFFFER_TTL = process.env.STREAM_BUFFFER_TTL || (1000 * 60 * 60 * 24); // Default is 1 day
const timing = require('debug')('eventstore:projection:timing');

/**
 * DistributedSignal
 * @typedef {import('./distributed-signal').DistributedSignal} DistributedSignal
 */

/**
 * EventstorePlaybackListField
 * @typedef {import('./eventstore-playback-list').EventstorePlaybackListField} EventstorePlaybackListField
 */

/**
 * EventstorePlaybackListSecondaryKey
 * @typedef {import('./eventstore-playback-list').EventstorePlaybackListSecondaryKey} EventstorePlaybackListSecondaryKey
 */

/**
 * PlaybackListConfig
 * @typedef {Object} PlaybackListConfig
 * @property {String} name The name of the playback list
 * @property {EventstorePlaybackListField[]} fields The fields
 * @property {Object.<string, EventstorePlaybackListSecondaryKey[]>} secondaryKeys the secondary keys that make up the non clustered index. A key value pair were key is the secondaryKey name and value an array of fields
 */

/**
 * StateListConfig
 * @typedef {Object} StateListConfig
 * @property {String} name The name of the state list
 */

/**
 * PlaybackListStoreConfig
 * @typedef {Object} ListStoreConfig
 * @property {String} host The name of the playback list
 * @property {Number} port The name of the playback list
 * @property {String} database The name of the playback list
 * @property {String} user The name of the playback list
 * @property {String} password The name of the playback list
 */

/**
 * ProjectionStoreConfig
 * @typedef {Object} ProjectionStoreConfig
 * @property {String} host 
 * @property {Number} port
 * @property {String} database
 * @property {String} user
 * @property {String} password
 * @property {String} name The name of the store list
 */

/**
 * EventstoreProjectionOptions
 * @typedef {Object} EventstoreProjectionOptions
 * @property {DistributedLock} distributedLock Distributed lock helper
 * @property {DistributedSignal} distributedSignal DistributedSignal helper
 * @property {Number} pollingMaxRevisions maximum number of revisions to get for every polling interval
 * @property {Number} pollingTimeout timeout in milliseconds for the polling interval
 * @property {String} projectionGroup name of the projectionGroup if using projection
 * @property {String} eventNameFieldName the field name of the event's name in the payload. Default is "name"
 * @property {ListStoreConfig} listStore the playback list store config options
 * @property {Number} playbackEventJobCount number of concurrent playback event jobs running in parallel
 * @property {Boolean} shouldExhaustAllEvents tells the projection if it should exhaust all the events when a projection job is triggered.
 * @property {String} context the context name of this eventstore. default context name is "default"
 * @property {EventstoreWithProjection} outputsTo the eventstore where emits and states are outputted to. default is itself
 */

/**
 * Event
 * @typedef {Object} Event
 * @property {String} id Payload of the event
 * @property {String} context Payload of the event
 * @property {Object} payload Payload of the event
 * @property {String} commitId Payload of the event
 * @property {Number} position Payload of the event
 * @property {String} streamId Payload of the event
 * @property {String} aggregate Payload of the event
 * @property {String} aggregateId Payload of the event
 * @property {Date} commitStamp Payload of the event
 * @property {Number} commitSequence Payload of the event
 * @property {Number} streamRevision Payload of the event
 * @property {Number} restInCommitStream Payload of the event
 * @property {Number} eventSequence the sequence of the event in the store
 */

/**
 * EventStream
 * @typedef {Object} EventStream
 * @property {Event[]} events The events of this stream
 * @property {Function} addEvent Add the event to the stream
 */

/**
 * ProjectionState
 * @typedef {Object} ProjectionState
 * @property {String} aggregate 
 * @property {String} aggregateId 
 * @property {String} context 
 * @property {Object} payload 
 */

/**
 * DoneCallbackFunction
 * @callback DoneCallbackFunction
 * @param {Error} error Error if any
 */

/**
 * ProjectionPlaybackFunction
 * @callback ProjectionPlaybackFunction
 * @param {Event} event The event to playback
 * @param {DoneCallbackFunction} done Callback to tell that playback is done consuming the event
 * @returns {void} Returns void
 */

/**
 * ProjectionPlaybackInterface
 * @typedef ProjectionPlaybackInterface
 * @property {Function} $init The function used to initialize the projection's state
 */

/**
 * ProjectionParams
 * @typedef {Object} Projection
 * @property {String} projectionId The unique projectionId of the projection
 * @property {String} projectionName The unique projectionName of the projection
 * @property {AggregateQuery} query The query to use to get the events for the projection
 * @property {ProjectionPlaybackInterface} playbackInterface The object to use for when an event for this projection is received
 * @property {Object} meta Optional user meta data
 * @property {("stream"|"")} partitionBy Partition the state by stream, using a function or no partition. If outputState is false, then this option is ignored
 * @property {("true"|"false")} outputState Saves the projection state as a stream of states. Default is false
 * @property {PlaybackListConfig} playbackList playback list configuration for the projection
 * @property {StateListConfig} stateList playback list configuration for the projection
 * @property {Number} processedDate the date this projection was last processed
 * @property {Number} offset the last offset saved in the projection
 */

/**
 * EventCallback
 * @typedef {Object} EventCallback
 * @property {Error} error Error if there is any. null if no error happens
 * @property {Event} event The new event for the subscription
 */

/**
 * AggregateQuery
 * @typedef {Object} AggregateQuery
 * @property {String} aggregate The aggregate
 * @property {String} aggregateId The aggregateId
 * @property {String} context The context
 * @property {String} streamId The streamId
 */

/**
 * StreamBuffer
 * @typedef {import('../eventStreamBuffer')} StreamBuffer
 */

/**
 * Alias
 * @typedef {Object} Alias
 * @property {String} key Alias key
 * @property {String} value Alias value
 */



/**
 * EventstoreWithProjection constructor
 * @param {EventstoreProjectionOptions} options additional options for the Eventstore projection extension
 * @constructor
 */

function EventstoreWithProjection(opts, store, distributedSignal, distributedLock, playbackListStore, playbackListViewStore, projectionStore, stateListStore) {
    const self = this;

    const defaults = {
        pollingMaxRevisions: 5,
        pollingTimeout: 1000,
        eventCallbackTimeout: 10000,
        lockTimeToLive: 1000,
        projectionGroup: 'default',
        eventNameFieldName: 'name',
        context: 'default',
        enableProjection: false,
        playbackEventJobCount: 10,
        outputsTo: this,
        shouldExhaustAllEvents: true
    };

    const options = _.defaults(opts, defaults);

    if (options.enableProjection) {
        self._outputEventstore = options.outputsTo;
        self._context = options.context;
        self._commitRedisChannel = `context:${options.context}:channel:commited`;
        self._completedJobRedisChannel = `projection-group:${options.projectionGroup}:channel:completed`

        self._processGetEventsJobQueues = {};
        self._distributedSignal = distributedSignal;
        self._distributedLock = distributedLock;
        self._playbackListStore = playbackListStore;
        self._projectionStore = projectionStore;
        self._playbackListViewStore = playbackListViewStore;
        self._stateListStore = stateListStore;

        self.pollingActive = true;
        self._subscriptions = {};
        self._projections = {};
        self._playbackLists = {};
        self._stateLists = {};
        self._playbackListViews = {};
        self._jobs = {};
        self._userDefinedFunctions = {};
        self._projectionGroupedSubscriptions = {};

        // Initialize Poller Pool and Token-Channel Map
        self._bufferedEventSubscriptionPool = {};
        self._tokenChannels = {};

        // Initialize Stream Buffer
        self._streamBuffers = {};
        // self._streamBuffersSubscriptions = {};
        self._streamBufferBuckets = {};
        self._streamBufferLRULocked = false;

        self._streamBufferLRUCleaner = async.queue(function(task, callback) {
            const channel = task.channel;

            const bufferedEventSubscription = self._bufferedEventSubscriptionPool[channel];
            if (bufferedEventSubscription && bufferedEventSubscription.isActive()) {
                debug('Clearing Stream Buffer only', channel);
                // There's an active buffered event subscription, do not cleanup the entire stream buffer. Instead, clear its contents only.
                self._streamBuffers[channel].clear();

                // Update the bucket of the buffer
                const currentBucket = self._streamBuffers[channel].bucket;
                const newBucket = self._getCurrentStreamBufferBucket();
                if (self._streamBufferBuckets[currentBucket]) {
                    delete self._streamBufferBuckets[currentBucket][channel];
                }

                if (!self._streamBufferBuckets[newBucket]) {
                    self._streamBufferBuckets[newBucket] = {};
                }

                self._streamBufferBuckets[newBucket][channel] = new Date().getTime();
                self._streamBuffers[channel].bucket = newBucket;
            } else {
                // There is no active buffered event subscriptions. The stream buffer can safely be closed.
                debug('Closing the Stream Buffer', channel);
                self._streamBuffers[channel].close();

                // Close the bufferedEventSubscription also if it's defined but not active
                if (bufferedEventSubscription) {
                    bufferedEventSubscription.close();

                    delete self._bufferedEventSubscriptionPool[channel];
                }
            }
            callback();
        });

        self._streamBufferLRUCleaner.drain = function() {
            if (global.gc) {
                global.gc();
            }

            const heapPercentage = self._getHeapPercentage();

            if (heapPercentage >= STREAM_BUFFER_MEMORY_THRESHOLD) {
                self._cleanOldestStreamBufferBucket();
            } else {
                self._streamBufferLRULocked = false;
            }
        };

        if (self.commit) {
            var originalMethod = self.commit;
            self.commit = function(eventstream, callback) {
                let args = [eventstream];
                args = _.without(args, undefined, null);
                callback = self._enhanceCallback(self, callback, function() {
                    const query = self._getQuery(args[0]);
                    if (query) {
                        const channels = self._queryToChannels(query);
                        timing("commit monkeypatch: ", channels, query, new Date());
                        _.each(channels, (channel) => {
                            // async signal
                            self._distributedSignal.signal(channel);
                        })
                    }
                });
                return originalMethod.apply(self, _.concat(args, callback || []));
            };
        }
    }

    debug('event store created with options:', options);
    Eventstore.call(self, options, store);
}

util.inherits(EventstoreWithProjection, Eventstore);

EventstoreWithProjection.prototype = Eventstore.prototype;
EventstoreWithProjection.prototype.constructor = EventstoreWithProjection;

/**
 * @type {EventstoreWithProjection}
 */
EventstoreWithProjection.prototype._outputEventstore;

/**
 * @type {EventstoreProjectionOptions}
 */
EventstoreWithProjection.prototype.options;


/**
 * @type {String}
 */
EventstoreWithProjection.prototype._completedJobRedisChannel;

/**
 * @type {String}
 */
EventstoreWithProjection.prototype._context;

/**
 * @type {Projection[]}
 */
EventstoreWithProjection.prototype._projections;

/**
 * @type {EventstorePlaybackList[]}
 */
EventstoreWithProjection.prototype._playbackLists;

/**
 * @type {EventStoreStateList[]}
 */
EventstoreWithProjection.prototype._stateLists;

/***************************************************************************************************
    PUBLIC METHODS 
***************************************************************************************************/

/**
 * @returns {void} - returns void
 */
EventstoreWithProjection.prototype.deactivatePolling = function() {
    const pollers = Object.values(this._bufferedEventSubscriptionPool);
    for (let i = 0; i < pollers.length; i++) {
        pollers[i].deactivate();
    }

    // this.pollingActive = false;
};

/**
 * @returns {void} - returns void
 */
EventstoreWithProjection.prototype.activatePolling = function() {
    const pollers = Object.values(this._bufferedEventSubscriptionPool);
    for (let i = 0; i < pollers.length; i++) {
        pollers[i].activate();
    }

    // this.pollingActive = true;
};

/**
 * @param {DoneCallbackFunction} callback success/error callback
 * @returns {void} - returns void
 */
EventstoreWithProjection.prototype.startAllProjections = function(callback) {
    debug('startAllProjections called');
    // just preserving that adrai's public functions are callback based but internally we can just use async/await for elegance
    this._startAllProjections().then(callback).catch(callback);
};

EventstoreWithProjection.prototype._startAllProjections = async function() {
    try {
        debug('_startAllProjections called');

        if (this.options.enableProjection === false) {
            throw new Error('enableProjection true is required with startAllProjections');
        }

        const allProjections =  await this._getProjections();

        for (let index = 0; index < allProjections.length; index++) {
            const projection = allProjections[index];
            if (!this._projections[projection.projectionId]) {
                // TODO: need to create a function that gets called to initialize in memory dictionaries
                // currently project and this function is a little coupled
                // initialize the playback list. make sure that it is also inside the lock
                await this._initPlaybackList(projection);

                // initialize the playback list. make sure that it is also inside the lock
                await this._initStateList(projection);

                await this._startWaitingForSignals(projection.projectionId);
            }
        }
        
        // start leader election
        this._doLeaderElection(this.options.projectionGroup);
    } catch (error) {
        console.error('error in _startAllProjections with params and error:', error);
        throw error;
    }
};

/**
 * @param {Projection} projection parameters for the projection
 * @param {Function} callback success/error callback
 * @returns {void} - returns void
 */
EventstoreWithProjection.prototype.project = function(projection, callback) {
    debug('projection called with params:', projection);
    // just preserving that adrai's public functions are callback based but internally we can just use async/await for elegance
    this._project(projection).then(callback).catch(callback);
};

/**
 * @param {AggregateQuery} query the query for the aggregate/category that we like to get
 * @param {Number} revision the revision from where we should start the subscription
 * @param {EventCallback} onEventCallback the callback to be called whenever an event is available for the subscription
 * @param {ErrorCallback} onErrorCallback the callback to be called whenever an error is thrown
 * @returns {String} - returns token
 */
EventstoreWithProjection.prototype.subscribe = function(query, revision, onEventCallback, onErrorCallback) {
    const self = this;
    self._hasSubscriptions = true;

    try {
        debug('subscribe called with params: ', query, revision);

        if (this.options.enableProjection === false) {
            throw new Error('enableProjection true is required when subscribing');
        }

        let targetQuery = query;
        if (typeof query === 'string' || query instanceof String) {
            // just use AggregateQuery type when passed is a string
            targetQuery = {
                aggregateId: query
            }
        }

        if (!targetQuery) {
            throw new Error('query is required');
        }

        if (!targetQuery.aggregateId && !targetQuery.streamId) {
            throw new Error('aggregateId or streamId should be present in query');
        }

        const targetOffset = parseInt(revision);
        if (isNaN(targetOffset) || targetOffset < 0) {
            throw new Error('offset should be greater than or equal to 0');
        }

        const token = shortid.generate();

        self._subscriptions[token] = {
            query: targetQuery,
            offset: revision
        };

        const channel = self._getChannel(targetQuery);
        if (!self._streamBuffers[channel]) {
            const bucket = self._getCurrentStreamBufferBucket();

            const options = {
                es: self,
                query: targetQuery,
                channel: channel,
                bucket: bucket,
                bufferCapacity: STREAM_BUFFER_MAX_CAPACITY,
                poolCapacity: STREAM_BUFFER_POOL_MAX_CAPACITY,
                ttl: STREAM_BUFFFER_TTL,
                onOfferEvent: function(currentBucket, channel) {
                    if (!self._streamBufferLRULocked) {
                        const heapPercentage = self._getHeapPercentage();
                        debug('HEAP %', heapPercentage);
                        if (heapPercentage >= STREAM_BUFFER_MEMORY_THRESHOLD) {
                            self._cleanOldestStreamBufferBucket();
                        }
                    }

                    const newBucket = self._getCurrentStreamBufferBucket();
                    if (self._streamBufferBuckets[currentBucket]) {
                        delete self._streamBufferBuckets[currentBucket][channel];
                    }

                    if (!self._streamBufferBuckets[newBucket]) {
                        self._streamBufferBuckets[newBucket] = {};
                    }

                    self._streamBufferBuckets[newBucket][channel] = new Date().getTime();
                    self._streamBuffers[channel].bucket = newBucket;
                },
                onInactive: (bucket, channel) => {
                    const bufferedEventSubscription = self._bufferedEventSubscriptionPool[channel];
                    if (bufferedEventSubscription && bufferedEventSubscription.isActive()) {
                        debug('Clearing Stream Buffer only', channel);
                        // There's an active buffered event subscription, do not cleanup the entire stream buffer. Instead, clear its contents only.
                        self._streamBuffers[channel].clear();

                        // Update the bucket of the buffer
                        const currentBucket = self._streamBuffers[channel].bucket;
                        const newBucket = self._getCurrentStreamBufferBucket();
                        if (self._streamBufferBuckets[currentBucket]) {
                            delete self._streamBufferBuckets[currentBucket][channel];
                        }

                        if (!self._streamBufferBuckets[newBucket]) {
                            self._streamBufferBuckets[newBucket] = {};
                        }

                        self._streamBufferBuckets[newBucket][channel] = new Date().getTime();
                        self._streamBuffers[channel].bucket = newBucket;
                    } else {
                        // There is no active buffered event subscriptions. The stream buffer can safely be closed.
                        debug('Closing the Stream Buffer', channel);
                        self._streamBuffers[channel].close();

                        // Close the bufferedEventSubscription also if it's defined but not active
                        if (bufferedEventSubscription) {
                            bufferedEventSubscription.close();

                            delete self._bufferedEventSubscriptionPool[channel];
                        }
                    }
                },
                onClose: (bucket, channel) => {
                    delete self._streamBuffers[channel];
                    // delete self._streamBuffersSubscriptions[channel];

                    if (self._streamBufferBuckets[bucket]) {
                        delete self._streamBufferBuckets[bucket][channel];
                    }
                }
            }

            self._streamBuffers[channel] = new StreamBuffer(options);

            if (!self._streamBufferBuckets[bucket]) {
                self._streamBufferBuckets[bucket] = {};
            }

            self._streamBufferBuckets[bucket][channel] = new Date().getTime();
        }

        if (!self._bufferedEventSubscriptionPool[channel]) {
            const pollerOptions = {
                es: self,
                distributedSignal: self._distributedSignal,
                streamBuffer: self._streamBuffers[channel],
                query: targetQuery,
                channel: channel,
                eventCallbackTimeout: self.options.eventCallbackTimeout,
                pollingTimeout: self.options.pollingTimeout,
                pollingMaxRevisions: self.options.pollingMaxRevisions,
                errorMaxRetryCount: self.options.errorMaxRetryCount || 5,
                errorRetryExponent: self.options.errorRetryExponent || 2
            };
            debug('CREATING NEW BufferedEventSubscription FOR: ', channel);
            self._bufferedEventSubscriptionPool[channel] = new BufferedEventSubscription(pollerOptions);
        } else {
            debug('FOUND EXISTING BufferedEventSubscription FOR: ', channel);
        }
        const bufferedEventSubscription = self._bufferedEventSubscriptionPool[channel];

        bufferedEventSubscription.subscribe(token, revision, onEventCallback, onErrorCallback);
        self._tokenChannels[token] = channel;

        return token;
    } catch (error) {
        console.error('error in subscribe with params and error: ', query, revision, onEventCallback, error);
        throw error;
    }
};

/**
 * @param {String} token the position/offset from where we should start the subscription
 * @returns {Boolean} returns true if subscription token is existing and it got unsubscribed. false if it token does not exist
 */
EventstoreWithProjection.prototype.unsubscribe = function(token) {
    const self = this;
    try {
        if (self._tokenChannels[token]) {
            const channel = self._tokenChannels[token];
            const bufferedEventSubscription = self._bufferedEventSubscriptionPool[channel];
            if (bufferedEventSubscription) {
                bufferedEventSubscription.unsubscribe(token);
                return true;
            }
            return false;
        } else {
            return false;
        }
        // if (this._subscriptions[token]) {
        //     delete this._subscriptions[token];
        //     return true;
        // } else {
        //     return false;
        // }
    } catch (error) {
        console.error('error in unsubscribe with params and error: ', token, error);
        throw error;
    }
};

/**
 * @param {String} listName the name of the playback list
 * @param {DoneCallbackFunction} done the last event that built this projection state
 * @returns {void} Returns void. Use the callback to the get playbacklist
 */
EventstoreWithProjection.prototype.getPlaybackList = function(listName) {
    try {
        // TODO: use the same process as statelist to create a new playbacklist instance
        return this._playbackLists[listName];
    } catch (error) {
        console.error('error in getPlaybackList with params and error: ', listName, error);
        throw error;
    }
};


/**
 * @param {String} listName the name of the state list
 * @param {DoneCallbackFunction} done the last event that built this projection state
 * @returns {void} Returns void. Use the callback to the get state list
 */
EventstoreWithProjection.prototype.getStateList = function(listName, state) {
    try {
        const stateList = new EventstoreStateList(this._stateListStore, listName, state);
        return stateList;
    } catch (error) {
        console.error('error in getStateList with params and error: ', listName, error);
        throw error;
    }
};

/**
 * @param {String} listName the name of the playback list view
 * @param {DoneCallbackFunction} done the last event that built this projection state
 * @returns {void} Returns void. Use the callback to the get playbacklist
 */
EventstoreWithProjection.prototype.getPlaybackListView = function(listName, done) {
    try {
        done(null, this._playbackListViews[listName]);
    } catch (error) {
        console.error('error in getPlaybackListView with params and error: ', listName, error);
        throw error;
    }
};

/**
 * @param {String} listName the name of the playback list view
 * @param {String} listQuery the list query for the playback list view
 * @param {String} totalCountQuery the total count query for the playback list view
 * @param {Alias} alias the query for the playback list view
 * @param {DoneCallbackFunction} done the last event that built this projection state
 * @returns {void} Returns void. Use the callback to the get playbacklist
 */
EventstoreWithProjection.prototype.registerPlaybackListView = function(listName, listQuery, totalCountQuery, opts, done) {
    let alias = undefined;
    if (done == undefined) {
        // NOTE: if alias is function, then it is done
        done = opts;
    } else {
        alias = opts.alias;
    }

    this._registerPlaybackListView(listName, listQuery, totalCountQuery, alias).then(done).catch(done);
};



/**
 * @param {String} functionName the name of the function to register
 * @param {Function} theFunction the function to call
 * @returns {void} Returns void.
 */
EventstoreWithProjection.prototype.registerFunction = function(functionName, theFunction) {
    // just set it. if functionName already exists just override
    this._userDefinedFunctions[functionName] = theFunction;
};


/**
 * @returns {Promise<Array<Projection>} returns Promise of type void
 */
 EventstoreWithProjection.prototype.getProjections = function(done) {
    try {
        return this._projectionStore.getProjections().then((data) => done(null, data)).catch(done);;
    } catch (error) {
        console.error('getProjections failed with parameters and error', error);
        done(error);
    }
};

 /**
  * 
  * @param {string} projectionId 
  * @returns {Promise<Projection>}
  */
 EventstoreWithProjection.prototype.getProjection = function(projectionId, done) {
    try {
        return this._projectionStore.getProjection(projectionId).then((data) => done(null, data)).catch(done);;
    } catch (error) {
        console.error('getProjection failed with parameters and error', projectionId, error);
        done(error);
    }
};


 /**
  * 
  * @param {string} projectionName 
  * @returns {Promise<Projection>}
  */
  EventstoreWithProjection.prototype.getProjectionByName = function(projectionName, done) {
    try {
        return this._projectionStore.getProjectionByName(projectionName).then((data) => done(null, data)).catch(done);;
    } catch (error) {
        console.error('getProjectionByName failed with parameters and error', projectionName, error);
        done(error);
    }
};


/**
 * @param {string} projectionId
 * @returns {Promise}
 */
 EventstoreWithProjection.prototype.pauseProjection = function(projectionId, done) {
    try {
        this._projectionStore.setState(projectionId, 'paused').then((data) => done(null, data)).catch(done);
    } catch (error) {
        console.error('pauseProjection failed with parameters and error', projectionId, error);
        done(error);
    }
};

/**
 * @param {string} projectionId
 * @returns {Promise}
 */
 EventstoreWithProjection.prototype.runProjection = function(projectionId, forced, done) {
    try {
        this._runProjection(projectionId, forced).then((data) => done(null, data)).catch(done);
    } catch (error) {
        console.error('runProjection failed with parameters and error', projectionId, error);
        done(error);
    }
};

/**
 * @param {string} projectionId
 * @returns {Promise}
 */
 EventstoreWithProjection.prototype._runProjection = async function(projectionId, forced) {
    try {
        if (forced) {
            const projection = await this._projectionStore.getProjection(projectionId);
            await this._projectionStore.setOffset(projectionId, projection.errorOffset);
        }
        await this._projectionStore.setState(projectionId, 'running');
        await this._projectionStore.setError(projectionId, null, null, null);
    } catch (error) {
        console.error('_runProjection failed with parameters and error', projectionId, forced, error);
        throw error;
    }
};


/**
 * @param {string} projectionId
 * @returns {Promise}
 */
 EventstoreWithProjection.prototype.resetProjection = function(projectionId, done) {
    try {
        this._resetProjection(projectionId).then((data) => done(null, data)).catch(done);
    } catch (error) {
        console.error('resetProjection failed with parameters and error', projectionId, error);
        done(error);
    }
};


/**
 * @param {string} projectionId
 * @returns {Promise}
 */
 EventstoreWithProjection.prototype.deleteProjection = function(projectionId, done) {
    try {
        this._deleteProjection(projectionId).then((data) => done(null, data)).catch(done);
    } catch (error) {
        console.error('deleteProjection failed with parameters and error', projectionId, error);
        done(error);
    }
};

/***************************************************************************************************
    PRIVATE METHODS 
***************************************************************************************************/

/**
 * @returns {Promise<Array<Projection>} returns Promise of type void
 */
 EventstoreWithProjection.prototype._getProjections = async function() {
    try {
        return this._projectionStore.getProjections();
    } catch (error) {
        console.error('getProjections failed with parameters and error', error);
        throw error;
    }
};


/**
 * 
 * @param {string} projectionId 
 */
 EventstoreWithProjection.prototype._deleteProjection = async function(projectionId) {
    try {
        await this._stopWaitingForProjectionSignals(projectionId);
        delete this._projections[projectionId];

        const projection = await this._projectionStore.getProjection(projectionId)

        if (projection.stateList) {
            let stateLists;
    
            if (_.isArray(projection.stateList)) {
                stateLists = projection.stateList;
            } else {
                stateLists = [projection.stateList];
            }
    
            for (let index = 0; index < stateLists.length; index++) {
                const stateListConfig = stateLists[index];
                await this._stateListStore.destroy(stateListConfig.name);
            }
        }

        if (projection.playbackList) {
            await this._playbackListStore.destroy(projection.playbackList.name);
        }

        await this._projectionStore.delete(projectionId);

        // TODO: also delete all events emitted by this projection. currently, it is deleted outside
    } catch (error) {
        console.error('_resetProjection failed with parameters and error', projectionId, error);
    }
};

/**
 * 
 * @param {string} projectionId 
 */
EventstoreWithProjection.prototype._resetProjection = async function(projectionId) {
    try {
        const projection = await this._projectionStore.getProjection(projectionId)

        if (projection.stateList) {
            let stateLists;
    
            if (_.isArray(projection.stateList)) {
                stateLists = projection.stateList;
            } else {
                stateLists = [projection.stateList];
            }
    
            for (let index = 0; index < stateLists.length; index++) {
                const stateListConfig = stateLists[index];
                await this._stateListStore.truncate(stateListConfig.name);
            }
        }

        if (projection.playbackList) {
            await this._playbackListStore.truncate(projection.playbackList.name);
        }

        await this._projectionStore.setOffset(projectionId, 0);

        // TODO: also delete all events emitted by this projection. currently, it is deleted outside
    } catch (error) {
        console.error('_resetProjection failed with parameters and error', projectionId, error);
    }
};

/**
 * @param {String} listName the name of the playback list view
 * @param {String} query the query for the playback list view
 * @param {Alias} alias the query for the playback list view
 * @param {DoneCallbackFunction} done the last event that built this projection state
 * @returns {void} Returns void. Use the callback to the get playbacklist
 */
EventstoreWithProjection.prototype._registerPlaybackListView = async function(listName, listQuery, totalCountQuery, alias) {
    try {

        if (!this.options.listStore || !this.options.listStore.connection) {
            throw new Error('listStore must be provided in the options');
        }

        if (this.options.listStore && this.options.listStore.connection) {
            if (!this.options.listStore.connection.host) {
                throw new Error('listStore.connection.host must be provided in the options');
            }

            if (!this.options.listStore.connection.port) {
                throw new Error('listStore.connection.port must be provided in the options');
            }

            if (!this.options.listStore.connection.database) {
                throw new Error('listStore.database must be provided in the options');
            }

            if (!this.options.listStore.connection.user) {
                throw new Error('listStore.connection.user must be provided in the options');
            }

            if (!this.options.listStore.connection.password) {
                throw new Error('listStore.password must be provided in the options');
            }
        }

        const EventstorePlaybackListView = require('./eventstore-playback-list-view');
        const playbackList = new EventstorePlaybackListView({
            host: this.options.listStore.connection.host,
            port: this.options.listStore.connection.port,
            database: this.options.listStore.connection.database,
            user: this.options.listStore.connection.user,
            password: this.options.listStore.connection.password,
            listName: listName,
            listQuery: listQuery,
            totalCountQuery: totalCountQuery,
            alias: alias
        });
        await playbackList.init();

        this._playbackListViews[listName] = playbackList;
    } catch (error) {
        console.error('error in getPlaybackListView with params and error: ', listName, error);
        throw error;
    }
};

/**
 * 
 */
EventstoreWithProjection.prototype._doLeaderElection = async function(projectionGroup) {
    // TODO: use distributed lock injected object for testing compliance
    const self = this;
    const ttlDuration = this.options.lockTimeToLive;
    
    const projectionSubscriptions = {};
    try {
        const lockKey = `projection-group-leader-locks:${projectionGroup}`;
        let lockToken = await this._distributedLock.lock(lockKey, ttlDuration);
        // acquired a lock. do the job
        let continueJob = true;

        while (continueJob) {
            try {
                _.forOwn(this._projections, async (projection, projectionId) => {
                    if (!projectionSubscriptions[projectionId]) {
                        let queryArray = [];
                        projectionSubscriptions[projectionId] = {};

                        if(!Array.isArray(projection.query)) {
                            queryArray.push(projection.query);
                        } else {
                            queryArray = projection.query;
                        }

                        _.forEach(queryArray, async function(q) {
                            const channel = self._getChannel(q);
                            projectionSubscriptions[projectionId][channel] = await self._distributedSignal.subscribe(channel, function(err, done) {
                                self._distributedSignal.signal(projectionId, projectionGroup);
                                done();
                            });
                        });
                    }
                });

                // sleep half of the ttlduration
                await this._sleep(ttlDuration / 2);
                await this._distributedLock.extend(lockToken, ttlDuration);
            } catch (error) {
                console.error('error in while loop in shared job', error);
                // if there is an error then exit while loop and then contend again
                continueJob = false;
            }
        }
    } catch (error) {
        if (error.name == 'LockError') {
            // ignore this just try again later to acquire lock
            // console.log('lost in lock contention. will try again in', ttlDuration);
        } else {
            console.error('error in doing shared timer job', error);
        }
    } finally {
        // cleanup
        const tasks = [];
        _.forOwn(projectionSubscriptions, (projections) => {
            _.forOwn(projections, (subscriptionId) => {
                tasks.push(self._distributedSignal.unsubscribe(subscriptionId));
            });
        });
        await Promise.all(tasks);

        // sleep before contending again
        await this._sleep(ttlDuration);
        this._doLeaderElection(projectionGroup);
    }
};

EventstoreWithProjection.prototype._sleep = async function(timeout, rejectOnTimeout) {
    return new Promise((resolve, reject) => {
        setTimeout(function() {
            if (rejectOnTimeout) {
                reject(new Error('timed out'));
            } else {
                resolve();
            }
        }, timeout);
    })
}

/**
 * 
 * @param {Function} callback original callback
 * @param {Function} preCallback hooked callback
 */
EventstoreWithProjection.prototype._enhanceCallback = function(owner, callback, preCallback) {
    var originalCallback = callback;

    callback = function() {
        if (originalCallback) {
            preCallback();
            return originalCallback.apply(owner, arguments);
        }
        return;
    };
    return callback;
};


/**
 * @param {AggregateQuery} query 
 * @returns {String} - returns a dot delimited channel
 */
EventstoreWithProjection.prototype._getChannel = function(query) {
    return `${query.context || 'all'}.${query.aggregate || 'all'}.${query.aggregateId || 'all'}`;
};

/**
 * Forms filtered query object for am object
 * @param {Any} object any object that may contain query properties 
 * @returns {AggregateQuery} - returns an aggregate query
 */
EventstoreWithProjection.prototype._getQuery = function(object) {
    let query = {};
    if (object.context) {
        query.context = object.context;
    }
    if (object.aggregate) {
        query.aggregate = object.aggregate;
    }
    if (object.aggregateId) {
        query.aggregateId = object.aggregateId;
    }
    if (object.streamId) {
        query.streamId = object.streamId;
    }
    return query;
};

/**
 * @param {String} jobId the job to process
 * @param {Object} jobData callback to notify that the job is done
 * @param {Object} lastResult last result that was passed to the done
 * @param {import('./jobs-manager').JobDoneCallback} done done callback. pass a result to be saved per jobid. this result will be passed to the lastResult parameter the next time a job with the same id is processed
 * @returns {Promise<void>} returns Promise of type void
 */
EventstoreWithProjection.prototype._processProjection = async function(projectionId) {
    try {
        // TODO: Remove in-memory projection list and use the storedProjection list
        // TODO: Try to research on how to serialize and deserialize javascript functions
        // TODO: Improve on typings 
        const projection = await this._projectionStore.getProjection(projectionId);
        let offset = projection && projection.offset ? projection.offset : 0;
        if (isNaN(offset)) {
            offset = 0;
        }
        if (projection) {
            if (projection.state == 'running') {
                let projectionQuery = _.clone(projection.query);

                const events = await this._getEventsAsync(this, projectionQuery, offset, this.options.pollingMaxRevisions);
                const eventsCount = _.isArray(events) ? events.length : 0;

                if (events) {
                    debug('got events in getEventsAsync for playback:', events, events.length);
                }

                let errorFault = null;
                let errorEvent = null;
                let errorOffset = null;
                for (let i = 0; i < events.length; i++) {
                    const event = events[i];
                    
                    try {
                        await this._playbackEvent(event, projection, this.options.eventCallbackTimeout);
                        offset = event.eventSequence;
                    } catch (error) {
                        console.error('error in playing back event in partitioned queue with params and error', event, projection.projectionId, error);
                        // send to an error stream for this projection with streamid: ${projectionId}-errors.
                        // this lets us see the playback errors as a stream and resolve it manually

                        errorFault = error;
                        errorEvent = event;
                        errorOffset = event.eventSequence;

                        /** @type {AggregateQuery} */
                        const errorStreamQuery = {
                            context: 'projection-errors',
                            aggregate: projection.projectionId,
                            aggregateId: `${projection.projectionId}-errors`
                        }
                        try {
                            await this._saveEventAsync(this._outputEventstore, errorStreamQuery, {
                                event: event.payload,
                                error: {
                                    stack: error.stack,
                                    message: error.message
                                }
                            });
                        } catch (error) {
                            console.error('error in saving to projection-errors with params', errorStreamQuery, event, error);
                            console.error('critical error. need to manually retry the above error.');
                        }

                        break;
                    }
                }

                await this._projectionStore.setProcessed(projection.projectionId, Date.now(), eventsCount > 0 ? offset : undefined, !(eventsCount > 0));

                if (errorFault) {
                    await this._projectionStore.setState(projection.projectionId, 'faulted');
                    await this._projectionStore.setError(projection.projectionId, errorFault, errorEvent, errorOffset);
                    this.emit('playbackError', errorFault);
                } else {
                    if (eventsCount == this.options.pollingMaxRevisions) {
                        await this._distributedSignal.signal(projection.projectionId, this.options.projectionGroup);
                    }
                    
                }
            }
        } else {
            console.error('_processJob. storedProjection is missing', projection.projectionId);
        }
    } catch (error) {
        console.error('_processJob failed with parameters and error', error);
        throw error;
    }
};


/**
 * @returns {void} returns void
 */
EventstoreWithProjection.prototype._waitForQueuesToDrain = async function(queues) {
    try {
        const tasks = [];
        _.forOwn(queues, (queue) => {
            tasks.push(new Promise((resolve) => {
                queue.drain = resolve;
            }));
        });

        await Promise.all(tasks);
    } catch (error) {
        console.error('_waitForQueuesToDrain failed with parameters and error', queues, error);
        throw error;
    }
};

/**
 * @param {Event} event the event to process
 * @param {Projection} projection the object/interface to use to playback the event
 * @param {Number} timeout callback to notify that the job is done
 * @returns {Promise<void>} returns a Promise that resolves to a void
 */
EventstoreWithProjection.prototype._getPlaybackFunctions = function(state) {
    const self = this;
    const funcs = {
        emit: function(targetQuery, event, done) {
            if (done) {
                self._saveEventAsync(self._outputEventstore, targetQuery, event).then(done).catch(done);
            } else {
                return self._saveEventAsync(self._outputEventstore, targetQuery, event);
            }
        },
        getPlaybackList: function(listName, done) {
            const playbackList = self.getPlaybackList(listName);
            if (done) {
                done(null, playbackList);
            } else {
                return Promise.resolve(playbackList);
            }
        },
        getStateList: function(listName, done) {
            const stateList = self.getStateList(listName, state);
            if (done) {
                done(null, stateList);
            } else {
                return Promise.resolve(stateList);
            }
        }
    };

    // get user defined functions and set it to the funcs object
    _.forOwn(this._userDefinedFunctions, (val, key) => {
        funcs[key] = val;
    })

    return funcs;
}



/**
 * @param {Event} obj the object to check
 * @returns {Boolean} returns true if obj is a Promise
 */
EventstoreWithProjection.prototype._isPromise = function(obj) {
    return obj && typeof obj.then == 'function';
}

/**
 * @param {Event} event the event to process
 * @param {Projection} projection the object/interface to use to playback the event
 * @param {Number} timeout callback to notify that the job is done
 * @returns {Promise<void>} returns a Promise that resolves to a void
 */
EventstoreWithProjection.prototype._playbackEvent = function(event, projection, timeout) {
    return new Promise((resolve, reject) => {
        // TODO: fix timeout with promise based callback. how to have an async await timeout handling?
        // TODO: create common functions as code are being repetitive already
        let timeoutHandle = setTimeout(() => {
            reject(new Error('timeout in calling the playbackInterface'));
        }, timeout);

        try {
            const self = this;

            debug('event in playbackEvent', event);

            let eventHandlerFunc = self._getEventHandlerFunc(projection, event);

            if (eventHandlerFunc) {
                if (projection.outputState === 'true') {
                    this._getProjectionState(projection, event).then((projectionState) => {
                        debug('got projectionState', projectionState);
                        const mutableState = _.cloneDeep(projectionState.state);

                        const doneCallback = function(error) {
                            clearTimeout(timeoutHandle);
                            timeoutHandle = null;
                            if (error) {
                                console.error('error in playbackFunction with params and error', event, projection.projectionId, error);
                                reject(error)
                            } else {
                                // if the old state and the mutableState is not equal (meaning the playback updated it) then output a state
                                if (!_.isEqual(projectionState.state, mutableState)) {
                                    debug('state changed, saving new state', projectionState.state, mutableState);
                                    // add event metadata to the state
                                    mutableState._meta = {
                                        fromEvent: event
                                    };
                                    self._saveProjectionState(projection.projectionId, projectionState.id, mutableState, self._context).then(resolve).catch(reject);
                                } else {
                                    debug('state did not change, continuing', )
                                    resolve();
                                }
                            }
                        }

                        const result = eventHandlerFunc.call(projection.playbackInterface, mutableState, event, self._getPlaybackFunctions(mutableState), doneCallback);
                        if (self._isPromise(result)) {
                            return result.then(doneCallback).catch(doneCallback);
                        };

                    }).catch(reject);
                } else {
                    const doneCallback = function(error) {
                        clearTimeout(timeoutHandle);
                        timeoutHandle = null;
                        if (error) {
                            console.error('error in playbackFunction with params and error', error);
                            console.error(JSON.stringify(event));
                            console.error(JSON.stringify(projection));
                            reject(error)
                        } else {
                            resolve();
                        }
                    };

                    const result = eventHandlerFunc.call(projection.playbackInterface, null, event, self._getPlaybackFunctions(), doneCallback);
                    if (self._isPromise(result)) {
                        return result.then(doneCallback).catch(doneCallback);
                    }
                }
            } else {
                debug('eventHandlerFunc is undefined. no function handler for this sevent');
                if (timeoutHandle) {
                    clearTimeout(timeoutHandle);
                }
                resolve();
            }
        } catch (error) {
            if (timeoutHandle) {
                clearTimeout(timeoutHandle);
            }
            console.error('error in _playbackEvent with params and error', error);
            console.error(JSON.stringify(event));
            console.error(JSON.stringify(projection));
            reject(error);
        }
    })
};


/**
 * @param {Eventstore} eventstore the eventstore to use
 * @param {AggregateQuery} targetQuery the query to emit the event
 * @param {Object} event the event to emit
 * @param {DoneCallbackFunction} done the last event that built this projection state
 * @returns {Promise<void>} returns a Promise that resolves to a void
 */
EventstoreWithProjection.prototype._saveEventAsync = async function(eventstore, targetQuery, event) {
    try {
        const stream = await this._getLastEventAsStreamAsync(eventstore, targetQuery);

        // TODO: add eventLink and eventSource
        await this._addEventToStream(stream, event);
        await this._commitStream(stream);
    } catch (error) {
        console.error('error in _emitAsync', targetQuery, event, error);
        throw error;
    }
};


/**
 * @param {String} projectionId the projectionid
 * @param {String} projectionStateId the projection state to save
 * @param {Object} newState the last event that built this projection state
 * @returns {Promise<void>} returns a Promise that resolves to a void
 */
EventstoreWithProjection.prototype._saveProjectionState = async function(projectionId, projectionStateId, newState, context) {
    const query = {
        aggregateId: projectionStateId,
        aggregate: 'states',
        context: context
    };

    await this._saveEventAsync(this._outputEventstore, query, newState);
};


/**
 * @param {Projection} projection the projection
 * @param {Event} lastProjectionEvent the last event got from the projection 
 * @returns {Promise<ProjectionState>} returns a Promise that resolves to a void
 */
EventstoreWithProjection.prototype._getProjectionState = async function(projection, lastProjectionEvent) {
    // default is no partition format: <projectionid>-result
    let streamId = this._getProjectionStateStreamId(projection, lastProjectionEvent);

    const query = {
        aggregateId: streamId,
        aggregate: 'states',
        context: this._context
    }

    debug('getting projection last state of streamId', streamId);
    const lastEvent = await this._getLastEventAsync(this._outputEventstore, query);

    debug('got projection last state of streamId', streamId, lastEvent);

    let state = {};
    if (lastEvent) {
        // payload is the last state
        state = lastEvent.payload;
    } else {
        // if we have no event yet given the streamId, then initialize it
        if (typeof projection.playbackInterface.$init === 'function') {
            try {
                const initState = projection.playbackInterface.$init();
                if (initState) {
                    state = initState;
                }
            } catch (error) {
                // NOTE: just log for now. and not let errors stop the playback loop
                console.error('error in playbackInterface.$init with params and error', projection, lastProjectionEvent, error)
            }
        }
    }

    return {
        id: streamId,
        state: state
    }
};

/**
 * @param {Projection} projection the projection
 * @param {Event} lastProjectionEvent the query 
 * @returns {String} returns a Promise that resolves to a void
 */
EventstoreWithProjection.prototype._getProjectionStateStreamId = function(projection, lastProjectionEvent) {
    // default is no partition format: <projectionid>-result

    let streamId = `${projection.projectionId}-result`;
    if (projection.partitionBy === 'stream') {
        let streamPartitionedStreamId = `${projection.projectionId}`;
        // format: <projectionid>-result[-<context>][-<aggregate>]-<aggregateId>
        if (lastProjectionEvent.context)
            streamPartitionedStreamId += `-${lastProjectionEvent.context}`;
        if (lastProjectionEvent.aggregate)
            streamPartitionedStreamId += `-${lastProjectionEvent.aggregate}`;
        streamPartitionedStreamId += `-${lastProjectionEvent.aggregateId || lastProjectionEvent.streamId}`;
        streamPartitionedStreamId += '-result';

        streamId = streamPartitionedStreamId;
    } else if (typeof projection.partitionBy === 'function') {
        try {
            const partitionId = projection.partitionBy(lastProjectionEvent);

            // if partitionId is defined then use it, else just use the default partititon which is by projection
            if (partitionId) {
                // format: <projectionid>-<paritionId>-result
                streamId = `${projection.projectionId}-${partitionId}-result`;
            }

        } catch (error) {
            // NOTE: log for now and use the default as the streamId (no partition)
            console.error('error in calling projection.partitionBy with params and error', projection, lastProjectionEvent, error);
        }
    }

    debug('_getProjectionStateStreamId streamId', lastProjectionEvent);

    return streamId;
};

/**
 * @param {Projection} projection the event 
 * @param {Event} event the event 
 * @returns {String} returns the function that the projection is handling
 */
EventstoreWithProjection.prototype._getEventHandlerFunc = function(projection, event) {
    // check if this event is handled by this projection
    const eventName = event.payload[this.options.eventNameFieldName];

    let eventHandlerFunc = projection.playbackInterface[eventName];

    if (!eventHandlerFunc) {
        // if no handler for this event, then check if there is an $any event handler.
        // if yes then use that
        const anyEventHandlerFunc = projection.playbackInterface['$any'];
        if (anyEventHandlerFunc) {
            eventHandlerFunc = anyEventHandlerFunc;
        }
    }

    return eventHandlerFunc;
};

/**
 * @returns {String} returns the projection job group name
 */
EventstoreWithProjection.prototype._getProjectionJobGroup = function() {
    return `projection-group:${this.options.projectionGroup}`;
};

/**
 * @param {Eventstore} eventstore the eventstore to use
 * @param {AggregateQuery} query the query for the aggregate/category that we like to get
 * @returns {Promise<Event>} returns a Promise that resolves to an event 
 */
EventstoreWithProjection.prototype._getLastEventAsync = async function(eventstore, query) {
    return new Promise((resolve, reject) => {
        try {
            eventstore.getLastEvent(query, (err, event) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(event);
                }
            });
        } catch (error) {
            console.error('_getLastEventAsync with params and error:', query, error);
            reject(error);
        }
    })
};

/**
 * @param {Eventstore} eventstore the eventstore to use
 * @param {AggregateQuery} query the query for the aggregate/category that we like to get
 * @param {Number} offset how many events to skip
 * @param {Number} limit max items to return
 * @returns {Promise<Event[]>} returns a Promise that resolves to an array of Events
 */
EventstoreWithProjection.prototype._getEventsAsync = async function(eventstore, query, offset, limit) {
    return new Promise((resolve, reject) => {
        try {
            eventstore.getEvents(query, offset, limit, (err, events) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(events);
                }
            });
        } catch (error) {
            console.error('_getEventsAsync with params and error:', query, offset, limit, error);
            reject(error);
        }
    })
};

/**
 * @param {AggregateQuery} query the query for the aggregate/category that we like to get
 * @param {Number} revMin Minimum revision boundary
 * @param {Number} revMax Maximum revision boundary
 * @returns {Promise<EventStream>} returns a Promise that resolves to an event 
 */
EventstoreWithProjection.prototype._getEventStreamAsync = async function(query, revMin, revMax) {
    return new Promise((resolve, reject) => {
        try {
            this.getEventStream(query, revMin, revMax, (err, stream) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(stream);
                }
            });
        } catch (error) {
            console.error('_getEventStreamAsync with params and error:', query, revMin, revMax, error);
            reject(error);
        }
    })
};


/**
 * @param {Eventstore} evenstore eventstore to use
 * @param {AggregateQuery} query the query for the aggregate/category that we like to get
 * @returns {Promise<EventStream>} returns a Promise that resolves to an event 
 */
EventstoreWithProjection.prototype._getLastEventAsStreamAsync = async function(evenstore, query) {
    return new Promise((resolve, reject) => {
        try {
            evenstore.getLastEventAsStream(query, (err, stream) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(stream);
                }
            });
        } catch (error) {
            console.error('_getLastEventAsStreamAsync with params and error:', query, error);
            reject(error);
        }
    })
};

/**
 * @param {AggregateQuery} query the query for the aggregate/category that we like to get
 * @param {Number} revMin Minimum revision boundary
 * @param {Number} revMax Maximum revision boundary
 * @returns {Promise<EventStream>} returns a Promise that resolves to an event 
 */
EventstoreWithProjection.prototype._getEventStreamBufferedAsync = async function(query, revMin, revMax) {
    return new Promise((resolve, reject) => {
        try {
            this.getEventStream(query, revMin, revMax, (err, stream) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(stream);
                }
            });
        } catch (error) {
            console.error('_getEventStreamAsync with params and error:', query, revMin, revMax, error);
            reject(error);
        }
    })
};

/**
 * @param {AggregateQuery} query
 * @returns {String[]} returns an array of possible topics of the query
 */
EventstoreWithProjection.prototype._queryToChannels = function(query) {
    const channels = [];

    channels.push('all.all.all');
    channels.push(`${query.context}.all.all`);
    channels.push(`${query.context}.${query.aggregate}.all`);
    channels.push(`${query.context}.${query.aggregate}.${query.aggregateId}`);

    return channels;
}

/**
 * @param {Projection} projection parameters for the projection
 * @returns {void} returns a Promise of type void
 */
EventstoreWithProjection.prototype._startWaitingForSignals = async function(projectionId) {
    const self = this;
    const subscriptionId = await this._distributedSignal.groupSubscribe(projectionId, this.options.projectionGroup, function(err, done) {
        self._processProjection(projectionId).catch(done).then(data => done(null, data));
    });
    
    self._projectionGroupedSubscriptions[projectionId] = subscriptionId;
}

/**
 * @param {String} projectionId parameters for the projection
 * @returns {void} returns a Promise of type void
 */
 EventstoreWithProjection.prototype._stopWaitingForProjectionSignals = async function(projectionId) {
    const subscriptionId = this._projectionGroupedSubscriptions[projectionId];
    delete this._projectionGroupedSubscriptions[projectionId];
    if (subscriptionId) {
        await this._distributedSignal.groupUnsubscribe(subscriptionId, this.options.projectionGroup);
    }
}


/**
 * @param {Projection} projection parameters for the projection
 * @returns {Promise<void>} returns a Promise of type void
 */
EventstoreWithProjection.prototype._project = async function(projection) {
    try {
        if (this.options.enableProjection === false) {
            throw new Error('enableProjection true is required for projections');
        }

        if (!projection) {
            throw new Error('projection is required');
        }

        if (!projection.projectionId) {
            throw new Error('projectionId is required');
        }

        if (!projection.query) {
            throw new Error('query is required');
        } else {
            let queryArray = [];
            if(!Array.isArray(projection.query)) {
                queryArray.push(projection.query);
            } else {
                queryArray = projection.query;
            }
            _.forEach(queryArray, function(q) {
                if (!q.aggregate && !q.context && !q.aggregateId && !q.streamId) {
                    throw new Error('at least an aggregate, context or aggregateId/streamId is required');
                }
            });
        }

        this._hasProjections = true;

        // use a distributed lock to make sure that we only create one event for the projection stream
        const projectionKey = `projection-group:${this.options.projectionGroup}:projection:${projection.projectionId}`;

        // set projection to private list of projections
        this._projections[projection.projectionId] = projection;

        let lockToken;
        const lockKey = projectionKey;
        try {
            lockToken = await this._distributedLock.lock(lockKey, 1900);
        } catch (error) {
            // console.error('was not able to acquire lock. just continue');
        }

        // add projection to the store if not exists
        const storedProjection = await this._projectionStore.getProjection(projection.projectionId);

        if (storedProjection) {
            this._projectionStore.updateProjection(projection);
        } else {
            this._projectionStore.createProjection(projection);
        }

        // initialize the playback list. make sure that it is also inside the lock
        await this._initPlaybackList(projection);

        // initialize the playback list. make sure that it is also inside the lock
        await this._initStateList(projection);

        this._startWaitingForSignals(projection.projectionId);

        // unlock
        if (lockToken) {
            await this._distributedLock.unlock(lockToken);
        }
    } catch (error) {
        console.error('error in _project with params and error', projection, error);
        throw error;
    }
};

/**
 * @param {Projection} projection the projection to initialize with a list
 * @returns {Promise<void>} returns a Promise of type void
 */
EventstoreWithProjection.prototype._initStateList = async function(projection) {

    if (projection.stateList) {

        let stateLists;

        if (_.isArray(projection.stateList)) {
            stateLists = projection.stateList;
        } else {
            stateLists = [projection.stateList];
        }

        for (let index = 0; index < stateLists.length; index++) {
            const stateListConfig = stateLists[index];
            // create the list
            await this._stateListStore.createList(stateListConfig);
        }
    }
};

/**
 * @param {Projection} projection the projection to initialize with a list
 * @returns {Promise<void>} returns a Promise of type void
 */
EventstoreWithProjection.prototype._initPlaybackList = async function(projection) {

    if (projection.playbackList) {
        if (!this.options.listStore) {
            throw new Error('listStore must be provided in the options');
        }

        const EventstorePlaybackList = require('./eventstore-playback-list');
        const playbackList = new EventstorePlaybackList({
            listName: projection.playbackList.name
        }, this._playbackListStore);
        await playbackList.createList(projection.playbackList);

        this._playbackLists[projection.playbackList.name] = playbackList;
    }
};


/**
 * @param {String} projectionId the projection id
 * @returns {Promise<EventStream>} - returns a stream as promise
 */
EventstoreWithProjection.prototype._getProjectionStream = async function(projectionId) {
    try {
        var queryProjection = {
            aggregateId: `projections:${projectionId}`,
            aggregate: 'projection',
            context: '__projections__'
        };
        const stream = await this._getLastEventAsStreamAsync(this._outputEventstore, queryProjection);
        return stream;
    } catch (error) {
        console.error('error in _getProjection:', projectionId, error);
        throw error;
    }
};

/**
 * @param {EventStream} stream the stream into which to add the projection
 * @param {Projection} projection the projection to add
 * @returns {Promise<void>} returns a Promise of type void
 */
EventstoreWithProjection.prototype._addProjectionStream = async function(stream, projection) {
    try {
        const event = {
            name: 'PROJECTION_CREATED',
            payload: {
                projectionId: projection.projectionId,
                query: projection.query,
                partitionBy: projection.partitionBy,
                projectionGroup: this.options.projectionGroup,
                meta: projection.meta
            }
        };
        await this._addEventToStream(stream, event);
        await this._commitStream(stream);
    } catch (error) {
        console.error('error in _addProjectionStream:', projection, error);
        throw error;
    }
};

/**
 * @param {EventStream} stream the stream into which to add the projection
 * @param {Projection} projection the projection to add
 * @returns {Promise<void>} returns a Promise of type void
 */
EventstoreWithProjection.prototype._addEventToStream = async function(stream, event) {
    debug('_addEventToStream called', stream, event);
    return new Promise((resolve, reject) => {
        try {
            stream.addEvent(event);
            resolve(stream);
        } catch (error) {
            console.error('error in _addEventToStream: ', stream, event, error);
            reject(error);
        }
    });
};

/**
 * @param {EventStream} stream the stream into which to add the projection
 * @returns {Promise<void>} returns a Promise of type void
 */
EventstoreWithProjection.prototype._commitStream = async function(stream) {
    debug('_commitStream called', stream);
    return new Promise((resolve, reject) => {
        try {
            stream.commit(function(err) {
                if (err) {
                    reject(err);
                } else {
                    resolve();
                }
            });
        } catch (error) {
            console.error('error in _commitStream: ', stream, error);
            reject(error);
        }
    });
};

/**
 * Gets the heap percentage from the heap statistics of the node app
 * @returns {Number} returns the current heap percentage value (0-1) of the node app
 */
EventstoreWithProjection.prototype._getHeapPercentage = function() {
    const heapStatistics = v8.getHeapStatistics();
    const heapUsed = heapStatistics.used_heap_size;
    const heapTotal = heapStatistics.heap_size_limit;
    const heapPercentage = heapUsed / heapTotal;

    return heapPercentage;
};

/**
 * Gets the current stream buffer bucket based on the current time.
 * @returns {String} returns the name of the bucket which is represented by the ISO datetime upto the hour
 */
EventstoreWithProjection.prototype._getCurrentStreamBufferBucket = function() {
    const now = new Date();
    const newBucket = now.toISOString().substring(0, 13); // ex. 2020-07-28T14
    return newBucket;
}

/**
 * Cleans up the oldest bucket from stream buffer bucket and locks the LRU execution to prevent getting executed multiple times
 */
EventstoreWithProjection.prototype._cleanOldestStreamBufferBucket = function() {
    this._streamBufferLRULocked = true;
    debug('locking LRU');

    const buckets = Object.keys(this._streamBufferBuckets);
    if (buckets && buckets.length > 0) {
        const sortedBuckets = _.sortBy(buckets, (i) => i);
        const bucketToClean = sortedBuckets[0];
        const channels = Object.keys(this._streamBufferBuckets[bucketToClean]);

        debug('cleaning bucket');
        debug(bucketToClean);

        if (channels && channels.length > 0) {
            channels.forEach((channel) => {
                this._streamBufferLRUCleaner.push({
                    channel
                });
            });
        }

        delete this._streamBufferBuckets[bucketToClean];
    }
};

module.exports = EventstoreWithProjection;