const Eventstore = require('../eventstore');
const _ = require('lodash');
const util = require('util');
const shortid = require('shortid');
const debug = require('debug')('eventstore:projection');
const async = require('async');
const ProjectionEventStreamBuffer = require('../projectionEventStreamBuffer');
const SubscriptionEventStreamBuffer = require('../subscriptionEventStreamBuffer');
const BufferedEventSubscription = require('../bufferedEventSubscription');
const v8 = require('v8');
const EventstoreStateList = require('./state-list/eventstore-state-list');
const EventstorePlaybackListQueryOnly = require('./eventstore-playback-list-query-only');
const Event = require('../event');
const EventStoreDuplicateError = require('../databases/errors/EventStoreDuplicateError');
const TaskAssignmentGroup = require('./task-assignment').TaskAssignmentGroup;
const WaitableFactory = require('./waitable-factory');
const { WaitableConsumer } = require('./waitable-factory/waitable-consumer');

// TODO: offset + 1
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
 * ProjectionConfiguration
 * @typedef {Object} ProjectionConfiguration
 * @property {String} projectionId The unique projectionId of the projection
 * @property {String} projectionName The unique projectionName of the projection
 * @property {AggregateQuery} query The query to use to get the events for the projection
 * @property {ProjectionPlaybackInterface} playbackInterface The object to use for when an event for this projection is received
 * @property {Object} meta Optional user meta data
 * @property {("stream"|"")} partitionBy Partition the state by stream, using a function or no partition. If outputState is false, then this option is ignored
 * @property {("true"|"false")} outputState Saves the projection state as a stream of states. Default is false
 * @property {PlaybackListConfig} playbackList playback list configuration for the projection
 * @property {StateListConfig} stateList playback list configuration for the projection
 */

/**
 * Projection
 * @typedef {Object} Projection
 * @property {string} projectionId
 * @property {ProjectionConfiguration} configuration
 * @property {Number} processedDate
 * @property {String} state
 * @property {String} context
 * @property {Number} [offset]
 * @property {Error} [error]
 * @property {Object} [errorEvent]
 * @property {Number} [errorOffset]
 * @property {Boolean} [isIdle]
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
 * SubscriptionEventStreamBuffer
 * @typedef {import('../subscriptionEventStreamBuffer')} SubscriptionEventStreamBuffer
 */

/**
 * ProjectionEventStreamBuffer
 * @typedef {import('../projectionEventStreamBuffer')} ProjectionEventStreamBuffer
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
        eventNameFieldName: 'name',
        context: 'default',
        enableProjection: false,
        enableProjectionEventStreamBuffer: false,
        playbackEventJobCount: 10,
        outputsTo: this,
        shouldExhaustAllEvents: true,
        maxConcurrencyRetryCount: 10,
        membershipPollingTimeout: 10000
    };

    const options = _.defaults(opts, defaults);
    if (options.enableProjection) {
        if (!options.projectionGroup) {
            throw new Error('projectionGroup is required with enableProjection true');
        }

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

        this._waitableFactory = new WaitableFactory({
            createRedisClient: options.redisCreateClient
        });

        this._waitableProducer = this._waitableFactory.createWaitableProducer();


        self.pollingActive = true;
        self._subscriptions = {};
        self._projections = {};
        self._playbackLists = {};
        self._stateLists = {};
        self._playbackListViews = {};
        self._jobs = {};
        self._userDefinedFunctions = {};

        if (options.enableProjectionEventStreamBuffer) {
            // Initialize Projection Stream Buffers
            self._projectionEventStreamBuffers = {};
            self._projectionEventStreamBufferBuckets = {};
            self._projectionEventStreamBufferLRULocked = false;

            self._projectionEventStreamBufferLRUCleaner = async.queue(function(task, callback) {
                const channel = task.channel;

                debug('Closing the Projection Event Stream Buffer', channel);
                self._projectionEventStreamBuffers[channel].close();

                callback();
            });

            self._projectionEventStreamBufferLRUCleaner.drain = function() {
                if (global.gc) {
                    global.gc();
                }

                const heapPercentage = self._getHeapPercentage();

                if (heapPercentage >= STREAM_BUFFER_MEMORY_THRESHOLD) {
                    self._cleanOldestProjectionEventStreamBufferBucket();
                } else {
                    self._projectionEventStreamBufferLRULocked = false;
                }
            };
        }

        // Initialize Subscription Poller Pool and Token-Channel Map
        self._bufferedEventSubscriptionPool = {};
        self._tokenChannels = {};

        // Initialize Subscription Stream Buffers
        self._subscriptionEventStreamBuffers = {};
        self._subscriptionEventStreamBufferBuckets = {};
        self._subscriptionEventStreamBufferLRULocked = false;

        self._subscriptionEventStreamBufferLRUCleaner = async.queue(function(task, callback) {
            const channel = task.channel;

            const bufferedEventSubscription = self._bufferedEventSubscriptionPool[channel];
            if (bufferedEventSubscription && bufferedEventSubscription.isActive()) {
                debug('Clearing the Subscription Stream Buffer only', channel);
                // There's an active buffered event subscription, do not cleanup the entire stream buffer. Instead, clear its contents only.
                self._subscriptionEventStreamBuffers[channel].clear();

                // Update the bucket of the buffer
                const currentBucket = self._subscriptionEventStreamBuffers[channel].bucket;
                const newBucket = self._getCurrentStreamBufferBucket();
                if (self._subscriptionEventStreamBufferBuckets[currentBucket]) {
                    delete self._subscriptionEventStreamBufferBuckets[currentBucket][channel];
                }

                if (!self._subscriptionEventStreamBufferBuckets[newBucket]) {
                    self._subscriptionEventStreamBufferBuckets[newBucket] = {};
                }

                self._subscriptionEventStreamBufferBuckets[newBucket][channel] = new Date().getTime();
                self._subscriptionEventStreamBuffers[channel].bucket = newBucket;
            } else {
                // There is no active buffered event subscriptions. The stream buffer can safely be closed.
                debug('Closing the Subscription Event Stream Buffer', channel);
                self._subscriptionEventStreamBuffers[channel].close();

                // Close the bufferedEventSubscription also if it's defined but not active
                if (bufferedEventSubscription) {
                    bufferedEventSubscription.close();

                    delete self._bufferedEventSubscriptionPool[channel];
                }
            }
            callback();
        });

        self._subscriptionEventStreamBufferLRUCleaner.drain = function() {
            if (global.gc) {
                global.gc();
            }

            const heapPercentage = self._getHeapPercentage();

            if (heapPercentage >= STREAM_BUFFER_MEMORY_THRESHOLD) {
                self._cleanOldestSubscriptionEventStreamBufferBucket();
            } else {
                self._subscriptionEventStreamBufferLRULocked = false;
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
                            // self._distributedSignal.signal(channel);
                            self._waitableProducer.signal(channel);
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
};

/**
 * @param {DoneCallbackFunction} callback success/error callback
 * @returns {void} - returns void
 */
EventstoreWithProjection.prototype.close = function(callback) {
    debug('close called');
    // just preserving that adrai's public functions are callback based but internally we can just use async/await for elegance
    this._close().then(callback).catch(callback);
};

EventstoreWithProjection.prototype._close = async function() {
    try {
        debug('_close called');

        await Promise.all([
            this._stopAllProjections()
        ]);
    } catch (error) {
        console.error('error in _close with params and error:', error);
        throw error;
    }
};

/**
 * @param {DoneCallbackFunction} callback success/error callback
 * @returns {void} - returns void
 */
EventstoreWithProjection.prototype.stopAllProjections = function(callback) {
    debug('stopAllProjections called');
    // just preserving that adrai's public functions are callback based but internally we can just use async/await for elegance
    this._stopAllProjections().then(callback).catch(callback);
};

EventstoreWithProjection.prototype._stopAllProjections = async function() {
    try {
        if (this._taskGroup) {
            this._taskGroup.leave();
        }
    } catch (error) {
        console.error('error in _stopAllProjections with params and error:', error);
        throw error;
    }
};

/**
 * Closes all Subscription Event StreamBuffers
 * @param {DoneCallbackFunction} callback success/error callback
 * @returns {void} - returns void
 */
EventstoreWithProjection.prototype.closeSubscriptionEventStreamBuffers = function(done) {
    this._subscriptionEventStreamBufferLRULocked = true;
    const channels = Object.keys(this._subscriptionEventStreamBuffers);

    for (let i = 0; i < channels.length; i++) {
        const channel = channels[i];
        this._subscriptionEventStreamBuffers[channel].close();
    }
    this._subscriptionEventStreamBuffers = {};
    this._subscriptionEventStreamBufferBuckets = {};
    this._subscriptionEventStreamBufferLRULocked = false;
    done(null);
};

/**
 * Closes all Projection Event StreamBuffers
 * @param {DoneCallbackFunction} callback success/error callback
 * @returns {void} - returns void
 */
EventstoreWithProjection.prototype.closeProjectionEventStreamBuffers = function(done) {
    if (this.options.enableProjectionEventStreamBuffer) {
        this._projectionEventStreamBufferLRULocked = true;
        const channels = Object.keys(this._projectionEventStreamBuffers);

        for (let i = 0; i < channels.length; i++) {
            const channel = channels[i];
            this._projectionEventStreamBuffers[channel].close();
        }
        this._projectionEventStreamBuffers = {};
        this._projectionEventStreamBufferBuckets = {};
        this._projectionEventStreamBufferLRULocked = false;
    }
    done(null);
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

        const allProjections = await this._getProjections(this._context);

        for (let index = 0; index < allProjections.length; index++) {
            const projection = allProjections[index];
            if (!this._projections[projection.projectionId]) {
                await this._project(projection.configuration, false);
            }
        }

        // start assigning tasks
        this._doTaskAssignment(this.options.projectionGroup);
    } catch (error) {
        console.error('error in _startAllProjections with params and error:', error);
        throw error;
    }
};

/**
 * @param {ProjectionConfiguration} projectionConfig parameters for the projection
 * @param {Function} callback success/error callback
 * @returns {void} - returns void
 */
EventstoreWithProjection.prototype.project = function(projectionConfig, callback) {
    debug('projection called with params:', projectionConfig);
    // just preserving that adrai's public functions are callback based but internally we can just use async/await for elegance
    this._project(projectionConfig, true).then(callback).catch(callback);
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
        if (!self._subscriptionEventStreamBuffers[channel]) {
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
                    if (!self._subscriptionEventStreamBufferLRULocked) {
                        const heapPercentage = self._getHeapPercentage();
                        debug('HEAP %', heapPercentage);
                        if (heapPercentage >= STREAM_BUFFER_MEMORY_THRESHOLD) {
                            self._cleanOldestSubscriptionEventStreamBufferBucket();
                        }
                    }

                    const newBucket = self._getCurrentStreamBufferBucket();
                    if (self._subscriptionEventStreamBufferBuckets[currentBucket]) {
                        delete self._subscriptionEventStreamBufferBuckets[currentBucket][channel];
                    }

                    if (!self._subscriptionEventStreamBufferBuckets[newBucket]) {
                        self._subscriptionEventStreamBufferBuckets[newBucket] = {};
                    }

                    self._subscriptionEventStreamBufferBuckets[newBucket][channel] = new Date().getTime();
                    self._subscriptionEventStreamBuffers[channel].bucket = newBucket;
                },
                onInactive: (bucket, channel) => {
                    const bufferedEventSubscription = self._bufferedEventSubscriptionPool[channel];
                    if (bufferedEventSubscription && bufferedEventSubscription.isActive()) {
                        debug('Clearing Stream Buffer only', channel);
                        // There's an active buffered event subscription, do not cleanup the entire stream buffer. Instead, clear its contents only.
                        self._subscriptionEventStreamBuffers[channel].clear();

                        // Update the bucket of the buffer
                        const currentBucket = self._subscriptionEventStreamBuffers[channel].bucket;
                        const newBucket = self._getCurrentStreamBufferBucket();
                        if (self._subscriptionEventStreamBufferBuckets[currentBucket]) {
                            delete self._subscriptionEventStreamBufferBuckets[currentBucket][channel];
                        }

                        if (!self._subscriptionEventStreamBufferBuckets[newBucket]) {
                            self._subscriptionEventStreamBufferBuckets[newBucket] = {};
                        }

                        self._subscriptionEventStreamBufferBuckets[newBucket][channel] = new Date().getTime();
                        self._subscriptionEventStreamBuffers[channel].bucket = newBucket;
                    } else {
                        // There is no active buffered event subscriptions. The stream buffer can safely be closed.
                        debug('Closing the Stream Buffer', channel);
                        self._subscriptionEventStreamBuffers[channel].close();

                        // Close the bufferedEventSubscription also if it's defined but not active
                        if (bufferedEventSubscription) {
                            bufferedEventSubscription.close();

                            delete self._bufferedEventSubscriptionPool[channel];
                        }
                    }
                },
                onClose: (bucket, channel) => {
                    delete self._subscriptionEventStreamBuffers[channel];

                    if (self._subscriptionEventStreamBufferBuckets[bucket]) {
                        delete self._subscriptionEventStreamBufferBuckets[bucket][channel];
                    }
                }
            }

            self._subscriptionEventStreamBuffers[channel] = new SubscriptionEventStreamBuffer(options);

            if (!self._subscriptionEventStreamBufferBuckets[bucket]) {
                self._subscriptionEventStreamBufferBuckets[bucket] = {};
            }

            self._subscriptionEventStreamBufferBuckets[bucket][channel] = new Date().getTime();
        }

        if (!self._bufferedEventSubscriptionPool[channel]) {
            const pollerOptions = {
                es: self,
                distributedSignal: self._distributedSignal,
                waitableFactory: self._waitableFactory,
                streamBuffer: self._subscriptionEventStreamBuffers[channel],
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
 * @returns {void} Returns void. Use the callback to the get playbacklist
 */
EventstoreWithProjection.prototype.getPlaybackList = function(listName) {
    try {
        // TODO: use the same process as statelist to create a new playbacklist instance
        return this._getPlaybackListByType(listName, 'queryonly');
    } catch (error) {
        console.error('error in getPlaybackList with params and error: ', listName, error);
        throw error;
    }
};

/**
 * @param {String} listName the name of the playback list
 * @param {("queryonly"|"mutable")} type
 * @returns {void} Returns void. Use the callback to the get playbacklist
 */
EventstoreWithProjection.prototype._getPlaybackListByType = function(listName, type) {
    try {
        if (type == 'queryonly') {
            return new EventstorePlaybackListQueryOnly({
                listName: listName
            }, this._playbackListStore);
        } else {
            return this._playbackLists[listName];
        }
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
            /**
             * @type {Projection}
             */
            const projection = await this._projectionStore.getProjection(projectionId);
            await this._projectionStore.setOffset(projectionId, projection.errorOffset);
        }
        await this._projectionStore.setState(projectionId, 'running');
        await this._projectionStore.setError(projectionId, null, null, null);

        if (this._taskGroup) {
            this._taskGroup.addTasks([projectionId]);
        }
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
EventstoreWithProjection.prototype._getProjections = async function(context) {
    try {
        return this._projectionStore.getProjections(context);
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
        if (this._taskGroup) {
            this._taskGroup.removeTasks([projectionId]);
        }
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
        /**
         * @type {Projection}
         */
        const projection = await this._projectionStore.getProjection(projectionId)
        const projectionConfig = projection.configuration;

        if (projectionConfig.stateList) {
            let stateLists;

            if (_.isArray(projectionConfig.stateList)) {
                stateLists = projectionConfig.stateList;
            } else {
                stateLists = [projectionConfig.stateList];
            }

            for (let index = 0; index < stateLists.length; index++) {
                const stateListConfig = stateLists[index];
                await this._stateListStore.truncate(stateListConfig.name);
            }
        }

        if (projectionConfig.playbackList) {
            await this._playbackListStore.truncate(projectionConfig.playbackList.name);
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
            connection: this.options.listStore.connection,
            pool: this.options.listStore.pool,
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
 * @returns {Promise<void>} - returns a Promise a Promise of type void
 */
EventstoreWithProjection.prototype._doTaskAssignment = async function(projectionGroup) {
    const self = this;
    const tasks = [];

    for (let projectionId in self._projections) {
        // on push initial tasks that are running state
        const projection = await this._projectionStore.getProjection(projectionId);
        if (projection && projection.state == 'running') {
            tasks.push(projectionId);
        }
    }

    self._taskGroup = new TaskAssignmentGroup({
        initialTasks: tasks,
        createRedisClient: self.options.redisCreateClient,
        groupId: projectionGroup,
        membershipPollingTimeout: self.options.membershipPollingTimeout,
        distributedLock: self._distributedLock
    });

    await self._taskGroup.join();

    let currentAssignments = [];
    let currentRebalanceId;
    self._taskGroup.on('rebalance', async (updatedAssignments, rebalanceId) => {
        currentAssignments = updatedAssignments;
        currentRebalanceId = rebalanceId;
        debug('on rebalance', updatedAssignments);
        self.emit('rebalance', updatedAssignments);

        _.each(updatedAssignments, async function(projectionId) {
            while (currentAssignments.find((item) => item == projectionId) && currentRebalanceId === rebalanceId) {
                const projection = self._projections[projectionId];
                if (projection) {
                    const projectionConfig = projection.configuration;
                    const pollingTimeout = self.options.pollingTimeout;
                    const ttlDuration = pollingTimeout + self.options.lockTimeToLive;


                    let heartBeat;
                    const startHeartBeat = function(lockToken, ttlDuration) {
                        heartBeat = setInterval(async function() {
                            try {
                                await self._distributedLock.extend(lockToken, ttlDuration);
                            } catch (error) {
                                clearTimeout(heartBeat);
                                console.error('error in doing heart beat', error, error.name);
                            }
                        }, (ttlDuration / 2));
                    };

                    const stopHeartBeat = async function(lockToken) {
                        if (heartBeat) {
                            clearTimeout(heartBeat);
                            heartBeat = undefined;
                        }
                        if (lockToken) {
                            await self._distributedLock.unlock(lockToken);
                        }
                    };


                    let lockToken;
                    /**
                     * @type {WaitableConsumer}
                     */
                    let waitableConsumer;
                    try {
                        const lockKey = `projection-locks:${projectionGroup}:${projectionId}`;
                        lockToken = await self._distributedLock.lock(lockKey, ttlDuration);
                        debug('protection lock acquired on. processing pending messages', projectionId);

                        startHeartBeat(lockToken, ttlDuration);

                        // acquired a lock. do the job
                        let continueJob = true;
                        let tryContinueProcessProjection = false;

                        let topics = [];
                        let queryArray = [];
                        if (!Array.isArray(projectionConfig.query)) {
                            queryArray.push(projectionConfig.query);
                        } else {
                            queryArray = projectionConfig.query;
                        }

                        for (const q of queryArray) {
                            const streamId = self._getChannel(q);
                            topics.push(streamId);
                        }

                        waitableConsumer = await self._waitableFactory.createWaitableConsumer(topics);

                        while (continueJob) {
                            try {
                                let reason;
                                if (tryContinueProcessProjection == false) {
                                    reason = await waitableConsumer.waitForSignal(self.options.pollingTimeout);
                                }
                                const eventsCount = await self._processProjection(projectionId);

                                console.log('es: reason', reason, eventsCount);
                                tryContinueProcessProjection = (eventsCount == self.options.pollingMaxRevisions);
                            } catch (error) {
                                console.error('error in while loop in shared job', error);
                                // if there is an error then exit while loop and then contend again
                                continueJob = false;
                                tryContinueProcessProjection = false;
                            } finally {
                                continueJob = (currentAssignments.find((item) => item == projectionId) && currentRebalanceId === rebalanceId);
                            }
                        };
                    } catch (error) {
                        if (error.name == 'LockError') {
                            // ignore this just try again later to acquire lock
                            // debug('lost in lock contention. will try again in', ttlDuration);
                        } else {
                            console.error('error in doing shared timer job', error, error.name);
                        }
                    } finally {
                        // REVIEW: moved stopHeartBeat to finally block
                        await stopHeartBeat(lockToken);
                        if (waitableConsumer) {
                            await waitableConsumer.stopWaitingForSignal();
                        }
                    }
                } else {
                    // if projection does not exists, remove from current assignments
                    const foundAssignment = currentAssignments.find((item) => item == projectionId);
                    if (foundAssignment) {
                        const index = currentAssignments.indexOf(foundAssignment);
                        if (index > -1) {
                            currentAssignments.splice(index, 1);
                        }
                    }
                }
            };
        });
    });
};

EventstoreWithProjection.prototype._sleep = async function(timeout, rejectOnTimeout) {
    return new Promise((resolve, reject) => {
        setTimeout(function() {
            if (rejectOnTimeout) {
                reject(new Error('timed out'));
            } else {
                resolve('sleep');
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
 * @returns {Promise<Boolean>} returns Promise of type boolean
 */
EventstoreWithProjection.prototype._processProjection = async function(projectionId) {
    let eventsCount = 0;
    try {
        /**
         * @type {Projection}
         */
        const projection = await this._projectionStore.getProjection(projectionId);
        if (projection) {
            const projectionConfig = projection.configuration;

            let offset = projection && projection.offset ? projection.offset : 0;
            if (isNaN(offset)) {
                offset = 0;
            }

            if (projection.state == 'running') {
                let projectionQuery = _.clone(projectionConfig.query);

                const events = await this._getProjectionEvents(this, projectionQuery, offset, this.options.pollingMaxRevisions);

                eventsCount = _.isArray(events) ? events.length : 0;

                if (events) {
                    debug('got events in getEventsAsync for playback:', events, events.length);
                }

                let errorFault = null;
                let errorEvent = null;
                let errorOffset = null;
                for (let i = 0; i < events.length; i++) {
                    const event = events[i];

                    try {
                        await this._playbackEvent(event, projectionConfig, this.options.eventCallbackTimeout);
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
                    return 0;
                } else {
                    this.emit('playbackSuccess', {
                        projectionId: projectionId,
                        eventsCount: eventsCount
                    });
                }
            }
        }
    } catch (error) {
        console.error('_processJob failed with parameters and error', error);
        throw error;
    }

    return eventsCount;
};

/**
 * @param {Eventstore} eventstore the eventstore to use
 * @param {AggregateQuery} query the query for the aggregate/category that we like to get
 * @param {Number} offset how many events to skip
 * @param {Number} limit max items to return
 * @returns {Promise<Event[]>} returns a Promise that resolves to an array of Events
 */
EventstoreWithProjection.prototype._getProjectionEvents = async function(eventstore, query, offset, limit) {
    let events = [];
    let projectionEventStreamBuffer;
    let channel;

    // CODE REVIEW FEEDBACK/EVALUATE: stream buffer might be abstracted in getEvents
    if (this.options.enableProjectionEventStreamBuffer) {
        channel = this._getChannel(query);
        debug('ProjectionEventStreamBuffer: Channel:', channel, offset, offset + limit);

        if (!this._projectionEventStreamBuffers[channel]) {
            this._createProjectionEventStreamBuffer(query, channel)
        }
        projectionEventStreamBuffer = this._projectionEventStreamBuffers[channel];
    }

    if (projectionEventStreamBuffer) {
        events = projectionEventStreamBuffer.getEventsInBuffer(offset, limit);

        if (events && events.length > 0) {
            debug('ProjectionEventStreamBuffer hit on channel:', channel);
        } else {
            debug('ProjectionEventStreamBuffer miss on channel:', channel);
            events = await this._getEventsAsync(eventstore, query, offset, limit);
            if (events && events.length > 0) {
                projectionEventStreamBuffer.offerEvents(events);
            }
        }
    } else {
        events = await this._getEventsAsync(eventstore, query, offset, limit);
    }
    return events;
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
                self._saveEventAsync(self._outputEventstore, targetQuery, event)
                    .then((events) => {
                        done(events);
                    }).catch(done);
            } else {
                return self._saveEventAsync(self._outputEventstore, targetQuery, event);
            }
        },
        getPlaybackList: function(listName, done) {
            const playbackList = self._getPlaybackListByType(listName, 'mutable');
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
 * @param {ProjectionConfiguration} projectionConfig the object/interface to use to playback the event
 * @param {Number} timeout callback to notify that the job is done
 * @returns {Promise<void>} returns a Promise that resolves to a void
 */
EventstoreWithProjection.prototype._playbackEvent = function(event, projectionConfig, timeout) {
    return new Promise((resolve, reject) => {
        // TODO: fix timeout with promise based callback. how to have an async await timeout handling?
        // TODO: create common functions as code are being repetitive already
        let timeoutHandle = setTimeout(() => {
            reject(new Error('timeout in calling the playbackInterface'));
        }, timeout);

        try {
            const self = this;

            debug('event in _playbackEvent', event);

            let eventHandlerFunc = self._getEventHandlerFunc(projectionConfig, event);

            if (eventHandlerFunc) {
                if (projectionConfig.outputState === 'true') {
                    this._getProjectionState(projectionConfig, event).then((projectionState) => {
                        debug('got projectionState', projectionState);
                        const mutableState = _.cloneDeep(projectionState.state);

                        const doneCallback = function(error) {
                            clearTimeout(timeoutHandle);
                            timeoutHandle = null;
                            if (error) {
                                console.error('error in playbackFunction with params and error', event, projectionConfig.projectionId, error);
                                reject(error)
                            } else {
                                // if the old state and the mutableState is not equal (meaning the playback updated it) then output a state
                                if (!_.isEqual(projectionState.state, mutableState)) {
                                    debug('state changed, saving new state', projectionState.state, mutableState);
                                    // add event metadata to the state
                                    mutableState._meta = {
                                        fromEvent: event
                                    };
                                    self._saveProjectionState(projectionConfig.projectionId, projectionState.id, mutableState, self._context).then(resolve).catch(reject);
                                } else {
                                    debug('state did not change, continuing', )
                                    resolve();
                                }
                            }
                        }

                        const result = eventHandlerFunc.call(projectionConfig.playbackInterface, mutableState, event, self._getPlaybackFunctions(mutableState), doneCallback);
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
                            console.error(JSON.stringify(projectionConfig));
                            reject(error)
                        } else {
                            resolve();
                        }
                    };

                    const result = eventHandlerFunc.call(projectionConfig.playbackInterface, null, event, self._getPlaybackFunctions(), doneCallback);
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
            console.error(JSON.stringify(projectionConfig));
            reject(error);
        }
    })
};


/**
 * @param {Eventstore} eventstore the eventstore to use
 * @param {AggregateQuery} targetQuery the query to emit the event
 * @param {Object} event the event to emit
 * @param {DoneCallbackFunction} done the last event that built this projection state
 * @returns {Promise<Event[]>} returns a Promise that resolves to Event Array
 */
EventstoreWithProjection.prototype._saveEventAsync = async function(eventstore, targetQuery, event) {
    try {
        let isSuccessful = false;
        let saveEventsError = null;
        let retries = 0;

        while (!isSuccessful && retries < this.options.maxConcurrencyRetryCount) {
            let result;
            try {
                const stream = await this._getLastEventAsStreamAsync(eventstore, targetQuery);

                // TODO: add eventLink and eventSource
                await this._addEventToStream(stream, event);
                result = await this._commitStream(stream);
            } catch (error) {
                if (error instanceof EventStoreDuplicateError) {
                    saveEventsError = error;
                    retries++;
                    continue;
                } else {
                    throw error;
                }
            }
            isSuccessful = true;
            return result;
        }

        if (!isSuccessful) {
            throw saveEventsError;
        }
    } catch (error) {
        console.error('error in _saveEventAsync', targetQuery, event, error);
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
 * @param {ProjectionConfiguration} projectionConfig the projection
 * @param {Event} lastProjectionEvent the last event got from the projection 
 * @returns {Promise<ProjectionState>} returns a Promise that resolves to a void
 */
EventstoreWithProjection.prototype._getProjectionState = async function(projectionConfig, lastProjectionEvent) {
    // default is no partition format: <projectionid>-result
    let streamId = this._getProjectionStateStreamId(projectionConfig, lastProjectionEvent);

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
        if (typeof projectionConfig.playbackInterface.$init === 'function') {
            try {
                const initState = projectionConfig.playbackInterface.$init();
                if (initState) {
                    state = initState;
                }
            } catch (error) {
                // NOTE: just log for now. and not let errors stop the playback loop
                console.error('error in playbackInterface.$init with params and error', projectionConfig, lastProjectionEvent, error)
            }
        }
    }

    return {
        id: streamId,
        state: state
    }
};

/**
 * @param {ProjectionConfiguration} projectionConfig the projection
 * @param {Event} lastProjectionEvent the query 
 * @returns {String} returns a Promise that resolves to a void
 */
EventstoreWithProjection.prototype._getProjectionStateStreamId = function(projectionConfig, lastProjectionEvent) {
    // default is no partition format: <projectionid>-result

    let streamId = `${projectionConfig.projectionId}-result`;
    if (projectionConfig.partitionBy === 'stream') {
        let streamPartitionedStreamId = `${projectionConfig.projectionId}`;
        // format: <projectionid>-result[-<context>][-<aggregate>]-<aggregateId>
        if (lastProjectionEvent.context)
            streamPartitionedStreamId += `-${lastProjectionEvent.context}`;
        if (lastProjectionEvent.aggregate)
            streamPartitionedStreamId += `-${lastProjectionEvent.aggregate}`;
        streamPartitionedStreamId += `-${lastProjectionEvent.aggregateId || lastProjectionEvent.streamId}`;
        streamPartitionedStreamId += '-result';

        streamId = streamPartitionedStreamId;
    } else if (typeof projectionConfig.partitionBy === 'function') {
        try {
            const partitionId = projectionConfig.partitionBy(lastProjectionEvent);

            // if partitionId is defined then use it, else just use the default partititon which is by projection
            if (partitionId) {
                // format: <projectionid>-<paritionId>-result
                streamId = `${projectionConfig.projectionId}-${partitionId}-result`;
            }

        } catch (error) {
            // NOTE: log for now and use the default as the streamId (no partition)
            console.error('error in calling projection.partitionBy with params and error', projectionConfig, lastProjectionEvent, error);
        }
    }

    debug('_getProjectionStateStreamId streamId', lastProjectionEvent);

    return streamId;
};

/**
 * @param {ProjectionConfiguration} projectionConfig the event 
 * @param {Event} event the event 
 * @returns {String} returns the function that the projection is handling
 */
EventstoreWithProjection.prototype._getEventHandlerFunc = function(projectionConfig, event) {
    // check if this event is handled by this projection
    const eventName = event.payload[this.options.eventNameFieldName];

    let eventHandlerFunc = projectionConfig.playbackInterface[eventName];

    if (!eventHandlerFunc) {
        // if no handler for this event, then check if there is an $any event handler.
        // if yes then use that
        const anyEventHandlerFunc = projectionConfig.playbackInterface['$any'];
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
 * @param {ProjectionConfiguration} projectionConfig parameters for the projection
 * @param {Boolean} shouldUpdateProjectionStore update the store 
 * @returns {Promise<void>} returns a Promise of type void
 */
EventstoreWithProjection.prototype._project = async function(projectionConfig, shouldUpdateProjectionStore) {
    try {
        if (this.options.enableProjection === false) {
            throw new Error('enableProjection true is required for projections');
        }

        if (!projectionConfig) {
            throw new Error('projection is required');
        }

        if (!projectionConfig.projectionId) {
            throw new Error('projectionId is required');
        }

        if (!projectionConfig.query) {
            throw new Error('query is required');
        } else {
            let queryArray = [];
            if (!Array.isArray(projectionConfig.query)) {
                queryArray.push(projectionConfig.query);
            } else {
                queryArray = projectionConfig.query;
            }
            _.forEach(queryArray, function(q) {
                if (!q.aggregate && !q.context && !q.aggregateId && !q.streamId) {
                    throw new Error('at least an aggregate, context or aggregateId/streamId is required');
                }
            });
        }

        this._hasProjections = true;

        // use a distributed lock to make sure that we only create one event for the projection stream
        const projectionKey = `projection-group:${this.options.projectionGroup}:projection:${projectionConfig.projectionId}`;

        /**
         * @type {Projection}
         */
        const projection = {
            projectionId: projectionConfig.projectionId,
            configuration: projectionConfig,
            context: this._context,
            offset: null
        };

        // set projection to private list of projections
        this._projections[projection.projectionId] = projection;

        let lockToken;
        const lockKey = projectionKey;
        try {
            lockToken = await this._distributedLock.lock(lockKey, 1900);
        } catch (error) {
            console.info('was not able to acquire lock. just continue');
        }

        if (shouldUpdateProjectionStore) {
            // add projection to the store if not exists
            const storedProjection = await this._projectionStore.getProjection(projection.projectionId);

            if (storedProjection) {
                this._projectionStore.updateProjection(projection);
            } else {
                if (projection && projection.configuration && projection.configuration.fromOffset === 'latest') {
                    projection.offset = await this._getLatestOffsetAsync();
                }

                this._projectionStore.createProjection(projection);
            }
        }

        // initialize the playback list. make sure that it is also inside the lock
        await this._initPlaybackList(projection);

        // initialize the playback list. make sure that it is also inside the lock
        await this._initStateList(projection);

        // unlock
        if (lockToken) {
            try {
                await this._distributedLock.unlock(lockToken);
            } catch (error) {
                console.info('was not able to unlock. just continue');
            }
        }
    } catch (error) {
        console.error('error in _project with params and error', projectionConfig, error);
        throw error;
    }
};

/**
 * @param {Projection} projection the projection to initialize with a list
 * @returns {Promise<void>} returns a Promise of type void
 */
EventstoreWithProjection.prototype._initStateList = async function(projection) {

    if (projection.configuration.stateList) {

        let stateLists;

        if (_.isArray(projection.configuration.stateList)) {
            stateLists = projection.configuration.stateList;
        } else {
            stateLists = [projection.configuration.stateList];
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

    if (projection.configuration.playbackList) {
        if (!this.options.listStore) {
            throw new Error('listStore must be provided in the options');
        }

        const EventstorePlaybackList = require('./eventstore-playback-list');
        const playbackList = new EventstorePlaybackList({
            listName: projection.configuration.playbackList.name
        }, this._playbackListStore);
        await playbackList.createList(projection.configuration.playbackList);

        this._playbackLists[projection.configuration.playbackList.name] = playbackList;
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
 * @returns {Promise<void>} returns a Promise of type void
 */
EventstoreWithProjection.prototype._getLatestOffsetAsync = async function() {
    return new Promise((resolve, reject) => {
        this.store.getLatestOffset((error, latestOffset) => {
            if (error) {
                reject(error);
            }
            resolve(latestOffset);
        });
    });
};

/**
 * @param {EventStream} stream the stream into which to add the projection
 * @returns {Promise<Event[]>} returns a Promise of type Event Array
 */
EventstoreWithProjection.prototype._commitStream = async function(stream) {
    debug('_commitStream called', stream);
    return new Promise((resolve, reject) => {
        try {
            stream.commit(function(err, eventstream) {
                if (err) {
                    reject(err);
                } else {
                    const events = [];
                    if (eventstream && eventstream.events && Array.isArray(eventstream.events) && eventstream.events.length > 0) {
                        eventstream.events.forEach(async function(emittedEvent) {
                            if (emittedEvent instanceof Event) {
                                events.push(emittedEvent);
                            }
                        });
                    }
                    resolve(events);
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
 * Creates an internal ProjectionEventStreamBuffer for the specified Projection query.
 */
EventstoreWithProjection.prototype._createProjectionEventStreamBuffer = function(query, channel) {
    const self = this;

    // initialize projection event stream buffer
    const bucket = self._getCurrentStreamBufferBucket();

    const options = {
        es: self,
        query: query,
        channel: channel,
        bucket: bucket,
        bufferCapacity: self.options.pollingMaxRevisions,
        ttl: STREAM_BUFFFER_TTL,
        onOfferEvent: function(currentBucket, channel) {
            if (!self._projectionEventStreamBufferLRULocked) {
                const heapPercentage = self._getHeapPercentage();
                debug('Projection Event Stream Buffer - HEAP %', heapPercentage);
                if (heapPercentage >= STREAM_BUFFER_MEMORY_THRESHOLD) {
                    self._cleanOldestProjectionEventStreamBufferBucket();
                }
            }

            const newBucket = self._getCurrentStreamBufferBucket();
            if (self._projectionEventStreamBufferBuckets[currentBucket]) {
                delete self._projectionEventStreamBufferBuckets[currentBucket][channel];
            }

            if (!self._projectionEventStreamBufferBuckets[newBucket]) {
                self._projectionEventStreamBufferBuckets[newBucket] = {};
            }

            self._projectionEventStreamBufferBuckets[newBucket][channel] = new Date().getTime();
            self._projectionEventStreamBuffers[channel].bucket = newBucket;
        },
        onInactive: (bucket, channel) => {
            debug('Closing the Projection Event Stream Buffer for channel', channel);
            self._projectionEventStreamBuffers[channel].close();
        },
        onClose: (bucket, channel) => {
            delete self._projectionEventStreamBuffers[channel];

            if (self._projectionEventStreamBufferBuckets[bucket]) {
                delete self._projectionEventStreamBufferBuckets[bucket][channel];
            }
        }
    }

    self._projectionEventStreamBuffers[channel] = new ProjectionEventStreamBuffer(options);

    if (!self._projectionEventStreamBufferBuckets[bucket]) {
        self._projectionEventStreamBufferBuckets[bucket] = {};
    }

    self._projectionEventStreamBufferBuckets[bucket][channel] = new Date().getTime();
};

/**
 * Cleans up the oldest bucket from subscription event stream buffer bucket and locks the LRU execution to prevent getting executed multiple times
 */
EventstoreWithProjection.prototype._cleanOldestSubscriptionEventStreamBufferBucket = function() {
    this._subscriptionEventStreamBufferLRULocked = true;
    debug('locking SubscriptionEventStreamBuffer LRU');

    const buckets = Object.keys(this._subscriptionEventStreamBufferBuckets);
    if (buckets && buckets.length > 0) {
        const sortedBuckets = _.sortBy(buckets, (i) => i);
        const bucketToClean = sortedBuckets[0];
        const channels = Object.keys(this._subscriptionEventStreamBufferBuckets[bucketToClean]);

        debug('cleaning SubscriptionEventStreamBuffer bucket');
        debug(bucketToClean);

        if (channels && channels.length > 0) {
            channels.forEach((channel) => {
                this._subscriptionEventStreamBufferLRUCleaner.push({
                    channel
                });
            });
        }

        delete this._subscriptionEventStreamBufferBuckets[bucketToClean];
    }
};

/**
 * Cleans up the oldest bucket from projection event stream buffer bucket and locks the LRU execution to prevent getting executed multiple times
 */
EventstoreWithProjection.prototype._cleanOldestProjectionEventStreamBufferBucket = function() {
    this._projectionEventStreamBufferLRULocked = true;
    debug('locking ProjectionEventStreamBuffer LRU');

    const buckets = Object.keys(this._projectionEventStreamBufferBuckets);
    if (buckets && buckets.length > 0) {
        const sortedBuckets = _.sortBy(buckets, (i) => i);
        const bucketToClean = sortedBuckets[0];
        const channels = Object.keys(this._projectionEventStreamBufferBuckets[bucketToClean]);

        debug('cleaning ProjectionEventStreamBuffer bucket');
        debug(bucketToClean);

        if (channels && channels.length > 0) {
            channels.forEach((channel) => {
                this._projectionEventStreamBufferLRUCleaner.push({
                    channel
                });
            });
        }

        delete this._projectionEventStreamBufferBuckets[bucketToClean];
    }
};

module.exports = EventstoreWithProjection;