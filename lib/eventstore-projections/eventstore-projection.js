const Eventstore = require('../eventstore');
const _ = require('lodash');
const util = require('util');
const shortid = require('shortid');
const debug = require('debug')('eventstore:projection');
const debugPP = require('debug')('eventstore:process:projection');
const debugBatch = require('debug')('eventstore:projection:batch');
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
const {
    WaitableConsumer
} = require('./waitable-factory/waitable-consumer');
const Semaphore = require('./semaphore');
const BullQueue = require('bull');

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
 * @property {Boolean} stopOnFaulted stops the projection when there is an error/fault that happens in an event handler. default is true
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
 * @property {Number} eventCallbackTimeout overrides the eventCallbackTimeout of the evenstore
 * @property {Array<string>} concurrentEventNames the event names that we can run asynchronously
 * @property {Number} concurrencyCount number of events that we can run asynchronously
 * @property {Number} shard number of events that we can run asynchronously
 * @property {Number} partition number of events that we can run asynchronously
 */

/**
 * Projection
 * @typedef {Object} Projection
 * @property {string} projectionId
 * @property {ProjectionConfiguration} configuration
 * @property {String} context
 * @property {String} state
 */

/**
 * ProjectionTask
 * @typedef {Object} ProjectionTask
 * @property {string} projectionTaskId
 * @property {string} projectionId
 * @property {String} shard
 * @property {String} partition
 * @property {String} [offset]
 * @property {Number} [processedDate]
 * @property {Boolean} [isIdle]
 * @property {Error} [error]
 * @property {Object} [errorEvent]
 * @property {String} [errorOffset]
 * @property {Projection} projection
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


class EventstoreWithProjection extends Eventstore {
    constructor(opts, store, distributedSignal, distributedLock, playbackListStore, playbackListViewStore, projectionStore, stateListStore) {
        super(opts, store);
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
            membershipPollingTimeout: 10000,
            stopOnFaulted: true,
            shouldDoTaskAssignment: true
        };

        const options = _.defaults({}, opts, defaults);
        this.options = _.defaults({}, this.options, options);

        this.shard = this.options.shard;
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
            self._projectionTasks = {};
            self._playbackLists = {};
            self._stateLists = {};
            self._playbackListViews = {};
            self._jobs = {};
            self._userDefinedFunctions = {};
            self._projectionEventSemaphores = {};
            self._currentAssignments = [];
            self._currentRebalanceId;

            if (options.enableProjectionEventStreamBuffer) {
                // Initialize Projection Stream Buffers
                self._projectionEventStreamBuffers = {};
                self._projectionEventStreamBufferBuckets = {};
                self._projectionEventStreamBufferLRULocked = false;

                self._projectionEventStreamBufferLRUCleaner = async.queue(function(task, callback) {
                    const channel = task.channel;

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
        }

        this._requiredEventMappings = {
            context: 'context',
            aggregate: 'aggregate',
            aggregateId: 'aggregateId'
        }

        this.eventMappings = this._requiredEventMappings;
    }

    init(callback) {
        if (!this.publisher && this.options.enableProjection) {
            this.useEventPublisher(function(event, done){
                done();
            });
        }

        super.init(callback);
    }

    /**
     * 
     * @param {Function} fn the function to be injected
     * @returns 
     */
    useEventPublisher(fn) {
        const self = this;
        this._useEventPublisherCallback = fn;
        super.useEventPublisher(function (event, done) {
            if (self._useEventPublisherCallback) {
                self._useEventPublisherCallback(event, function() {
                    // TODO: this also assumes that es.defineEventMappings have aggregateId or the event has aggregateId
                    if (self.options.enableProjection) {
                        self._sendSignal({
                            context: event.context,
                            aggregate: event.aggregate,
                            aggregateId: event.aggregateId
                        }, done);
                    }
                })
            } else {
                if (self.options.enableProjection) {
                    self._sendSignal({
                        context: event.context,
                        aggregate: event.aggregate,
                        aggregateId: event.aggregateId
                    }, done);
                }
            }
        });
    }

    /**
     * 
     * @param {Object} mappings 
     * @returns 
     */
    defineEventMappings(mappings) {
        // NOTE: overridden the implementation of the Eventstore class
        // context, aggregate and aggregateId are required in useEventPublisher so this is overridden
        const combinedMapping = _.defaults(mappings, this._requiredEventMappings);
        return super.defineEventMappings(combinedMapping);
    };

    /**
     * @returns {void} - returns void
     */
    deactivatePolling() {
        const pollers = Object.values(this._bufferedEventSubscriptionPool);
        for (let i = 0; i < pollers.length; i++) {
            pollers[i].deactivate();
        }

        // this.pollingActive = false;
    };

    /**
     * @returns {void} - returns void
     */
    activatePolling() {
        const pollers = Object.values(this._bufferedEventSubscriptionPool);
        for (let i = 0; i < pollers.length; i++) {
            pollers[i].activate();
        }
    };

    /**
     * @param {DoneCallbackFunction} callback success/error callback
     * @returns {void} - returns void
     */
    close(callback) {
        debug('close called');
        // just preserving that adrai's public functions are callback based but internally we can just use async/await for elegance
        this._close().then(callback).catch(callback);
    };

    async _close() {
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
    stopAllProjections(callback) {
        debug('stopAllProjections called');
        debug('ES-SHARD', this.options.shard, 'stopAllProjections called');
        // just preserving that adrai's public functions are callback based but internally we can just use async/await for elegance
        this._stopAllProjections().then(callback).catch(callback);
    };

    async _stopAllProjections() {
        try {
            debugPP('_stopAllProjections this._currentAssignments', this._currentAssignments);
            this._currentAssignments = [];

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
    closeSubscriptionEventStreamBuffers(callback) {
        this._subscriptionEventStreamBufferLRULocked = true;
        const channels = Object.keys(this._subscriptionEventStreamBuffers);

        for (let i = 0; i < channels.length; i++) {
            const channel = channels[i];
            this._subscriptionEventStreamBuffers[channel].close();
        }
        this._subscriptionEventStreamBuffers = {};
        this._subscriptionEventStreamBufferBuckets = {};
        this._subscriptionEventStreamBufferLRULocked = false;
        callback(null);
    };

    /**
     * Closes all Projection Event StreamBuffers
     * @param {DoneCallbackFunction} callback success/error callback
     * @returns {void} - returns void
     */
    closeProjectionEventStreamBuffers(callback) {
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
        callback(null);
    };

    /**
     * @param {DoneCallbackFunction} callback success/error callback
     * @returns {void} - returns void
     */
    startAllProjections(callback) {
        debug('startAllProjections called');
        debug(callback);
        // just preserving that adrai's public functions are callback based but internally we can just use async/await for elegance
        this._startAllProjections().then(() => {
            debug("startAllProjections did succeed");
            callback();
        }).catch((err) => {
            debug("startAllProjections did error");
            callback();
        });
    };

    async _startAllProjections() {
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
            // TODO: review for smater way
            if (this.options.shouldDoTaskAssignment) {
                this._doTaskAssignment(this.options.projectionGroup);
            }

            debug('_startAllProjections called done');
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
    project(projectionConfig, callback) {
        debug('ES-SHARD', this.shard, 'projection called with params:', projectionConfig);
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
    subscribe(query, revision, onEventCallback, onErrorCallback) {
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
                    waitableConsumer: self._waitableFactory.createWaitableConsumer([channel]),
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
    unsubscribe(token) {
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
    getPlaybackList(listName) {
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
    _getPlaybackListByType(listName, type) {
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
    getStateList(listName, state) {
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
    getPlaybackListView(listName, done) {
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
    registerPlaybackListView(listName, listQuery, totalCountQuery, opts, done) {
        let alias = undefined;
        let paginationPrimaryKeys = undefined;
        let defaultPaginationPrimaryKeySortDirection = undefined;
        let forceIndexHints = undefined;
        let ignoreIndexHints = undefined;
        if (done == undefined) {
            // NOTE: if alias is function, then it is done
            done = opts;
        } else {
            alias = opts.alias;
            paginationPrimaryKeys = opts.paginationPrimaryKeys;
            defaultPaginationPrimaryKeySortDirection = opts.defaultPaginationPrimaryKeySortDirection;
            forceIndexHints = opts.forceIndexHints;
            ignoreIndexHints = opts.ignoreIndexHints;
        }

        this._registerPlaybackListView(listName, listQuery, totalCountQuery, alias, paginationPrimaryKeys, defaultPaginationPrimaryKeySortDirection, forceIndexHints, ignoreIndexHints).then(done).catch(done);
    };

    /**
     * @param {String} functionName the name of the function to register
     * @param {Function} theFunction the function to call
     * @returns {void} Returns void.
     */
    registerFunction(functionName, theFunction) {
        // just set it. if functionName already exists just override
        this._userDefinedFunctions[functionName] = theFunction;
    };

    /**
     * @returns {Promise<Array<Projection>} returns Promise of type void
     */
    getProjections(done) {
        try {
            return this._projectionStore.getProjections().then((data) => done(null, data)).catch(done);;
        } catch (error) {
            console.error('getProjections failed with parameters and error', error);
            done(error);
        }
    };

    /**
     * Gets a lists of projection tasks
     * @param {String} projectionId the projection id to use. optional
     * @param {Function} done callback function
     */
    getProjectionTasks(projectionId, done) {
        let callback;
        if (typeof projectionId === 'function') {
            callback = projectionId;
            debug('CLUSTERED getProjectionTasks: no id');
            this._getProjectionTasks().then((projectionTasks) => {
                callback(null, projectionTasks);
            }).catch(callback);
        } else {
            callback = done;
            debug('CLUSTERED getProjectionTasks:', projectionId);
            this._getProjectionTasks(projectionId).then((projectionTasks) => {
                callback(null, projectionTasks);
            }).catch(callback);
        }
    };

    /**
     * 
     * @param {string} projectionId 
     * @returns {Promise<Projection>}
     */
    getProjection(projectionId, done) {
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
    getProjectionByName(projectionName, done) {
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
    pauseProjection(projectionId, done) {
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
    runProjection(projectionId, forced, done) {
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
    async _runProjection(projectionId, forced) {
        debug('_runProjection called with parameters', projectionId, forced);
        try {
            const projectionTasks = await this._projectionStore.getProjectionTasks(projectionId);
            const projectionTaskIds = [];
            for(const projectionTask of projectionTasks) {
                const projectionTaskId = projectionTask.projectionTaskId;
                projectionTaskIds.push(projectionTaskId);

                if (forced) {
                    await this._projectionStore.setOffset(projectionTaskId, projectionTask.errorOffset);
                }
                await this._projectionStore.setError(projectionTaskId, null, null, null);
            }
            await this._projectionStore.setState(projectionId, 'running');

            debug('_runProjection projection task ids', projectionTaskIds);
            if (this._taskGroup) {
                this._taskGroup.addTasks(projectionTaskIds);
            }

            return projectionTaskIds;
        } catch (error) {
            console.error('_runProjection failed with parameters and error', projectionId, forced, error);
            throw error;
        }
    };


    /**
     * @param {string} projectionId
     * @returns {Promise}
     */
    resetProjection(projectionId, done) {
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
    deleteProjection(projectionId, done) {
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
     * 
     * @param {string} aggregateId 
     */
    _sendSignal(query, done) {
        const self = this;
        const partition = this.store.getPartitionId(query.aggregateId);
        const shard = self.options.shard;
        const channels = self._queryToChannels(query, shard, partition);

        for (const channel of channels) {
            self._waitableProducer.signal(channel);
        }

        done(null, partition);
    };

    /**
     * @returns {Promise<Array<Projection>} returns Promise of type void
     */
    async _getProjections(context) {
        try {
            return this._projectionStore.getProjections(context);
        } catch (error) {
            console.error('getProjections failed with parameters and error', error);
            throw error;
        }
    };

    /**
     * Gets a lists of projection tasks
     * @param {String} projectionId the projection id to use. optional
     * @returns {Promise<Array<ProjectionTask>} returns Promise of type void
     */
    async _getProjectionTasks(projectionId) {
        try {
            return this._projectionStore.getProjectionTasks(projectionId);
        } catch (error) {
            console.error('_getProjectionTasks failed with parameters and error', error);
            throw error;
        }
    };

    /**
     * 
     * @param {string} projectionId 
     */
    async _deleteProjection(projectionId) {
        try {
            debug('CLUSTERED: _deleteProjection: ', projectionId);
            const projectionTaskIds = [];
            const projectionTasks = await this._projectionStore.getProjectionTasks(projectionId);
            debug('_deleteProjection: tasks', projectionTasks);
            for(const projectionTask of projectionTasks) {
                const projectionTaskId = projectionTask.projectionTaskId;
                projectionTaskIds.push(projectionTaskId);
                delete this._projectionTasks[projectionTaskId];
            }

            if (this._taskGroup) {
                this._taskGroup.removeTasks(projectionTaskIds);
            }
            delete this._projections[projectionId];

            const projection = await this._projectionStore.getProjection(projectionId)

            if (projection && projection.stateList) {
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

            if (projection && projection.playbackList) {
                await this._playbackListStore.destroy(projection.playbackList.name);
            }

            await this._projectionStore.deleteProjection(projectionId);

            debug('CLUSTERED: _deleteProjection DONE: ', projectionId);
            return projectionTaskIds;
        } catch (error) {
            console.error('_resetProjection failed with parameters and error', projectionId, error);
        }
    };

    /**
     * 
     * @param {string} projectionId 
     */
    async _resetProjection(projectionId) {
        try {
            /**
             * @type {Projection}
             */
            const projection = await this._projectionStore.getProjection(projectionId);

            if (projection) {
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

                const projectionTasks = await this._projectionStore.getProjectionTasks(projectionId);
                for(const projectionTask of projectionTasks) {
                    const projectionTaskId = projectionTask.projectionTaskId;
                    await this._projectionStore.setOffset(projectionTaskId, this._serializeProjectionOffset([0]));
                }
            }
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
    async _registerPlaybackListView(listName, listQuery, totalCountQuery, alias, paginationPrimaryKeys, defaultPaginationPrimaryKeySortDirection, forceIndexHints, ignoreIndexHints) {
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
                alias: alias,
                paginationPrimaryKeys: paginationPrimaryKeys,
                defaultPaginationPrimaryKeySortDirection: defaultPaginationPrimaryKeySortDirection,
                forceIndexHints: forceIndexHints,
                ignoreIndexHints: ignoreIndexHints
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
    async _doTaskAssignment(projectionGroup) {
        const self = this;
        const tasks = [];

        for (let projectionId in self._projections) {
            tasks.push(projectionId);
        }

        self._taskGroup = new TaskAssignmentGroup({
            initialTasks: tasks,
            createRedisClient: self.options.redisCreateClient,
            groupId: projectionGroup,
            membershipPollingTimeout: self.options.membershipPollingTimeout,
            distributedLock: self._distributedLock
        });

        await self._taskGroup.join();
        self._taskGroup.on('rebalance', async (updatedAssignments, rebalanceId) => {
            self.rebalance(updatedAssignments, rebalanceId);
        });
    };

    async rebalance(updatedAssignments, rebalanceId) {
        const self = this;
        self._currentAssignments = updatedAssignments;
        self._currentRebalanceId = rebalanceId;

        debug('ES-SHARD', self.options.shard, 'REBALANCE CALLED', updatedAssignments);
        self.emit('rebalance', updatedAssignments);

        _.each(updatedAssignments, async function(projectionTaskId) {
            while (self._currentAssignments.find((item) => item == projectionTaskId) && self._currentRebalanceId === rebalanceId) {
                try {
                    const projectionTask = self._projectionTasks[projectionTaskId];
                    debug('ES-SHARD', self.options.shard, 'FOUND PROJECTION TASK:', projectionTaskId);
                    if (projectionTask) {
                        const projectionConfig = projectionTask.projection.configuration;
                        const pollingTimeout = self.options.pollingTimeout;
                        const ttlDuration = pollingTimeout + self.options.lockTimeToLive;

                        let heartBeat;
                        let isHeartBeating = false;
                        const startHeartBeat = function(lockToken, ttlDuration) {
                            heartBeat = setInterval(async function() {
                                try {
                                    await self._distributedLock.extend(lockToken, ttlDuration);
                                } catch (error) {
                                    isHeartBeating = false;
                                    clearTimeout(heartBeat);
                                }
                            }, (ttlDuration / 2));
                            isHeartBeating = true;
                        };

                        const stopHeartBeat = async function(lockToken) {
                            isHeartBeating = false;
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
                            const lockKey = `projection-locks:${self.options.projectionGroup}:${projectionTaskId}`;
                            lockToken = await self._distributedLock.lock(lockKey, ttlDuration);
                            debug('protection lock acquired on. processing pending messages', projectionTaskId);

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
                                const streamId = self._getChannel(q, projectionTask.shard, projectionTask.partition);
                                topics.push(streamId);
                            }

                            waitableConsumer = self._waitableFactory.createWaitableConsumer(topics);
                            while (continueJob) {
                                try {
                                    if (!isHeartBeating) {
                                        break;
                                    }

                                    if (tryContinueProcessProjection == false) {
                                        await waitableConsumer.waitForSignal(self.options.pollingTimeout);
                                    }
                                    debug('ES-SHARD', self.options.shard, 'PROCESSING PROJECTION', projectionTaskId);
                                    const eventsCount = await self._processProjection(projectionTaskId);
                                    tryContinueProcessProjection = (eventsCount == self.options.pollingMaxRevisions);
                                } catch (error) {
                                    console.error('error in while loop in shared job', error);
                                    continueJob = false;
                                    tryContinueProcessProjection = false;
                                } finally {
                                    continueJob = (self._currentAssignments.find((item) => item == projectionTaskId) && self._currentRebalanceId === rebalanceId);
                                    if (!continueJob) {
                                        debug('ES-SHARD', self.options.shard, 'NO LONGER CONTINUING JOB', projectionTaskId);
                                    }
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
                        const foundAssignment = self._currentAssignments.find((item) => item == projectionTaskId);
                        if (foundAssignment) {
                            const index = self._currentAssignments.indexOf(foundAssignment);
                            if (index > -1) {
                                self._currentAssignments.splice(index, 1);
                            }
                        }
                    }
                } catch (error) {
                    if (error.name == 'LockError') {
                        console.error('LockError in outer while loop of task. Contend again', error);
                    } else {
                        console.error('Unknown error in outer while loop of task. Will just continue to contend as well', error, error.name);
                    }
                }
            };
            debug('ES-SHARD', self.options.shard, 'JOB COMPLETE', projectionTaskId);
        });
    }

    async _sleep(timeout, rejectOnTimeout) {
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
    _enhanceCallback(owner, callback, preCallback) {
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
     * @param {AggregateQuery[]} queryArray 
     * @returns {String} - returns a dot delimited channel
     */
    _getCompositeChannel(queryArray) {
        let channel = '';
        
        const sortedQuery = _.sortBy(queryArray, [function(q) { 
            return [
                q.context,
                q.aggregate,
                q.aggregateId,
                q.shard,
                q.partition
            ]; 
        }]);

        for(let i = 0; i < sortedQuery.length; i++) {
            const query = sortedQuery[i];
            if (i > 0) {
                channel += '-';
            }
            channel += this._getChannel(query, query.shard, query.partition);
        }
        
        return channel;
    };

    /**
     * @param {AggregateQuery} query 
     * @param {Number} shard
     * @param {string} partition
     * @returns {String} - returns a dot delimited channel
     */
    _getChannel(query, shard, partition) {
        if (_.isNil(shard) || _.isNil(partition)) {
            return `${query.context || 'all'}.${query.aggregate || 'all'}.${query.aggregateId || 'all'}`;
        } else {
            return `${query.context || 'all'}.${query.aggregate || 'all'}.${query.aggregateId || 'all'}.${shard}.${partition}`;
        }
    };

    /**
     * Forms filtered query object for am object
     * @param {Any} object any object that may contain query properties 
     * @returns {AggregateQuery} - returns an aggregate query
     */
    _getQuery(object) {
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
    async _processProjection(projectionTaskId) {
        let eventsCount = 0;
        try {
            /**
             * @type {ProjectionTask}
             */
            const projectionTask = await this._projectionStore.getProjectionTask(projectionTaskId);
            if (projectionTask) {
                // console.log('   _processProjection: Found projection task state', projectionTask.projection.state);
                // TODO: this is only for multi-current. What if single concurrent?
                const projectionConfig = projectionTask.projection.configuration;
                let offset = projectionTask && projectionTask.offset ? this._deserializeProjectionOffset(projectionTask.offset) : 0;
                if (isNaN(offset) && !Array.isArray(offset)) {
                    offset = 0;
                } else if (Array.isArray(offset) && isNaN(offset[0])) {
                    offset = [0];
                }

                if (projectionTask.projection.state == 'running' || (projectionTask.projection.state == 'faulted' && !this.options.stopOnFaulted)) {
                    debug('ES-SHARD', this.shard, 'RUNNING TASK', projectionTaskId);
                    let projectionQuery = _.clone(projectionConfig.query);

                    // add partition to query
                    if (!_.isNil(projectionTask.shard) && !_.isNil(projectionTask.partition)) {
                        if (_.isArray(projectionQuery)) {
                            for (let query of projectionQuery) {
                                query.shard = projectionTask.shard;
                                query.partition = projectionTask.partition === '__unpartitioned' ? undefined : projectionTask.partition;
                            }
                        } else {
                            projectionQuery.shard = projectionTask.shard;
                            projectionQuery.partition = projectionTask.partition === '__unpartitioned' ? undefined : projectionTask.partition;
                        }
                    }

                    const events = await this._getProjectionEvents(this, projectionQuery, offset, this.options.pollingMaxRevisions);

                    eventsCount = _.isArray(events) ? events.length : 0;

                    // if (eventsCount) {
                        // debug('ES-SHARD', this.shard, 'PROJECTION TASK GOT EVENTS AT OFFSET', offset, projectionTaskId, events.length);
                        // console.log('got events in getEventsAsync for playback:', events, events.length, projectionConfig.shard, projectionConfig.partition, projectionQuery);
                    // } else {
                        // debug('ES-SHARD', this.shard, 'PROJECTION TASK NO EVENTS AT OFFSET', offset, projectionTaskId);
                    // }

                    let errorFault = null;
                    let errorEvent = null;
                    let errorOffset = null;

                    for (let i = 0; i < events.length; i++) {
                        const event = events[i];

                        try {
                            await this._playbackEvent(event, projectionConfig, projectionConfig.eventCallbackTimeout || this.options.eventCallbackTimeout);
                            if (Array.isArray(offset)) {
                              offset[this.shard] = event.eventSequence;
                            } else {
                              offset = event.eventSequence;
                            }
                        } catch (error) {
                            console.error('error in playing back event in partitioned queue with params and error', event, projectionTask.projectionTaskId, error);
                            // send to an error stream for this projection with streamid: ${projectionId}-errors.
                            // this lets us see the playback errors as a stream and resolve it manually

                            errorFault = error;
                            errorEvent = event;
                            errorOffset = this._serializeProjectionOffset(event.eventSequence);

                            /** @type {AggregateQuery} */
                            const errorStreamQuery = {
                                context: 'projection-errors',
                                aggregate: projectionTask.projectionId,
                                aggregateId: `${projectionTask.projectionTaskId}-errors`
                            }
                            try {
                                await this._saveEventAsync(this._outputEventstore, errorStreamQuery, {
                                    event: event.payload,
                                    error: error ? JSON.stringify(error, Object.getOwnPropertyNames(error)) : null
                                });
                            } catch (error) {
                                console.error('error in saving to projection-errors with params', errorStreamQuery, event, error);
                                console.error('critical error. need to manually retry the above error.');
                            }

                            await this._projectionStore.setState(projectionTask.projectionId, 'faulted');
                            await this._projectionStore.setError(projectionTask.projectionTaskId, errorFault, errorEvent, errorOffset);

                            this.emit('playbackError', errorFault);

                            // break if we should stop processing when faulted/with error
                            if (this.options.stopOnFaulted) {
                                break;
                            }
                        }
                    }

                    // set to processed if no errors OR if we are not stopping if there is a fault
                    await this._projectionStore.setProcessed(projectionTask.projectionTaskId, Date.now(), eventsCount > 0 ? this._serializeProjectionOffset(offset) : undefined, !(eventsCount > 0));

                    this.emit('playbackSuccess', {
                        projectionId: projectionTask.projectionId,
                        projectionTaskId: projectionTaskId,
                        eventsCount: eventsCount
                    });
                    debug('ES-SHARD', this.shard, 'DONE RUNNING TASK', projectionTaskId);
                } else {
                    debug('ES-SHARD', this.shard, 'NOT RUNNING TASK', projectionTaskId);
                }
            } else {
                debug('ES-SHARD', this.shard, 'NO TASK', projectionTaskId);
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
    async _getProjectionEvents(eventstore, query, offset, limit) {
        const self = this;
        let events = [];
        
        if (self.options.enableProjectionEventStreamBuffer) {
            let projectionEventStreamBuffer;
            let channel;
            if (Array.isArray(query)) {
                channel = self._getCompositeChannel(query);
            } else {
                channel = self._getChannel(query, query.shard, query.partition);
            }
            const semaphoreKey = `${channel}-${offset}-${limit}`;
    
            if (!self._projectionEventSemaphores[semaphoreKey]) {
                self._projectionEventSemaphores[semaphoreKey] = new Semaphore(semaphoreKey, 1);
            }
    
            // Note: Semaphore logic only if streambuffer is on. With sharding and buffer off, this is uneccessary
            const semaphore = self._projectionEventSemaphores[semaphoreKey];
            const token = await semaphore.acquire();
    
            try {
                debug('ProjectionEventStreamBuffer: Channel:', channel, offset, offset + limit);
                if (!self._projectionEventStreamBuffers[channel]) {
                    self._createProjectionEventStreamBuffer(query, channel);
                }
                projectionEventStreamBuffer = self._projectionEventStreamBuffers[channel];

                if (projectionEventStreamBuffer) {
                    events = projectionEventStreamBuffer.getEventsInBuffer(offset, limit);
    
                    if (events && events.length > 0) {
                        debug('ProjectionEventStreamBuffer hit on channel:', channel, offset, offset + limit);
                    } else {
                        debug('ProjectionEventStreamBuffer miss on channel:', channel, offset, offset + limit);
                        events = await self._getEventsAsync(eventstore, query, offset, limit);
                        if (events && events.length > 0) {
                            projectionEventStreamBuffer.offerEvents(events, offset);
                        }
                    }
                } else {
                    events = await self._getEventsAsync(eventstore, query, offset, limit);
                }
                return events;
            } finally {
                await semaphore.release(token);
            }
        } else {
            events = await self._getEventsAsync(eventstore, query, offset, limit);
            return events;
        }
    };

    /**
     * @returns {void} returns void
     */
    async _waitForQueuesToDrain(queues) {
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
    _getPlaybackFunctions(state) {
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
    _isPromise(obj) {
        return obj && typeof obj.then == 'function';
    }

    /**
     * @param {Event} event the event to process
     * @param {ProjectionConfiguration} projectionConfig the object/interface to use to playback the event
     * @param {Number} timeout callback to notify that the job is done
     * @returns {Promise<void>} returns a Promise that resolves to a void
     */
    _playbackEvent(event, projectionConfig, timeout) {
        return new Promise((resolve, reject) => {
            // TODO: fix timeout with promise based callback. how to have an async await timeout handling?
            // TODO: create common functions as code are being repetitive already
            let timeoutHandle = setTimeout(() => {
                reject(new Error(`timeout in calling the playbackInterface after ${timeout}ms`));
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
    async _saveEventAsync(eventstore, targetQuery, event) {
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
                    if (error.name == 'EventStoreDuplicateError') {
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
    async _saveProjectionState(projectionId, projectionStateId, newState, context) {
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
    async _getProjectionState(projectionConfig, lastProjectionEvent) {
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
    _getProjectionStateStreamId(projectionConfig, lastProjectionEvent) {
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
    _getEventHandlerFunc(projectionConfig, event) {
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
    _getProjectionJobGroup() {
        return `projection-group:${this.options.projectionGroup}`;
    };

    /**
     * @param {Eventstore} eventstore the eventstore to use
     * @param {AggregateQuery} query the query for the aggregate/category that we like to get
     * @returns {Promise<Event>} returns a Promise that resolves to an event 
     */
    async _getLastEventAsync(eventstore, query) {
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
    async _getEventsAsync(eventstore, query, offset, limit) {
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
     * @param {Eventstore} eventstore the eventstore to use
     * @param {AggregateQuery} query the query for the aggregate/category that we like to get
     * @param {Number} offset how many events to skip
     * @param {Number} limit max items to return
     * @returns {Promise<Stream>} returns a Promise that resolves to an array of Events
     */
    async _streamEventsAsync(eventstore, query, offset, limit) {
      return await eventstore.streamEvents(query, offset, limit);
    };

    // NOTE: unused??
    /**
     * @param {AggregateQuery} query the query for the aggregate/category that we like to get
     * @param {Number} revMin Minimum revision boundary
     * @param {Number} revMax Maximum revision boundary
     * @returns {Promise<EventStream>} returns a Promise that resolves to an event 
     */
    async _getEventStreamAsync(query, revMin, revMax) {
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
    async _getLastEventAsStreamAsync(evenstore, query) {
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
    async _getEventStreamBufferedAsync(query, revMin, revMax) {
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
     * @param {Number} shard
     * @param {string} partition
     * @returns {String[]} returns an array of possible topics of the query
     */
    _queryToChannels(query, shard, partition) {
        const channels = [];

        channels.push('all.all.all');
        channels.push(`${query.context}.all.all`);
        channels.push(`${query.context}.${query.aggregate}.all`);
        channels.push(`${query.context}.${query.aggregate}.${query.aggregateId}`);
        if (!_.isNil(shard) && !_.isNil(partition)) {
            channels.push(`all.all.all.${shard}.${partition}`);
            channels.push(`${query.context}.all.all.${shard}.${partition}`);
            channels.push(`${query.context}.${query.aggregate}.all.${shard}.${partition}`);
            channels.push(`${query.context}.${query.aggregate}.${query.aggregateId}.${shard}.${partition}`);
        }

        return channels;
    }

    /**
     * @param {ProjectionConfiguration} projectionConfig parameters for the projection
     * @param {Boolean} shouldUpdateProjectionStore update the store 
     * @returns {Promise<void>} returns a Promise of type void
     */
    async _project(projectionConfig, shouldUpdateProjectionStore) {
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
            const lockKey = `${projectionKey}-shard-${this.shard}`;
            try {
                debug('ES-SHARD', this.shard, 'AWAITING LOCK ON KEY', lockKey);
                lockToken = await this._distributedLock.lock(lockKey, 1900);
                debug('ES-SHARD', this.shard, 'DONE AWAITING LOCK ON KEY', lockKey);
            } catch (error) {
                console.info('ES-SHARD', this.shard, 'was not able to acquire lock. just continue');
            }

            let projectionTasks;
            if (shouldUpdateProjectionStore) {
                // console.log('_project: updating projection store');
                // add projection to the store if not exists
                const storedProjection = await this._projectionStore.getProjection(projection.projectionId);

                if (storedProjection) {
                    await this._projectionStore.updateProjection(projection);
                    debug('ES-SHARD', this.options.shard, 'UPDATED PROJECTION', projectionConfig.projectionId);
                } else {
                    await this._projectionStore.createProjection(projection);
                    debug('ES-SHARD', this.options.shard, 'CREATED PROJECTION', projectionConfig.projectionId);
                }

                const storedProjectionTasks = await this._projectionStore.getProjectionTasks(projection.projectionId, this.shard);

                if (storedProjectionTasks.length === 0) {
                    const createProjectionTaskPromises = [];
                    const partitions = this.store.getPartitions();
                    const hasPartitions = !partitions || !partitions.length ? false : true;
                    const createProjectionTaskFunc = async (partition) => {
                      let projectionTaskOffset = 0;
                      if ((projection && projection.configuration && projection.configuration.fromOffset === 'latest') || !hasPartitions) {
                          projectionTaskOffset = await this._getLatestOffsetAsync(partition);
                      }
                      const newProjectionTask = {
                          projectionTaskId: this._getProjectionTaskId(projection.projectionId, hasPartitions ? partition : null),
                          projectionId: projection.projectionId,
                          shard: this.shard,
                          partition: hasPartitions ? partition : '__unpartitioned',
                          offset: this._serializeProjectionOffset(projectionTaskOffset)
                      };
                      debug('ES-SHARD', this.shard, 'CREATING PROJECTION TASK', newProjectionTask.projectionTaskId);
                      createProjectionTaskPromises.push(this._projectionStore.createProjectionTask(newProjectionTask));
                    }
                    if (!hasPartitions) {
                      await createProjectionTaskFunc(null)
                    } else {
                      for (const partition of partitions) {
                        await createProjectionTaskFunc(partition)
                      }
                    }
                    
                    await Promise.all(createProjectionTaskPromises);
                } else {
                    debug('ES-SHARD', this.options.shard, 'TASKS ALREADY EXISTING FOR PROJECTION', projectionConfig.projectionId);
                    projectionTasks = storedProjectionTasks;
                }
            } else {
                debug('ES-SHARD', this.options.shard, 'shouldUpdateProjectionStore false', projectionConfig.projectionId);
            }

            // set projection task to private list of projection tasks
            if (!projectionTasks) {
                projectionTasks = await this._projectionStore.getProjectionTasks(projection.projectionId, this.shard);
            }
            for (const projectionTask of projectionTasks) {
                this._projectionTasks[projectionTask.projectionTaskId] = projectionTask;
                debug('ES-SHARD', this.options.shard, 'SETTING TASKS IN MEMORY', projectionConfig.projectionId);
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

            debug('ES-SHARD', this.options.shard, '_PROJECT DONE', projectionConfig.projectionId);
        } catch (error) {
            console.error('error in _project with params and error', projectionConfig, error);
            throw error;
        }
    };

    _getProjectionTaskId(projectionId, partition) {
      if (partition) {
          return `${projectionId}:shard${this.shard}:partition${partition}`;
      } else {
          return projectionId;
      }
    }

    _getPartitions() {
        if (!this._partitions) {
            this._partitions = this.store.getPartitions();
        }
        return this._partitions;
    }

    _deserializeProjectionOffset(serializedProjectionOffset) {
        return JSON.parse(Buffer.from(serializedProjectionOffset, 'base64').toString('utf8'));
    }

    _serializeProjectionOffset(projectionOffset) {
        return Buffer.from(JSON.stringify(projectionOffset)).toString('base64');
    }
    

    /**
     * @param {Projection} projection the projection to initialize with a list
     * @returns {Promise<void>} returns a Promise of type void
     */
    async _initStateList(projection) {
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
    async _initPlaybackList(projection) {

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
    async _getProjectionStream(projectionId) {
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
    async _addEventToStream(stream, event) {
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
    async _getLatestOffsetAsync(partition) {
        return new Promise((resolve, reject) => {
            this.store.getLatestOffset(partition, (error, latestOffset) => {
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
    async _commitStream(stream) {
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
    _getHeapPercentage() {
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
    _getCurrentStreamBufferBucket() {
        const now = new Date();
        const newBucket = now.toISOString().substring(0, 13); // ex. 2020-07-28T14
        return newBucket;
    }

    /**
     * Creates an internal ProjectionEventStreamBuffer for the specified Projection query.
     */
    _createProjectionEventStreamBuffer(query, channel) {
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
    _cleanOldestSubscriptionEventStreamBufferBucket() {
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
    _cleanOldestProjectionEventStreamBufferBucket() {
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
}


/***************************************************************************************************
    PUBLIC METHODS 
***************************************************************************************************/


module.exports = EventstoreWithProjection;
