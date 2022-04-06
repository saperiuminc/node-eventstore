const util = require('util');
const v8 = require('v8');
const async = require('async');
const debug = require('debug')('eventstore:projection');
const _ = require('lodash');
const EventSubscriptionQueryManager = require('./event-subscription-query-manager');
const SubscriptionEventStreamBuffer = require('../subscriptionEventStreamBuffer');

// TODO: offset + 1
const STREAM_BUFFER_MEMORY_THRESHOLD = process.env.STREAM_BUFFER_MEMORY_THRESHOLD || 0.8; // Defaults to 80%
const STREAM_BUFFER_MAX_CAPACITY = process.env.STREAM_BUFFER_MAX_CAPACITY || 10;
const STREAM_BUFFER_POOL_MAX_CAPACITY = process.env.STREAM_BUFFER_POOL_MAX_CAPACITY || 5;
const STREAM_BUFFFER_TTL = process.env.STREAM_BUFFFER_TTL || (1000 * 60 * 60 * 24); // Default is 1 day

/**
 * EventSubscriptionQueryManager constructor
 * @class
 * @constructor
 */
function StreamBufferedEventSubscriptionQueryManager(options) {
    const self = this;
    EventSubscriptionQueryManager.call(this, options);
    self.eventSubscriptionQueryManager = options.eventSubscriptionQueryManager;

    // Initialize Subscription Stream Buffers
    self._subscriptionEventStreamBuffers = {};
    self._subscriptionEventStreamBufferBuckets = {};
    self._subscriptionEventStreamBufferLRULocked = false;

    self._subscriptionEventStreamBufferLRUCleaner = async.queue(function(task, callback) {
        const channel = task.channel;

        if (self.isChannelActive(channel)) {
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

util.inherits(StreamBufferedEventSubscriptionQueryManager, EventSubscriptionQueryManager);

_.extend(StreamBufferedEventSubscriptionQueryManager.prototype, {
    /**
     * Gets the heap percentage from the heap statistics of the node app
     * @returns {Number} returns the current heap percentage value (0-1) of the node app
     */
    _getHeapPercentage: function() {
        const heapStatistics = v8.getHeapStatistics();
        const heapUsed = heapStatistics.used_heap_size;
        const heapTotal = heapStatistics.heap_size_limit;
        const heapPercentage = heapUsed / heapTotal;

        return heapPercentage;
    },

    /**
     * @param {AggregateQuery} query 
     * @param {Number} shard
     * @param {string} partition
     * @returns {String} - returns a dot delimited channel
     */
    _getChannel(query, shard, partition) {
        if (_.isNumber(parseInt(shard)) && (_.isNumber(partition) || partition === '__unpartitioned')) {
            return `${query.context || 'all'}.${query.aggregate || 'all'}.${query.aggregateId || 'all'}.${shard}.${partition}`;
        } else {
            return `${query.context || 'all'}.${query.aggregate || 'all'}.${query.aggregateId || 'all'}`;
        }
    },

    /**
     * Cleans up the oldest bucket from subscription event stream buffer bucket and locks the LRU execution to prevent getting executed multiple times
     */
    _cleanOldestSubscriptionEventStreamBufferBucket() {
        const self = this;
        self._subscriptionEventStreamBufferLRULocked = true;
        debug('locking SubscriptionEventStreamBuffer LRU');

        const buckets = Object.keys(self._subscriptionEventStreamBufferBuckets);
        if (buckets && buckets.length > 0) {
            const sortedBuckets = _.sortBy(buckets, (i) => i);
            const bucketToClean = sortedBuckets[0];
            const channels = Object.keys(self._subscriptionEventStreamBufferBuckets[bucketToClean]);

            debug('cleaning SubscriptionEventStreamBuffer bucket');
            debug(bucketToClean);

            if (channels && channels.length > 0) {
                channels.forEach((channel) => {
                    self._subscriptionEventStreamBufferLRUCleaner.push({
                        channel
                    });
                });
            }

            delete self._subscriptionEventStreamBufferBuckets[bucketToClean];
        }
    },

    /**
     * Gets the current stream buffer bucket based on the current time.
     * @returns {String} returns the name of the bucket which is represented by the ISO datetime upto the hour
     */
    _getCurrentStreamBufferBucket() {
        const now = new Date();
        const newBucket = now.toISOString().substring(0, 13); // ex. 2020-07-28T14
        return newBucket;
    },

    /**
     * @param {AggregateQuery} query the query for the aggregate/category that we like to get
     * @param {Number} revMin Minimum revision boundary
     * @param {Number} revMax Maximum revision boundary
     * @param {Number} retryAttempts Current retry attempt
     * @returns {Promise<EventStream>} returns a Promise that resolves to an event, or rejects with an error if the max retry attempts have been reached
     */
    getEventStream: async function(query, revMin, revMax) {
        const self = this;
        const channel = self._getChannel(query);
        let streamBuffer = self._subscriptionEventStreamBuffers[channel];
        if (!streamBuffer) {
            const bucket = self._getCurrentStreamBufferBucket();

            const options = {
                es: self.es,
                query: query,
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
                    if (self.isChannelActive(channel)) {
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
            streamBuffer = self._subscriptionEventStreamBuffers[channel];

            if (!self._subscriptionEventStreamBufferBuckets[bucket]) {
                self._subscriptionEventStreamBufferBuckets[bucket] = {};
            }

            self._subscriptionEventStreamBufferBuckets[bucket][channel] = new Date().getTime();
        }

        let stream = streamBuffer.getEventsInBufferAsStream(revMin, revMax);
        if (stream && stream.events.length > 0) {
            return stream;
        } else {
            stream = await self.eventSubscriptionQueryManager.getEventStream(query, revMin, revMax);
            if (stream && stream.events.length > 0) {
                streamBuffer.offerEvents(stream.events);
            }
            return stream;
        }
    },

    /**
     * @param {AggregateQuery} query the query for the aggregate/category that we like to get
     * @returns {Promise<Event>} returns a Promise that resolves to an event 
     */
    getLastEvent: async function(query) {
        return await this.eventSubscriptionQueryManager.getLastEvent(query);
    },

    /**
     * Closes all Subscription Event StreamBuffers
     * @returns {void} - returns void
     */
    clear: async function() {
        const self = this;
        await self.eventSubscriptionQueryManager.clear();

        self._subscriptionEventStreamBufferLRULocked = true;
        const channels = Object.keys(self._subscriptionEventStreamBuffers);

        for (let i = 0; i < channels.length; i++) {
            const channel = channels[i];
            self._subscriptionEventStreamBuffers[channel].close();
        }
        self._subscriptionEventStreamBuffers = {};
        self._subscriptionEventStreamBufferBuckets = {};
        self._subscriptionEventStreamBufferLRULocked = false;
        return;
    },

    setChannelActive: function(channel, isActive) {
        return this.eventSubscriptionQueryManager.setChannelActive(channel, isActive);
    },

    isChannelActive: function(channel) {
        return this.eventSubscriptionQueryManager.isChannelActive(channel);
    },
});

module.exports = StreamBufferedEventSubscriptionQueryManager;