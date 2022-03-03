/* eslint-disable require-jsdoc */
const util = require('util');
const PartitionedStore = require('../../partitioned-store');
const _ = require('lodash');
const v8 = require('v8');
const debug = require('debug')('eventstore:clustered:streambuffer');
const async = require('async');
const ProjectionEventStreamBuffer = require('../../../projectionEventStreamBuffer');
const Semaphore = require('../../../eventstore-projections/semaphore');

function StreamBufferedPartitionedStore(options, offsetManager, partitionedStore) {
    const self = this;

    let defaults = {
        pollingMaxRevisions: 100,
        streamBufferMemoryThreshold: 0.8,                       // Defaults to 80%
        streamBufferTTL: (1000 * 60 * 60 * 24)                  // Default is 1 day
    };

    self._options = Object.assign(defaults, options);
    self._partitionedStore = partitionedStore;

    if (!self._partitionedStore) {
        throw new Error('missing or invalid mysqlStore');
    }

    self._partitionedStore.on('connect', function () {
        self.emit('connect');
    });

    self._partitionedStore.on('disconnect', function () {
        self.emit('disconnect');
    });

    self._projectionEventSemaphores = {};
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

        if (heapPercentage >= self._options.streamBufferMemoryThreshold) {
            self._cleanOldestProjectionEventStreamBufferBucket();
        } else {
            self._projectionEventStreamBufferLRULocked = false;
        }
    };

    PartitionedStore.call(this, options, offsetManager);
}

util.inherits(StreamBufferedPartitionedStore, PartitionedStore);

_.extend(StreamBufferedPartitionedStore.prototype, {
    connect: function(callback) {
        this._partitionedStore.connect(callback);
    },

    disconnect: function(callback) {
        this._partitionedStore.disconnect(callback);
    },

    getPartitionId: function(aggregateId) {
        return this._partitionedStore.getPartitionId(aggregateId);
    },

    getPartitions: function(type) {
        return this._partitionedStore.getPartitions(type);
    },

    getLatestOffset: function(shard, partition, callback) {
        this._partitionedStore.getLatestOffset(shard, partition, callback);
    },

    addEvents: function(events, callback) {
        this._partitionedStore.addEvents(events, callback);
    },

    getEvents: function(query, skip, limit, callback) {
        this._getBufferedEvents(query, skip, limit).then(data => {
            callback(null, data);
        }).catch(callback);
    },

    _getBufferedEvents: async function(query, skip, limit) {
        const self = this;
        let events = [];

        let projectionEventStreamBuffer;
        let channel;
        if (Array.isArray(query)) {
            channel = self._getCompositeChannel(query);
        } else {
            channel = self._getChannel(query);
        }
        const semaphoreKey = `${channel}-${skip}-${limit}`;

        if (!self._projectionEventSemaphores[semaphoreKey]) {
            self._projectionEventSemaphores[semaphoreKey] = new Semaphore(semaphoreKey, 1);
        }

        const semaphore = self._projectionEventSemaphores[semaphoreKey];
        const token = await semaphore.acquire();

        try {
            debug('ProjectionEventStreamBuffer: Channel:', channel, skip, limit);
            if (!self._projectionEventStreamBuffers[channel]) {
                self._createProjectionEventStreamBuffer(query, channel);
            }
            projectionEventStreamBuffer = self._projectionEventStreamBuffers[channel];

            if (projectionEventStreamBuffer) {
                events = projectionEventStreamBuffer.getEventsInBuffer(skip, limit);

                if (events && events.length > 0) {
                    debug('ProjectionEventStreamBuffer hit on channel:', channel, skip, limit);
                } else {
                    debug('ProjectionEventStreamBuffer miss on channel:', channel, skip, limit);
                    events = await self._getEvents(query, skip, limit);
                    if (events && events.length > 0) {
                        projectionEventStreamBuffer.offerEvents(events, skip);
                    }
                }
            } else {
                events = await self._getEvents(query, skip, limit);
            }
            return events;
        } finally {
            await semaphore.release(token);
        }
    },

    _getEvents: async function (query, skip, limit) {
        return new Promise((resolve, reject) => {
            this._partitionedStore.getEvents(query, skip, limit, function(err, data) {
                if (err) reject(err);
                resolve(data);
            });
        });
    },

    getEventsSince: function(date, skip, limit, callback) {
        this._partitionedStore.getEventsSince(date, skip, limit, callback);
    },

    getEventsByRevision: function(query, revMin, revMax, callback) {
        this._partitionedStore.getEventsByRevision(query, revMin, revMax, callback);
    },

    getLastEvent: function(query, callback) {
        this._partitionedStore.getLastEvent(query, callback);
    },

    getUndispatchedEvents: function(query, callback) {
        this._partitionedStore.getUndispatchedEvents(query, callback);
    },

    setEventToDispatched: function(id, callback) {
        this._partitionedStore.setEventToDispatched(id, callback);
    },

    addSnapshot: function(snapshot, callback) {
        this._partitionedStore.addSnapshot(snapshot, callback);
    },

    getSnapshot: function(query, revMax, callback) {
        this._partitionedStore.getSnapshot(query, revMax, callback);
    },

    cleanSnapshots: function(query, callback) {
        this._partitionedStore.cleanSnapshots(query, callback);
    },

    clear: function(done) {
        this._partitionedStore.clear(done);
    },

    /**
     * @param {AggregateQuery} query 
     * @param {Number} shard
     * @param {string} partition
     * @returns {String} - returns a dot delimited channel
     */
    _getChannel: function(query) {
        return `${query.context || 'all'}.${query.aggregate || 'all'}.${query.aggregateId || 'all'}`;
    },

    /**
     * @param {AggregateQuery[]} queryArray 
     * @returns {String} - returns a dot delimited channel
     */
    _getCompositeChannel: function(queryArray) {
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
            channel += this._getChannel(query);
        }

        return channel;
    },

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
     * Gets the current stream buffer bucket based on the current time.
     * @returns {String} returns the name of the bucket which is represented by the ISO datetime upto the hour
     */
    _getCurrentStreamBufferBucket: function() {
        const now = new Date();
        const newBucket = now.toISOString().substring(0, 13); // ex. 2020-07-28T14
        return newBucket;
    },

    /**
     * Creates an internal ProjectionEventStreamBuffer for the specified Projection query.
     */
    _createProjectionEventStreamBuffer: function(query, channel) {
        const self = this;

        // initialize projection event stream buffer
        const bucket = self._getCurrentStreamBufferBucket();

        const options = {
            offsetManager: self.getOffsetManager(),
            query: query,
            channel: channel,
            bucket: bucket,
            bufferCapacity: self._options.pollingMaxRevisions,
            ttl: self._options.streamBufferTTL,
            onOfferEvent: function(currentBucket, channel) {
                if (!self._projectionEventStreamBufferLRULocked) {
                    const heapPercentage = self._getHeapPercentage();
                    debug('Projection Event Stream Buffer - HEAP %', heapPercentage);
                    if (heapPercentage >= self._options.streamBufferMemoryThreshold) {
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
    },

    /**
     * Cleans up the oldest bucket from projection event stream buffer bucket and locks the LRU execution to prevent getting executed multiple times
     */
    _cleanOldestProjectionEventStreamBufferBucket() {
        const self = this;

        self._projectionEventStreamBufferLRULocked = true;
        debug('locking ProjectionEventStreamBuffer LRU');

        const buckets = Object.keys(self._projectionEventStreamBufferBuckets);
        if (buckets && buckets.length > 0) {
            const sortedBuckets = _.sortBy(buckets, (i) => i);
            const bucketToClean = sortedBuckets[0];
            const channels = Object.keys(self._projectionEventStreamBufferBuckets[bucketToClean]);

            debug('cleaning ProjectionEventStreamBuffer bucket');
            debug(bucketToClean);

            if (channels && channels.length > 0) {
                channels.forEach((channel) => {
                    self._projectionEventStreamBufferLRUCleaner.push({
                        channel
                    });
                });
            }

            delete self._projectionEventStreamBufferBuckets[bucketToClean];
        }
    }
});

module.exports = StreamBufferedPartitionedStore;
