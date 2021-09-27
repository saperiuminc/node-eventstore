const EventStream = require('./eventStream');
const PriorityQueue = require('js-priority-queue');

/**
 * Eventstore
 * @typedef {import('./eventstore')} Eventstore
 */

/**
 * EventStream
 * @typedef {import('./eventStream')} EventStream
 */

 /**
 * EventStreamBufferOptions
 * @typedef {Object} SubscriptionEventStreamBufferOptions
 * @property {Eventstore} es reference to the eventstore of the stream buffer
 * @property {Object} query object that represents the query of the stream buffer
 * @property {String} channel string value of the channel based on the query of the stream buffer
 * @property {String} bucket string value of the bucket where the stream buffer is saved
 * @property {Number} bufferCapacity maximum capacity of the stream buffer
 * @property {Number} poolCapacity maximum capacity of the offered events pool
 * @property {Number} ttl time to live of the stream buffer in milliseconds
 * @property {Function} onOfferEvent Optional function that will be triggered when an event is offered
 * @property {Function} onOfferEventError Optional function that will be triggered when an invalid event is offered
 * @property {Function} onInactive Optional function that will be triggered when the stream buffer is considered inactive
 * @property {Function} onClose Optional function that will be triggered when the stream buffer is closed
 */

/**
 * EventStreamBuffer constructor
 * @class
 * @param {SubscriptionEventStreamBufferOptions} options options for the stream buffer
 * @constructor
 */
function SubscriptionEventStreamBuffer(options) {
    this._streamBuffer = [];

    this._offeredEventsPool = new PriorityQueue(
        {
            comparator: function (firstEvent, secondEvent) {
                return firstEvent.streamRevision - secondEvent.streamRevision;
            }
        }
    );

    this.es = options.es;
    this.query = options.query;
    this.channel = options.channel;
    this.bucket = options.bucket;
    this.bufferCapacity = options.bufferCapacity;
    this.poolCapacity = options.poolCapacity
    this.ttl = options.ttl;
    
    this.onOfferEvent = options.onOfferEvent; // Checks memory utilization if need to trigger clean up
    this.onOfferEventError = options.onOfferEventError; // Triggers if an event cannot be added to the buffer and the pool
    this.onInactive = options.onInactive; // Triggers if the timer for the time to live is reached
    this.onClose = options.onClose; // Delete Stream Buffer in EventStoreProjection

    this.createdAt = new Date().getTime();
    this._updateStreamBuffer(this.createdAt);
}

SubscriptionEventStreamBuffer.prototype = {
    /**
     * offer an event to the offeredEventsPool priority queue
     * @param {Object} event
     */
    offerEvent: function (event) {
        const self = this;
        const latestRevision = self.getLatestRevision();
        if (self._streamBuffer.length <= 0 || event.streamRevision === latestRevision + 1) {
            // Add the event straight to the buffer, without going through the pool
            self._addEvent(event);
            // Attempt to add other events from the pool
            self._addEventsFromPool();
            if (self.onOfferEvent) {
                self.onOfferEvent(self.bucket, self.channel);
            }
        } else if (event.streamRevision > latestRevision) {
            // Add the candidate event to the pool
            self._offeredEventsPool.queue(event);

            // If pool is at max capacity, then reset the buffer and populate the latest events from the pool
            if (self._offeredEventsPool.length === self.poolCapacity) {
                self._resetBufferWithLatestEventsFromPool();
            }
            if (self.onOfferEvent) {
                self.onOfferEvent(self.bucket, self.channel);
            }
        } else {
            // Event is invalid, since the buffer has a newer event
            if (self.onOfferEventError) {
                self.onOfferEventError(self.bucket, self.channel);
            }
        }
    },

    offerEvents: function (events) {
        const self = this;
        if (events) {
            events.forEach((event) => {
                self.offerEvent(event);
            });
        }
    },

    /**
     * private method that adds an event to the stream buffer if the buffer is initially empty or if input event has a revision that is 1 higher than the latest revision.
     * if the buffer is full, then the oldest event will be removed before adding the new event.
     * @param {Object} event
     */
    _addEvent: function (event) {
        const self = this;

        // add the event automatically if stream buffer is empty
        if (self._streamBuffer.length <= 0) {
            self._streamBuffer.push(event);
            self._updateStreamBuffer();
        } else {
            // only add the event if revision is 1 greater than the current latest revision
            // remove the oldest event first if stream buffer is at full bufferCapacity
            if (self._streamBuffer.length === self.bufferCapacity) {
                self._streamBuffer.shift();
            }
            self._streamBuffer.push(event);
            self._updateStreamBuffer();
        }
    },

    /**
     * private method to add events from the offeredEventsPool priority queue
     */
    _addEventsFromPool: function () {
        const self = this;

        if (self._offeredEventsPool.length > 0) {
            if (self._streamBuffer.length <= 0 || (self._offeredEventsPool.peek().streamRevision === self.getLatestRevision() + 1)) {
                // add the event to the buffer
                self._addEvent(self._offeredEventsPool.dequeue());

                // try to add more events
                self._addEventsFromPool();
            }
        }
    },

    /**
     * private method to reset the stream buffer, and add the latest contiguous events from the pool
     */
    _resetBufferWithLatestEventsFromPool: function() {
        const self = this;

        // Reset the stream buffer
        self._streamBuffer.length = 0;

        // Get the latest contiguous events from the pool
        const poolEvents = [];
        while (self._offeredEventsPool.length > 0) {
            const offeredEvent = self._offeredEventsPool.dequeue();

            if (poolEvents.length === 0 || poolEvents[poolEvents.length - 1].streamRevision === (offeredEvent.streamRevision - 1)) {
                poolEvents.push(offeredEvent);
            } else {
                poolEvents.length = 0;
                poolEvents.push(offeredEvent);
            }
        }

        // Add the events to the stream buffer
        self._streamBuffer = self._streamBuffer.concat(poolEvents);
        self._updateStreamBuffer();
    },

    /**
     * get the latest revision of the stream buffer
     * @returns {Number} the revision of the latest event in the stream buffer
     */
    getLatestRevision: function () {
        const self = this;
        if (!self._streamBuffer || self._streamBuffer.length <= 0) {
            return -1;
        }
        return self._streamBuffer[self._streamBuffer.length - 1].streamRevision;
    },

    /**
     * get the oldest revision of the stream buffer
     * @returns {Number} the revision of the oldest event in the stream buffer
     */
    getOldestRevision: function () {
        const self = this;
        if (!self._streamBuffer || self._streamBuffer.length <= 0) {
            return -1;
        }
        return self._streamBuffer[0].streamRevision;
    },

    /**
     * get all events existing in the stream buffer
     * @returns {Array} array of events
     */
    getAllEventsInBuffer: function () {
        const self = this;
        if (!self._streamBuffer || self._streamBuffer.length <= 0) {
            return [];
        }
        return self._streamBuffer;
    },

    /**
     * Get events within the range of the revMin and revMax inputs
     * @param {Number} revMin minimum revision of the event to be fetched
     * @param {Number} revMax maximum revision of the event to be fetched
     * @returns {Array} array of events that matches the revisions input
     */
    getEventsInBuffer: function (revMin, revMax) {
        const self = this;
        const oldestRevision = self.getOldestRevision();
        const latestRevision = self.getLatestRevision();
        if (self._streamBuffer.length === 0 || revMin < oldestRevision || revMin > latestRevision) {
            // need to retrieve the events from the ES instead
            return [];
        } else {
            const start = revMin - oldestRevision;
            let end = revMax - oldestRevision;
            if (end >= self._streamBuffer.length) {
                end = self._streamBuffer.length;
            }
            return self._streamBuffer.slice(start, end);
        }
    },

    /**
     * Get events within the range of the revMin and revMax inputs and returns it as an EventStream
     * @param {Number} revMin minimum revision of the event to be fetched
     * @param {Number} revMax maximum revision of the event to be fetched
     * @returns {EventStream} EventStream of events that matches the revisions input
     */
    getEventsInBufferAsStream: function (revMin, revMax) {
        const self = this;
        const events = self.getEventsInBuffer(revMin, revMax);
        return new EventStream(self.es, self.query, events);
    },

    /**
     * Updates the updateAt property and refreshes clean up timer of the stream buffer
     * @param {Number} updatedAt Optional date time value in milliseconds
     */
    _updateStreamBuffer: function (updatedAt) {
        const self = this;
        self.updatedAt = updatedAt || new Date().getTime();

        if (self.inactiveTimeout) {
            clearTimeout(self.inactiveTimeout);
        }

        self.inactiveTimeout = setTimeout(function () {
            self.onInactive(self.bucket, self.channel);
        }, self.ttl);
        // self.inactiveTimeout = setTimeout(function () {
        //     self.close();
        // }, self.ttl);
    },

    clear: function() {
        const self = this;

        self._streamBuffer.length = 0;
        self._offeredEventsPool.clear();

        self._updateStreamBuffer();
    },

    /**
     * Closes the stream buffer. All resources used are cleared can no longer be used. This subsequently calls the onCleanUp function to delete the stream buffer 
     */
    close: function () {
        const self = this;

        if (self.inactiveTimeout) {
            clearTimeout(self.inactiveTimeout);
        }

        self._streamBuffer.length = 0;
        self._streamBuffer = null;
        self._offeredEventsPool.clear();
        self._offeredEventsPool = null;

        if (self.onClose) {
            self.onClose(self.bucket, self.channel);
        }
    }
};

module.exports = SubscriptionEventStreamBuffer;