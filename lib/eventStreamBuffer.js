const _ = require('lodash');
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
 * @typedef {Object} EventStreamBufferOptions
 * @property {Eventstore} es reference to the eventstore of the stream buffer
 * @property {Object} query object that represents the query of the stream buffer
 * @property {String} channel string value of the channel based on the query of the stream buffer
 * @property {String} bucket string value of the bucket where the stream buffer is saved
 * @property {Number} capacity maximum capacity of the stream buffer
 * @property {Number} ttl time to live of the stream buffer in milliseconds
 * @property {Function} onOfferEvent Optional function that will be triggered when an offent is offered
 * @property {Function} onCleanUp Optional function that will be triggered when the stream buffer is cleaned up
 */

/**
 * EventStreamBuffer constructor
 * @class
 * @param {EventStreamBufferOptions} options options for the stream buffer
 * @constructor
 */
function EventStreamBuffer(options) {
    this.streamBuffer = [];

    // TODO: Set a capacity on PriorityQueue, then implement a reset buffer by getting the biggest and latest set from the pool if the capacity is reached
    this.offeredEventsPool = new PriorityQueue(
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
    this.capacity = options.capacity
    this.ttl = options.ttl;
    
    this.onOfferEvent = options.onOfferEvent; // Checks memory utilization if need to trigger clean up
    this.onCleanUp = options.onCleanUp; // Delete Stream Buffer in EventStoreProjection

    this.createdAt = new Date().getTime();
    this._updateStreamBuffer(this.createdAt);
}

EventStreamBuffer.prototype = {
    /**
     * adds an event to the stream buffer if the buffer is initially empty or if input event has a revision that is 1 higher than the latest revision.
     * if the buffer is full, then the oldest event will be removed before adding the new event.
     * @param {Object} event
     */
    addEvent: function (event) {
        const self = this;

        // add the event automatically if stream buffer is empty
        if (self.streamBuffer.length <= 0) {
            self.streamBuffer.push(event);
            self._updateStreamBuffer();
            console.log('STREAM BUFFER: ADDING EVENT', event);
        } else {
            const latestRevision = self.getLatestRevision();
            console.log('STREAM BUFFER: LATEST REV', latestRevision, self.streamBuffer.length);
            // only add the event if revision is 1 greater than the current latest revision
            if (event.streamRevision === latestRevision + 1) {
                // remove the oldest event first if stream buffer is at full capacity
                if (self.streamBuffer.length === self.capacity) {
                    self.streamBuffer.shift();
                }
                self.streamBuffer.push(event);
                self._updateStreamBuffer();
                console.log('STREAM BUFFER: ADDING EVENT', event, latestRevision);
            } else {
                console.log('STREAM BUFFER: NOT ADDING EVENT', event, latestRevision);
            }
        }
    },

    /**
     * adds multiple events to the stream buffer by utilizing the addEvent function
     * @param {Array} events
     */
    addEvents: function (events) {
        const self = this;
        _.forEach(events, (event) => {
            self.addEvent(event);
        });
    },

    /**
     * offer an event to the offeredEventsPool priority queue
     * @param {Object} event
     */
    offerEvent: function (event) {
        const self = this;
        const latestRevision = self.getLatestRevision();
        if (self.streamBuffer.length <= 0 || event.streamRevision === latestRevision + 1) {
            // Add the event straight to the buffer, without going through the pool
            self.addEvent(event);
            // Attempt to add other events from the pool
            self._addEventsFromPool();
        } else if (event.streamRevision > latestRevision) {
            // Add the candidate event to the pool
            self.offeredEventsPool.queue(event);
            // Attempt to add events from the pool
            self._addEventsFromPool();
        } else {
            // Event is invalid, since the buffer has a newer event
            console.log('STREAM BUFFER: Got an older event:', event)
        }

        if (self.onOfferEvent) {
            self.onOfferEvent(self.bucket, self.channel);
        }
    },

    /**
     * offers multiple events to the offeredEventsPool priority queue by utilizing the offerEvent function
     * @param {Array} events
     */
    offerEvents: function (events) {
        const self = this;
        _.forEach(events, (event) => {
            self.offerEvent(event);
        });
    },

    /**
     * private method to add events from the offeredEventsPool priority queue
     */
    _addEventsFromPool: function () {
        const self = this;

        if (self.offeredEventsPool.length > 0) {
            if (self.streamBuffer.length <= 0 || (self.offeredEventsPool.peek().streamRevision === self.getLatestRevision() + 1)) {
                // add the event to the buffer
                self.addEvent(self.offeredEventsPool.dequeue());

                // try to add more events
                self._addEventsFromPool();
            }
        }
    },

    /**
     * get the latest revision of the stream buffer
     * @returns {Number} the revision of the latest event in the stream buffer
     */
    getLatestRevision: function () {
        const self = this;
        if (!self.streamBuffer || self.streamBuffer.length <= 0) {
            return -1;
        }
        return self.streamBuffer[self.streamBuffer.length - 1].streamRevision;
    },

    /**
     * get the oldest revision of the stream buffer
     * @returns {Number} the revision of the oldest event in the stream buffer
     */
    getOldestRevision: function () {
        const self = this;
        if (!self.streamBuffer || self.streamBuffer.length <= 0) {
            return -1;
        }
        return self.streamBuffer[0].streamRevision;
    },

    /**
     * get all events existing in the stream buffer
     * @returns {Array} array of events
     */
    getAllEventsInBuffer: function () {
        const self = this;
        if (!self.streamBuffer || self.streamBuffer.length <= 0) {
            return [];
        }
        return self.streamBuffer[0, self.streamBuffer.length - 1];
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
        if (self.streamBuffer.length === 0 || revMin < oldestRevision || revMin > latestRevision) {
            // need to retrieve the events from the ES instead
            return [];
        } else {
            const start = revMin - oldestRevision;
            let end = revMax - oldestRevision;
            if (end >= self.streamBuffer.length) {
                end = self.streamBuffer.length;
            }
            console.log(`REV MIN MAX ${revMin} ${revMax} ${start} ${end} ${oldestRevision} ${latestRevision}`);
            return self.streamBuffer.slice(start, end);
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

        if (self.cleanUpTimeout) {
            clearTimeout(self.cleanUpTimeout);
        }

        self.cleanUpTimeout = setTimeout(function () {
            self.cleanUp();
        }, self.ttl);
    },

    /**
     * Clears the cleanUpTimeout if still existing and calls the onCleanUp function to delete the stream buffer 
     */
    cleanUp: function () {
        const self = this;

        if (self.cleanUpTimeout) {
            clearTimeout(self.cleanUpTimeout);
        }

        if (self.onCleanUp) {
            self.onCleanUp(self.bucket, self.channel);
        }
    }

};

module.exports = EventStreamBuffer;