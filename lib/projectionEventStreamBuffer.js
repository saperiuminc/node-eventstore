/**
 * Eventstore
 * @typedef {import('./eventstore')} Eventstore
 */

 /**
 * ProjectionEventStreamBufferOptions
 * @typedef {Object} ProjectionEventStreamBufferOptions
 * @property {Eventstore} es reference to the eventstore of the stream buffer
 * @property {Object} query object that represents the query of the stream buffer
 * @property {String} channel string value of the channel based on the query of the stream buffer
 * @property {String} bucket string value of the bucket where the stream buffer is saved
 * @property {Number} bufferCapacity maximum capacity of the stream buffer
 * @property {Number} ttl time to live of the stream buffer in milliseconds
 * @property {Function} onOfferEvent Optional function that will be triggered when an event is offered
 * @property {Function} onOfferEventError Optional function that will be triggered when an invalid event is offered
 * @property {Function} onInactive Optional function that will be triggered when the stream buffer is considered inactive
 * @property {Function} onClose Optional function that will be triggered when the stream buffer is closed
 */

/**
 * ProjectionEventStreamBuffer constructor
 * @class
 * @param {ProjectionEventStreamBufferOptions} options options for the stream buffer
 * @constructor
 */
function ProjectionEventStreamBuffer(options) {
    this._streamBuffer = [];

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

ProjectionEventStreamBuffer.prototype = {
    /**
     * offer an event to the stream buffer
     * @param {Object} event
     */
    offerEvent: function (event) {
        const self = this;
        const latestEventSequence = self.getLatestEventSequence();
        if (self._streamBuffer.length <= 0 || event.eventSequence > latestEventSequence) {
            // Add the event to the buffer
            self._addEvent(event);
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
     * private method that adds an event to the stream buffer if the buffer is initially empty or if input event has an eventSequence that is higher than the latest eventSequence.
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
            // only add the event if eventSequence is greater than the current latest eventSequence
            // remove the oldest event first if stream buffer is at full bufferCapacity
            if (self._streamBuffer.length === self.bufferCapacity) {
                self._streamBuffer.shift();
            }
            self._streamBuffer.push(event);
            self._updateStreamBuffer();
        }
    },

    /**
     * get the latest eventSequence of the stream buffer
     * @returns {Number} the eventSequence of the latest event in the stream buffer
     */
    getLatestEventSequence: function () {
        const self = this;
        if (!self._streamBuffer || self._streamBuffer.length <= 0) {
            return -1;
        }
        return self._streamBuffer[self._streamBuffer.length - 1].eventSequence;
    },

    /**
     * get the oldest eventSequence of the stream buffer
     * @returns {Number} the eventSequence of the oldest event in the stream buffer
     */
    getOldestEventSequence: function () {
        const self = this;
        if (!self._streamBuffer || self._streamBuffer.length <= 0) {
            return -1;
        }
        return self._streamBuffer[0].eventSequence;
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
     * Get events within the range of the offset and limit inputs
     * @param {Number} offset minimum eventSequence of the event to be fetched
     * @param {Number} limit maximum number of the event to be fetched
     * @returns {Array} array of events that matches the input criteria
     */
    getEventsInBuffer: function (offset, limit) {
        const self = this;
        const offsetEventSequence = offset + 1;
        const oldestEventSequence = self.getOldestEventSequence();
        const latestEventSequence = self.getLatestEventSequence();
        if (self._streamBuffer.length === 0 || offsetEventSequence < oldestEventSequence - 1 || offsetEventSequence >= latestEventSequence) {
            // need to retrieve the events from the ES instead
            return [];
        } else {
            let start = 0;
            while (self._streamBuffer[start] < offsetEventSequence && start < self._streamBuffer.length) {
                start++;
            }
            let end = start + limit;
            if (end > self._streamBuffer.length) {
                end = self._streamBuffer.length;
            }
            return self._streamBuffer.slice(start, end);
        }
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
    },

    clear: function() {
        const self = this;

        self._streamBuffer.length = 0;

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

        if (self.onClose) {
            self.onClose(self.bucket, self.channel);
        }
    }
};

module.exports = ProjectionEventStreamBuffer;