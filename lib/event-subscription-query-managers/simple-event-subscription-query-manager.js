const util = require('util');
const debug = require('debug')('eventstore:projection');
const EventSubscriptionQueryManager = require('./event-subscription-query-manager');

/**
 * SimpleEventSubscriptionQueryManager constructor
 * @class
 * @constructor
 */
function SimpleEventSubscriptionQueryManager(options) {
    EventSubscriptionQueryManager.call(this, options);
}

util.inherits(SimpleEventSubscriptionQueryManager, EventSubscriptionQueryManager);

SimpleEventSubscriptionQueryManager.prototype = {
    /**
     * @param {AggregateQuery} query the query for the aggregate/category that we like to get
     * @param {Number} revMin Minimum revision boundary
     * @param {Number} revMax Maximum revision boundary
     * @param {Number} retryAttempts Current retry attempt
     * @returns {Promise<EventStream>} returns a Promise that resolves to an event, or rejects with an error if the max retry attempts have been reached
     */
    _getEventStreamAsyncWithRetry: async function(query, revMin, revMax, retryAttempts) {
        const self = this;

        // Error Function for handling retry attempts
        retryAttempts = retryAttempts || 0;
        const onError = function(error) {
            if (retryAttempts < self.errorMaxRetryCount) {
                const retryTime = Math.pow(self.errorRetryExponent, retryAttempts) * 1000;
                return new Promise((resolve) => {
                    debug(`_getEventStreamAsyncWithRetry - onError: retrying after ${retryTime/1000} seconds`, self.query);
                    setTimeout(resolve, retryTime);
                }).then(() => {
                    return self._getEventStreamAsyncWithRetry(query, revMin, revMax, retryAttempts + 1);
                });
            } else {
                console.error(`_getEventStreamAsyncWithRetry - onError: exceeded max retries: ${retryAttempts}. Giving up . . .`, self.query);
                console.error(error);
                return Promise.reject(error);
            }
        };

        try {
            return await self._getEventStreamAsync(query, revMin, revMax);
        } catch (error) {
            return onError(error);
        }
    },

    /**
     * @param {AggregateQuery} query the query for the aggregate/category that we like to get
     * @param {Number} revMin Minimum revision boundary
     * @param {Number} revMax Maximum revision boundary
     * @returns {Promise<EventStream>} returns a Promise that resolves to an event 
     */
    _getEventStreamAsync: async function(query, revMin, revMax) {
        const self = this;
        return new Promise((resolve, reject) => {
            try {
                self.es.getEventStream(query, revMin, revMax, (err, stream) => {
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
    },

     /**
     * @param {AggregateQuery} query the query for the aggregate/category that we like to get
     * @returns {Promise<Event>} returns a Promise that resolves to an event 
     */
    _getLastEventAsync: async function(query) {
        const self = this;
        return new Promise((resolve, reject) => {
            try {
                self.es.getLastEvent(query, (err, event) => {
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
    },

    /**
     * @param {AggregateQuery} query the query for the aggregate/category that we like to get
     * @param {Number} revMin Minimum revision boundary
     * @param {Number} revMax Maximum revision boundary
     * @param {Number} retryAttempts Current retry attempt
     * @returns {Promise<EventStream>} returns a Promise that resolves to an event, or rejects with an error if the max retry attempts have been reached
     */
    getEventStream: async function(query, revMin, revMax) {
        return await this._getEventStreamAsyncWithRetry(query, revMin, revMax);
    },

    /**
     * @param {AggregateQuery} query the query for the aggregate/category that we like to get
     * @returns {Promise<Event>} returns a Promise that resolves to an event 
     */
    getLastEvent: async function(query) {
        return await this._getLastEventAsync(query);
    },

    clear: async function() {
        // do nothing
        return;
    },

    setChannelActive: function(channel, isActive) {
        if (isActive) {
            this._activeChannels[channel] = true;
        } else {
            delete this._activeChannels[channel];
        }
    },

    isChannelActive: function(channel) {
        return this._activeChannels[channel];
    }
}

module.exports = SimpleEventSubscriptionQueryManager;