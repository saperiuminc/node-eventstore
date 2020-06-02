const Eventstore = require('../eventstore');
const _ = require('lodash');
const util = require('util');
const shortid = require('shortid');

/**
 * EventstoreProjectionConfig
 * @typedef {Object} EventstoreProjectionConfig
 */

/**
 * EventStreamSubscription
 * @typedef {Object} EventStreamSubscription
 * @property {String} token Unique token for the subscription
 * @property {Function} cancel Unique token for the subscription
 */

/**
 * Event
 * @typedef {Object} Event
 * @property {Number} streamRevision The revision of the event
 */

/**
 * EventStream
 * @typedef {Object} EventStream
 * @property {Event[]} events The events of this stream
 */

/**
 * EventCallback
 * @typedef {Object} EventCallback
 * @property {Error} error Error if there is any. null if no error happens
 * @property {Event} event The new event for the subscription
 */

/**
 * SubscribeCallback
 * @typedef {Object} SubscribeCallback
 * @property {Error} error Error if there is any. null if no error happens
 * @property {string} subscriptionToken The subscription token
 */

/**
 * ProjectionParams
 * @typedef {Object} ProjectionParams
 * @property {String} projectionId The unique projectionId of the projection
 * @property {AggregateQuery} query The query to use to get the events for the projection
 * @property {Object} userData Optional userData
 * @property {("instance"|"")} partitionBy Partition the state by instance
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
 * @class Eventstore
 * @returns {Eventstore}
 */
function EventstoreWithProjection(options, store) {

    if (options) {
        options.pollingMaxRevisions = options.pollingMaxRevisions || 5;
        options.pollingTimeout = !isNaN(parseInt(options.pollingTimeout)) ? parseInt(options.pollingTimeout) : 1000;
        options.eventCallbackTimeout = !isNaN(parseInt(options.eventCallbackTimeout)) ? parseInt(options.eventCallbackTimeout) : 10000;
        if (options.projectionConfig) {
            this.projectionName = options.projectionConfig.name;
        }
    }

    this.pollingActive = true;

    this._subscriptions = {};

    Eventstore.call(this, options, store);
}

util.inherits(EventstoreWithProjection, Eventstore);


EventstoreWithProjection.prototype = Eventstore.prototype;
EventstoreWithProjection.prototype.constructor = EventstoreWithProjection;

/**
 * @param {AggregateQuery} query the query for the aggregate/category that we like to get
 * @param {Number} offset the position/offset from where we should start the subscription
 * @param {SubscribeCallback} callback the success or callback function
 * @returns {void} - returns void
 */
EventstoreWithProjection.prototype.deactivatePolling = function(callback) {
    try {
        this.pollingActive = false;
    } catch (error) {
        console.error(error);
        if (callback) {
            callback(error);
        }
    }
};

/**
 * @param {AggregateQuery} query the query for the aggregate/category that we like to get
 * @param {Number} offset the position/offset from where we should start the subscription
 * @param {SubscribeCallback} callback the success or callback function
 * @returns {void} - returns void
 */
EventstoreWithProjection.prototype.activatePolling = function(callback) {
    try {
        this.pollingActive = true;
    } catch (error) {
        console.error(error);
        if (callback) {
            callback(error);
        }
    }
};

/**
 * @param {ProjectionParams} projectionParams parameters for the projection
 * @param {Function} callback success/error callback
 * @returns {void} - returns void
 */
EventstoreWithProjection.prototype.project = function(projectionParams, callback) {
    try {
        // just preserving that adrai's public functions are callback based but internally we can just use async/await for elegance
        this._project(projectionParams).then(callback).catch(function(error) {
            if (callback) {
                callback(error);
            }
        });
    } catch (error) {
        if (callback) {
            callback(error);
        }
    }
};

/**
 * @param {AggregateQuery} query the query for the aggregate/category that we like to get
 * @param {Number} offset the position/offset from where we should start the subscription
 * @param {EventCallback} onEventCallback the callback to be called whenever an event is available for the subscription
 * @returns {String} - returns token
 */
EventstoreWithProjection.prototype.subscribe = function(query, offset, onEventCallback) {
    try {
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

        const targetOffset = parseInt(offset);
        if (isNaN(targetOffset) || targetOffset < 0) {
            throw new Error('offset should be greater than or equal to 0');
        }

        const token = shortid.generate();

        this._subscriptions[token] = {
            query: query,
            offset: offset
        };

        // async polling subscription
        this._doPollingSubscription(token, targetQuery, offset, onEventCallback);

        return token;
    } catch (error) {
        console.error('error in subscribe with params and error: ', query, offset, onEventCallback, error);
        throw error;
    }
};

/**
 * @param {String} token the position/offset from where we should start the subscription
 * @returns {Boolean} returns true if subscription token is existing and it got unsubscribed. false if it token does not exist
 */
EventstoreWithProjection.prototype.unsubscribe = function(token) {
    try {
        if (this._subscriptions[token]) {
            delete this._subscriptions[token];
            return true;
        } else {
            return false;
        }
    } catch (error) {
        console.error('error in subscribe with params and error: ', query, offset, onEventCallback, error);
        throw error;
    }
};


/**
 * @param {String} token unique token for this subscription
 * @param {AggregateQuery} query the query for the aggregate/category that we like to get
 * @param {Number} offset the position/offset from where we should start the subscription
 * @param {EventCallback} onEventCallback the callback to be called whenever an event is available for the subscription
 * @returns {Promise<string>} returns a void Promise
 */
EventstoreWithProjection.prototype._doPollingSubscription = async function(token, query, offset, onEventCallback) {
    const lastEvent = await this._getLastEventAsync(query);
    let revMin, revMax;

    if (lastEvent) {
        // if passed offset is greater than the current stream revision, then set to minimum plus 1 or subscribe to the next stream revision
        if (offset > lastEvent.streamRevision + 1) {
            revMin = lastEvent.streamRevision + 1;
        }
    } else {
        // if no event yet then we always start from zero
        revMin = 0;
    }

    revMax = revMin + this.options.pollingMaxRevisions;

    // start getEventStream loop. until token got unsubscribed or projection got deactivated
    // NOTE: we have a try catch on the do while to make sure that we dont break the loop. and another try catch on the for loop to make sure that if something happens
    // on the callback, we also dont break the loop
    let min = revMin;
    let max = revMax;
    do {
        try {
            const stream = await this._getEventStreamAsync(query, min, max);

            for (let index = 0; index < stream.events.length; index++) {
                const event = stream.events[index];
                try {
                    await this._emitEventToCallback(onEventCallback, event, this.options.eventCallbackTimeout);
                } catch (error) {
                    console.error('error in _doPollingSubscription loop: ', error)
                }
            }

            // continue the min with min + the number of events that were got from the stream
            min = min + stream.events.length;
            max = min + this.options.pollingMaxRevisions;

            await this._sleepUntilQueryIsNotified(query, this.options.pollingTimeout);
        } catch (error) {
            console.error('error in _doPollingSubscription loop with error: ', error);
        }

    } while (this.pollingActive && this._subscriptions[token]);
};

/**
 * @param {EventCallback} onEventCallback the callback to be called whenever an event is available for the subscription
 * @param {Event} event the event to emit
 * @param {Number} callbackTimeout the number of milliseconds to wait for the callback to call the iterator callback before throwing an error
 * @returns {Promise<void>} returns a Promise that is void
 */
EventstoreWithProjection.prototype._emitEventToCallback = async function(onEventCallback, event, callbackTimeout) {
    return new Promise((resolve, reject) => {
        try {
            if (onEventCallback) {
                const timeoutHandle = setTimeout(() => {
                    reject(new Error('timeout in onEventCallback'));
                }, callbackTimeout);
                onEventCallback(null, event, () => {
                    clearTimeout(timeoutHandle);
                    resolve();
                });
            } else {
                resolve();
            }
        } catch (error) {
            console.error('_emitEventToCallback with params and error:', onEventCallback, event, error);
            reject(error);
        }
    })
};

/**
 * @param {AggregateQuery} query the query for the aggregate/category that we like to get
 * @param {Number} offset the position/offset from where we should start the subscription
 * @returns {Promise<Event>} returns a Promise that resolves to an event 
 */
EventstoreWithProjection.prototype._sleepUntilQueryIsNotified = async function(query, sleepUntil) {
    return new Promise((resolve, reject) => {
        try {
            const timeOutToken = setTimeout(() => {
                resolve(false);
            }, sleepUntil);
        } catch (error) {
            console.error('_sleepUntilQueryIsNotified with params and error:', query, sleepUntil, error);
            reject(error);
        }
    })
};

/**
 * @param {AggregateQuery} query the query for the aggregate/category that we like to get
 * @param {Number} offset the position/offset from where we should start the subscription
 * @returns {Promise<Event>} returns a Promise that resolves to an event 
 */
EventstoreWithProjection.prototype._getLastEventAsync = async function(query) {
    return new Promise((resolve, reject) => {
        try {
            this.getLastEvent(query, (err, event) => {
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

EventstoreWithProjection.prototype._project = async function(projectionParams) {
    try {
        if (!projectionParams) {
            throw new Error('projectionParams is required');
        }

        if (!projectionParams.projectionId) {
            throw new Error('projectionId is required');
        }

        if (!projectionParams.query) {
            throw new Error('query is required');
        }
        const projectionId = projectionParams.projectionId;
        const channel = this._getChannel(projectionParams.query);
        const key = `projections:${this.projectionName}:${projectionId}:${channel}`;

        var queryProjection = {
            aggregateId: key,
            aggregate: 'projection',
            context: '__projections__'
        };

        const stream = await this._getLastEventAsStream(queryProjection);
    } catch (error) {
        console.error(error);
        console.error('error in Eventstore._project');
        throw error;
    }
};

/**
 * @param {AggregateQuery} query - aggregate query
 * @returns {Promise<stream>} - returns a stream as promise
 */
EventstoreWithProjection.prototype._getLastEventAsStream = function(query) {
    return new Promise((resolve, reject) => {
        try {
            this.getLastEventAsStream(query, function(err, stream) {
                if (err) {
                    reject(err);
                } else {
                    resolve(stream);
                }
            });
        } catch (error) {
            reject(error);
        }
    });
}

EventstoreWithProjection.prototype._getChannel = function(query) {
    const channel = (typeof(query) === 'string' ?
        `::${query}` :
        `${query.context || ''}:${query.aggregate || ''}:${query.aggregateId || query.streamId || ''}`);
    return channel;
}

module.exports = EventstoreWithProjection;