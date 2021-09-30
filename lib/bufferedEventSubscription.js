const debug = require('debug')('eventstore:bufferedEventSubscription');

/**
 * BufferedEventSubscriptionOptions
 * @typedef {Object} BufferedEventSubscriptionOptions
 * @property {Eventstore} es reference to the eventstore of the subscription
 * @property {EventStreamBuffer} streamBuffer reference to the stream buffer of the subscription
 * @property {Object} query object that represents the query of the subscription
 * @property {String} channel string value of the channel based on the query of the subscription
 * @property {Number} eventCallbackTimeout the number of milliseconds to wait for the callback to call the iterator callback before throwing an error
 * @property {Number} pollingTimeout timeout in milliseconds for the subscription's internal polling interval
 * @property {Number} pollingMaxRevisions maximum number of revisions to get for every polling interval
 */

/**
 * BufferedEventSubscription constructor
 * @class
 * @param {BufferedEventSubscriptionOptions} options options for the buffered event subscription
 * @constructor
 */
function BufferedEventSubscription (options) {
    this.es = options.es;
    this.waitable = options.waitable;
    this.streamBuffer = options.streamBuffer;
    this.query = options.query;
    this.channel = options.channel;
    this.eventCallbackTimeout = options.eventCallbackTimeout;
    this.pollingTimeout = options.pollingTimeout;
    this.pollingMaxRevisions = options.pollingMaxRevisions;
    this.errorMaxRetryCount = options.errorMaxRetryCount;
    this.errorRetryExponent = options.errorRetryExponent;

    this.activeSubscriptions = {};
    this.catchingUpSubscriptions = {};
    this.hasActivePoller = false;
    this.pollerRevMin = -1;
    this.pollerRevMax = -1;
}

BufferedEventSubscription.prototype = {
    /**
     * Subscribe to the buffered event subscription assigned to the query/channel
     * This will asynchronously query the streambuffer and the eventstore, and emit all existing events from the input revision
     * After which, this will asynchronously then create and subscribe to an internal poller that will emit new events whenever an event is received
     * @param {String} subscriptionToken the subscription token assigned to the subscriber
     * @param {Number} revision the position/offset from where we should start the subscription
     * @param {Function} onEventCallback callback to trigger whenever an event is emitted
     * @param {Function} onErrorCallback callback to trigger whenever an error is thrown at any point during polling
     * @returns {String} the subscription token assigned to the subscriber
     */
    subscribe: function(subscriptionToken, revision, onEventCallback, onErrorCallback) {
        const self = this;

        // Invoke catch-up and subscribe to the internal poller asynchronously
        self._catchUpAndSubscribeToPoller(subscriptionToken, revision, onEventCallback, onErrorCallback);

        // Return the subscription token to the subscriber
        return subscriptionToken;
    },

    /**
     * Unsubscribe to the buffered event subscription assigned to the query/channel
     * If there are no other active subscriptions, then the internal poller will stop.
     * @param {String} subscriptionToken the subscription token assigned to the subscriber
     */
    unsubscribe: function(subscriptionToken) {
        const self = this;

        if (self.catchingUpSubscriptions && self.catchingUpSubscriptions[subscriptionToken]) {
            debug('deleting catchup subscription', subscriptionToken);
            delete self.catchingUpSubscriptions[subscriptionToken];
        }
        if (self.activeSubscriptions && self.activeSubscriptions[subscriptionToken]) {
            debug('deleting active subscription', subscriptionToken);
            delete self.activeSubscriptions[subscriptionToken];
        }
    },

    /**
     * Deactivates the internal poller if it is running
     */
    deactivate: function() {
        const self = this;
        self.hasActivePoller = false;
    },

    /**
     * Activates the internal poller if it is not running and if there are active subscriptions
     */
    activate: function() {
        const self = this;

        // Only activate if the internal poller is inactive and there are active subscribers
        if (!self.hasActivePoller && self.activeSubscriptions && Object.values(self.activeSubscriptions).length > 0 &&
            self.pollerRevMin > -1 && self.pollerRevMax > -1) {
            self._startPolling(self.pollerRevMin, self.pollerRevMax);
        }
    },

    /**
     * Determines whether or not the BufferedEventSubscription is active.
     * @returns {boolean} true if the BufferedEventSubscription is active, and false otherwise.
     */
    isActive: function() {
        const self = this;

        return self.hasActivePoller && self.activeSubscriptions && Object.values(self.activeSubscriptions).length > 0;
    },

    /**
     * Closes the BufferedEventSubscription, but only if it is inactive.
     */
    close: function() {
        const self = this;

        if (!self.isActive()) {
            this.activeSubscriptions = null;
            this.catchingUpSubscriptions = null;
        }
    },

    /**
     * Private method that emits all stored events from the given revision that are stored in either the buffer and the eventstore,
     * and then registers the subscriber to the internal poller
     * @param {String} subscriptionToken the subscription token assigned to the subscriber
     * @param {Number} revision the position/offset from where we should emit stored events
     * @param {Function} onEventCallback callback to trigger whenever an event is emitted
     * @param {Function} onErrorCallback callback to trigger whenever an error is thrown at any point during polling
     */
    _catchUpAndSubscribeToPoller: async function(subscriptionToken, revision, onEventCallback, onErrorCallback) {
        const self = this;

        try {
            // Add first the token to the catch-up subscriptions
            if (self.catchingUpSubscriptions) {
                self.catchingUpSubscriptions[subscriptionToken] = true;
            }

            // Instantiate the min and max revision range based on input revision & compared with the last event
            let revMin, revMax;

            const lastEvent = await self._getLastEventAsync(self.query);
            if (lastEvent) {
                if (revision > lastEvent.streamRevision + 1) {
                    revMin = lastEvent.streamRevision + 1;
                } else {
                    revMin = revision;
                }
            } else {
                revMin = 0;
            }
            revMax = revMin + self.pollingMaxRevisions;
        
            let min = revMin;
            let max = revMax;
            let isDoneEmittingOldEvents = false;
        
            debug('CATCHUP: INITIAL RANGE:', min, max);
            // As long as the token is in catch-up state and is not done emitting events: Retrieve all events that are in the buffer & es, then subscribe to a poller
            while(self.catchingUpSubscriptions && self.catchingUpSubscriptions[subscriptionToken] && !isDoneEmittingOldEvents) {
                debug('CATCHING UP...', min, max, self.query);
                // Attempt to retrieve the stream events from the Stream Buffer, given the current min and max range
                let stream;
                if (self.streamBuffer) {
                    stream = self.streamBuffer.getEventsInBufferAsStream(min, max);
                } else {
                    debug('CATCHUP: NO STREAM BUFFER');
                }
    
                if (stream && stream.events.length > 0) {
                    // Signifies events retrieved from Stream Buffer
    
                    // Found events in the Buffer. Emit the events
                    debug('Emitting old events from stream buffer');
                    await self._emitEvents(stream.events, onEventCallback);
        
                    // Increase min & max
                    min = min + stream.events.length;
                    max = min + self.pollingMaxRevisions;
    
                    // Continue loop
                    continue;
                } else {
                    // No events retrieved from the Stream Buffer. Verify if there are events from the eventstore, or subscribe to a poller
    
                    // Check first if there is an active internal poller for the given range
                    const hasPoller = self._isActiveAtRange(min, max);
    
                    if (!hasPoller) {
                        // No internal poller for this range. Verify if there are events in the ES before starting a new internal poller
    
                        // Check first if there are events in the event store
                        stream = await self._getEventStreamAsyncWithRetry(self.query, min, max);
    
                        if (stream && stream.events.length > 0) {
                            // Found events in the ES. Attempt to offer the events to the buffer, then emit the events
                            if (self.streamBuffer) {
                                self.streamBuffer.offerEvents(stream.events);
                            }
                            
                            // Emit the events
                            debug('Emitting old events from eventstore');
                            await self._emitEvents(stream.events, onEventCallback);
                
                            // Increase the min & max
                            min = min + stream.events.length; // REVIEW WHY MIN DEPENDS ON LENGTH, NOT ON ACTUAL LAST REVISION
                            max = min + self.pollingMaxRevisions;
    
                            // Continue loop
                            continue;
                        } else {
                            // Still no events from ES. We are done emitting initial events. Start an internal poller from here
                            self._createAndSubscribeToPoller(subscriptionToken, min, max, onEventCallback, onErrorCallback);
    
                            // Break the loop
                            isDoneEmittingOldEvents = true;
                            break;
                        }
                    } else {
                        // Done emitting initial events since we ended up with the internal poller
    
                        // Subscribe to the internal poller
                        self._subscribeToPoller(subscriptionToken, onEventCallback, onErrorCallback);
    
                        // Break the loop
                        isDoneEmittingOldEvents = true;
                        break;
                    }
                }
            };
        } catch (error) {
            // Error caught on catch-up phase. Unsubscribe the subscriber
            self.unsubscribe(subscriptionToken);
            
            // Trigger the error callback
            onErrorCallback(error);
        }
    },

    /**
     * Private method that emits all events to a specified callback
     * @param {Array} events the events to emit
     * @param {Function} onEventCallback callback to trigger for each event
     */
    _emitEvents: async function(events, onEventCallback) {
        const self = this;
        for(let i = 0; i < events.length; i++) {
            const event = events[i];
            try {
                await self._emitEventToCallback(onEventCallback, event);
            } catch (error) {
                console.error('error in _emitEventsToListeners loop: ', event, error)
            }
        }
    },

    /**
     * Private method that emits all events to all registered subscribers
     * @param {Array} events the events to emit
     */
    _emitEventsToListeners: async function(events) {
        const self = this;
        const listeners = Object.values(self.activeSubscriptions);

        for(let i = 0; i < events.length; i++) {
            const event = events[i];
            for(let k = 0; k < listeners.length; k++) {
                const onEventCallback = listeners[k].onEventCallback;
                try {
                    await self._emitEventToCallback(onEventCallback, event);
                } catch (error) {
                    console.error('error in _emitEventsToListeners loop: ', event, error)
                }
            }
        }
    },

    /**
     * @param {EventCallback} onEventCallback the callback to be called whenever an event is available for the subscription
     * @param {Event} event the event to emit
     * @returns {Promise<void>} returns a Promise that is void
     */
    _emitEventToCallback: async function(onEventCallback, event) {
        const self = this;
        return new Promise((resolve, reject) => {
            try {
                if (onEventCallback) {
                    const timeoutHandle = setTimeout(() => {
                        reject(new Error('timeout in onEventCallback'));
                    }, self.eventCallbackTimeout);
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
        });
    },

    /**
     * Private method that emits an error to all registered subscribers
     * @param {Error} error the error to emit
     */
    _emitErrorToListeners: async function(error) {
        const self = this;
        const listeners = Object.values(self.activeSubscriptions);

        for(let k = 0; k < listeners.length; k++) {
            const onErrorCallback = listeners[k].onErrorCallback;
            if (onErrorCallback) {
                onErrorCallback(error);
            }
        }
    },

    /**
     * Private method that checks whether or not there is an active internal poller for the given revision range
     * @param {Number} min the minimum revision
     * @param {Number} max the maximum revision
     */
    _isActiveAtRange: function(min, max){
        const self = this;

        // Verify if there are active subscriptions, and the range matches the internal poller's current range
        return self.isActive() && self.pollerRevMin === min && self.pollerRevMax === max;
    },

    /**
     * Private method that creates the internal poller and registers the subscriber to that poller
     * @param {String} subscriptionToken the subscription token assigned to the subscriber
     * @param {Number} min the minimum revision range of the internal poller
     * @param {Number} max the maximum revision range of internal poller
     * @param {Function} onEventCallback callback function of the subscriber, to trigger whenever an event is emitted
     */
    _createAndSubscribeToPoller: function(subscriptionToken, min, max, onEventCallback, onErrorCallback){
        const self = this;

        // Only valid if the subscription token is in catch-up state
        if (self.catchingUpSubscriptions && self.catchingUpSubscriptions[subscriptionToken]) {
            const token = self._subscribeToPoller(subscriptionToken, onEventCallback, onErrorCallback);
    
            if (!self.hasActivePoller) {
                // Trigger Polling asynchronously. Poller will trigger as long as their are subscriptions
                self._startPolling(min, max);
            }
            return token;
        }
    },

    /**
     * Private method that registers the subscriber to the internal poller
     * @param {String} subscriptionToken the subscription token assigned to the subscriber
     * @param {Function} onEventCallback callback function of the subscriber, to trigger whenever an event is emitted
     */
    _subscribeToPoller: function(subscriptionToken, onEventCallback, onErrorCallback){
        const self = this;
        // Only valid if the subscription token is in catch-up state
        if (self.catchingUpSubscriptions && self.catchingUpSubscriptions[subscriptionToken]) {
            // Delete the token from the catch-up subscriptions
            delete self.catchingUpSubscriptions[subscriptionToken];

            // Add the token to the active subscriptions
            if (self.activeSubscriptions) {
                self.activeSubscriptions[subscriptionToken] = {
                    onEventCallback: onEventCallback,
                    onErrorCallback: onErrorCallback
                };
                return subscriptionToken;
            }
        }
    },

    /**
     * Private method that triggers the internal poller and runs as long as there are active subscribers
     * @param {Number} min the minimum revision range of the internal poller
     * @param {Number} max the maximum revision range of internal poller
     */
    _startPolling: async function(min, max) {
        const self = this;
        self.pollerRevMin = min;
        self.pollerRevMax = max;

        self.hasActivePoller = true;
        try {
            while (self.hasActivePoller && self.activeSubscriptions && Object.keys(self.activeSubscriptions).length > 0) {
                debug('Polling...', self.pollerRevMin, self.pollerRevMax, self.query);
                await self._pollForNotify(self.pollerRevMin);

                if (self.hasActivePoller && self.activeSubscriptions && Object.keys(self.activeSubscriptions).length > 0) {
                    // Try to read ES if there are new events
                    const stream = await self._getEventStreamAsyncWithRetry(self.query, self.pollerRevMin, self.pollerRevMax);
            
                    if (stream.events.length <= 0) {
                        // No new events retrieved from ES. Initiate poll again
                    } else {
                        // New events retrieved from ES.
            
                        // Add these events to the Buffer
                        if (self.streamBuffer) {
                            debug('OFFERING EVENT TO BUFFER');
                            self.streamBuffer.offerEvents(stream.events);
                        } else {
                            debug('NOT OFFERING EVENT TO BUFFER');
                        }

                        // Emit the events via publish for all poller subscriptions
                        debug('Poller emitting new events via polling');
                        await self._emitEventsToListeners(stream.events);
            
                        // Increase the min & max
                        self.pollerRevMin = self.pollerRevMin + stream.events.length; // REVIEW WHY MIN DEPENDS ON LENGTH, NOT ON ACTUAL LAST REVISION
                        self.pollerRevMax = self.pollerRevMin + self.pollingMaxRevisions;
                    }
                    continue;
                } else {
                    break;
                }
            };
        } catch (error) {
            debug(`Poller for channel ${self.channel} has stopped polling via error`);
            // Error caught on polling phase. Emit the error to all active subscribers
            self._emitErrorToListeners(error);

            // The poller is considered deactivated at this point.
        }
        debug(`Poller for channel ${self.channel} has stopped polling`);
        self.hasActivePoller = false;
    },

    _sleep: async function (timeout, rejectOnTimeout) {
        return new Promise((resolve, reject) => {
          setTimeout(function () {
            if (rejectOnTimeout) {
              reject(new Error('timed out'));
            } else {
              resolve();
            }
          }, timeout);
        })
      },
    /**
     * Private method that does the internal polling
     * @param {Number} minRev the minimum revision range to verify whether or not there are new events
     */
    _pollForNotify: async function(minRev) {
        const self = this;
        debug('polling at query:', self.query, new Date().getTime());
        try {
            await this.waitable.waitForSignal(self.channel, self.pollingTimeout);
        } catch (error) {
            console.error('_pollForNotify with params and error:', self.query, self.pollingTimeout, error);
            throw error;
        }
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
    }
};

module.exports = BufferedEventSubscription;
