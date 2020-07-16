const Eventstore = require('../eventstore');
const _ = require('lodash');
const util = require('util');
const shortid = require('shortid');
const debug = require('debug')('eventstore:projection');
var Redis = require("ioredis");
var PubSub = require('pubsub-js');
const {
    resolve
} = require('path');
const {
    reject, cargo
} = require('async');

/**
 * JobsManager
 * @typedef {import('./jobs-manager').JobsManager} JobsManager
 */

/**
 * JobsManager
 * @typedef {import('./distributed-lock').DistributedLock} DistributedLock
 */

/**
 * EventstoreProjectionOptions
 * @typedef {Object} EventstoreProjectionOptions
 * @property {DistributedLock} distributedLock Distributed lock helper
 * @property {JobsManager} jobsManager Jobs manager
 * @property {Number} pollingMaxRevisions maximum number of revisions to get for every polling interval
 * @property {Number} pollingTimeout timeout in milliseconds for the polling interval
 * @property {String} projectionGroup name of the projectionGroup if using projection
 * @property {String} eventNameFieldName the field name of the event's name in the payload. Default is "name"
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
 * @property {String} id The events of this stream
 * @property {Object} state Add the event to the stream
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
 * ProjectionParams
 * @typedef {Object} Projection
 * @property {String} projectionId The unique projectionId of the projection
 * @property {AggregateQuery} query The query to use to get the events for the projection
 * @property {ProjectionPlaybackInterface} playbackInterface The object to use for when an event for this projection is received
 * @property {Object} meta Optional user meta data
 * @property {("stream"|"")} partitionBy Partition the state by stream, using a function or no partition. If outputState is false, then this option is ignored
 * @property {("true"|"false")} outputState Saves the projection state as a stream of states. Default is false
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
 * EventstoreWithProjection constructor
 * @param {EventstoreProjectionOptions} options additional options for the Eventstore projection extension
 * @constructor
 */
function EventstoreWithProjection(options, store) {
    this.pollingActive = true;
    this._subscriptions = {};
    this._projections = {};
    this._jobs = {};

    if (options) {
        options.notifyCommitRedisChannel = options.notifyCommitRedisChannel || 'NOTIFY_COMMIT_REDIS_CHANNEL';
        options.notifyCompletedJobRedisChannel = options.notifyCompletedJobRedisChannel || 'NOTIFY_COMPLETED_JOB_REDIS_CHANNEL';
        options.pollingMaxRevisions = options.pollingMaxRevisions || 5;
        options.pollingTimeout = !isNaN(parseInt(options.pollingTimeout)) ? parseInt(options.pollingTimeout) : 1000;
        options.eventCallbackTimeout = !isNaN(parseInt(options.eventCallbackTimeout)) ? parseInt(options.eventCallbackTimeout) : 10000;
        options.projectionGroup = options.projectionGroup ? `${options.projectionGroup}` : 'default';
        options.eventNameFieldName = options.eventNameFieldName || 'name';
    }

    debug('event store created with options:', options);
    Eventstore.call(this, options, store);
}

util.inherits(EventstoreWithProjection, Eventstore);

EventstoreWithProjection.prototype = Eventstore.prototype;
EventstoreWithProjection.prototype.constructor = EventstoreWithProjection;

/**
 * @type {EventstoreProjectionOptions}
 */
EventstoreWithProjection.prototype.options;

/**
 * @type {Projection[]}
 */
EventstoreWithProjection.prototype._projections;

/**
 * @returns {void} - returns void
 */
EventstoreWithProjection.prototype.deactivatePolling = function() {
    this.pollingActive = false;
};

/**
 * @returns {void} - returns void
 */
EventstoreWithProjection.prototype.activatePolling = function() {
    this.pollingActive = true;
};

/**
 * @param {DoneCallbackFunction} callback success/error callback
 * @returns {void} - returns void
 */
EventstoreWithProjection.prototype.startAllProjections = function(callback) {
    try {
        debug('startAllProjections called');

        // process job groups
        if (this.options.jobsManager) {
            const jobGroup = this._getProjectionJobGroup();
            this.options.jobsManager.processJobGroup(this, jobGroup, this._processJob, this._processCompletedJob).then(() => {
                if (callback) {
                    callback();
                }
            }).catch((err) => {
                callback(err);
            });
        } else {
            if (callback) {
                callback();
            }
        }
    } catch (error) {
        console.error('error in startAllProjections with params and error:', error);
        if (callback) {
            callback(error);
        }
    }
};

/**
 * @param {Projection} projection parameters for the projection
 * @param {Function} callback success/error callback
 * @returns {void} - returns void
 */
EventstoreWithProjection.prototype.project = function(projection, callback) {
    try {
        debug('projection called with params:', projection);
        // just preserving that adrai's public functions are callback based but internally we can just use async/await for elegance
        this._project(projection).then(callback).catch(function(error) {
            if (callback) {
                callback(error);
            }
        });
    } catch (error) {
        console.error('error in project with params and error:', projection, error);
        if (callback) {
            callback(error);
        }
    }
};

/**
 * @param {AggregateQuery} query the query for the aggregate/category that we like to get
 * @param {Number} revision the revision from where we should start the subscription
 * @param {EventCallback} onEventCallback the callback to be called whenever an event is available for the subscription
 * @returns {String} - returns token
 */
EventstoreWithProjection.prototype.subscribe = function(query, revision, onEventCallback) {
    try {
        debug('subscribe called with params: ', query, revision);
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

        this._subscriptions[token] = {
            query: query,
            offset: revision
        };

        // async polling subscription
        this._doPollingSubscription(token, targetQuery, revision, onEventCallback);

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
    try {
        if (this._subscriptions[token]) {
            delete this._subscriptions[token];
            return true;
        } else {
            return false;
        }
    } catch (error) {
        console.error('error in unsubscribe with params and error: ', token, error);
        throw error;
    }
};

/**
 * @returns {void} - returns void
 */
EventstoreWithProjection.prototype.setupNotifyPubSub = function(redisSub, redisPub) {
    const self = this;
    self.redisSub = redisSub;
    self.redisPub = redisPub;
    
    self.redisSub.on('ready', function() {
        self.redisSub.subscribe(self.options.notifyCommitRedisChannel, self.options.notifyCompletedJobRedisChannel);
    });

    self.redisSub.on('message', function(redisChannel, strData) {
        self._processReceivedRedisMessage(redisChannel, strData);
    });

    if (self.commit) {
        var originalMethod = self.commit;
        self.commit = function (eventstream, callback) {
            let args = [eventstream];
            args = _.without(args, undefined, null);
            callback = self._enhanceCallback(callback, function() {
                const query = self._getQuery(args[0]);
                if (query) {
                    var data = {
                        query: query,
                        revision: args[0].lastRevision
                    };
                    self.redisPub.publish(self.options.notifyCommitRedisChannel, JSON.stringify(data));
                }
            });
            return originalMethod.apply(self, _.concat(args, callback || []));
        };
    }
}

/**
 * 
 * @param {Function} callback original callback
 * @param {Function} preCallback hooked callback
 */
EventstoreWithProjection.prototype._enhanceCallback = function(callback, preCallback) {
    const self = this;
    var originalCallback = callback;
    
    callback = function () {
        if (originalCallback) {
            preCallback();
            return originalCallback.apply(self, arguments);
        }
        return;
    };
    return callback;
}

/**
 * 
 * @param {EventStoreSettings} options 
 * @param {String} redisChannel channel from where the message came from
 * @param {*} strData 
 */
EventstoreWithProjection.prototype._processReceivedRedisMessage = function(redisChannel, strData) {
    const self = this;
    let data = JSON.parse(strData);
    if(redisChannel == self.options.notifyCommitRedisChannel) {
        // publish to all channels supported
        const fullChannel = self._getChannel(data.query);
        PubSub.publish(fullChannel, data);

        const streamChannel = self._getChannel({
            aggregateId: data.query.aggregateId,
            streamId: data.query.streamId
        });
        PubSub.publish(streamChannel, data);
    } else if(redisChannel == self.options.notifyCompletedJobRedisChannel) {
        if(self._jobs && data.jobId) {
            const job = self._jobs[data.jobId];
            if(job) {
                PubSub.unsubscribe(job.token);
                delete self._jobs[job.jobId];
            }
        }
    }
}

/**
 * @param {AggregateQuery} query 
 * @returns {String} - returns a dot delimited channel
 */
EventstoreWithProjection.prototype._getChannel = function(query) {
    let channel;
    if(query.context) {
        channel += query.context;
        if(query.aggregate) {
            channel = channel + `.${query.aggregate}`
        }
        if(query.aggregateId) {
            channel = channel + `.${query.aggregateId}`
        }
    } else {
        if(query.aggregateId) {
            channel = query.aggregateId;
        }
        if(query.streamId) {
            channel = query.streamId;
        }
    }
    return channel;
}

/**
 * Forms filtered query object for am object
 * @param {Any} object any object that may contain query properties 
 * @returns {AggregateQuery} - returns an aggregate query
 */
EventstoreWithProjection.prototype._getQuery = function(object) {
    let query = {};
    if(object.context) {
        query.context = object.context;
    }
    if(object.aggregate) {
        query.aggregate = object.aggregate;
    }
    if(object.aggregateId) {
        query.aggregateId = object.aggregateId;
    }
    if(object.streamId) {
        query.streamId = object.streamId;
    }
    return query;
}

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
        } else {
            revMin = offset;
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

            debug('got stream: ', stream);

            // NOTE: sleep if events length is 0, this means no events to process
            if(stream.events.length <= 0) {
                await this._sleepUntilQueryIsNotified(query, this.options.pollingTimeout, min);
            }

            for (let index = 0; index < stream.events.length; index++) {
                const event = stream.events[index];
                try {
                    await this._emitEventToCallback(onEventCallback, event, this.options.eventCallbackTimeout);
                } catch (error) {
                    console.error('error in _doPollingSubscription loop: ', event, error)
                }
            }

            // continue the min with min + the number of events that were got from the stream
            min = min + stream.events.length;
            max = min + this.options.pollingMaxRevisions;
        } catch (error) {
            console.error('error in _doPollingSubscription loop with error: ', query, min, max, error);
        }

    } while (this.pollingActive && this._subscriptions[token]);
};

/**
 * @param {String} jobId the job to process
 * @param {Object} jobData callback to notify that the job is done
 * @param {Object} lastResult last result that was passed to the done 
 * @param {import('./jobs-manager').JobDoneCallback} done done callback. pass a result to be saved per jobid. this result will be passed to the lastResult parameter the next time a job with the same id is processed
 * @returns {Promise<void>} returns Promise of type void
 */
EventstoreWithProjection.prototype._processJob = async function(jobId, jobData, lastResult) {
    try {
        // jobData is the projection
        const jobProjection = jobData;
        // we need to get the one in _projections because it has the callback
        const projection = this._projections[jobProjection.projectionId];
        let offset = lastResult ? parseInt(lastResult.lastOffset) : 0;

        if (isNaN(offset)) {
            offset = 0;
        }

        let eventsCount = 0;
        do {
            const events = await this._getEventsAsync(projection.query, offset, this.options.pollingMaxRevisions);
            eventsCount = events.length;

            if (events) {
                debug('got events in getEventsAsync for playback:', events, events.length);
            }

            for (let i = 0; i < events.length; i++) {
                const event = events[i];
                try {
                    debug('got event in getEventsAsync for playback:', event);
                    await this._playbackEvent(event, projection, this.options.pollingTimeout);
                } catch (error) {
                    console.error('error in playing back event in loop with params and error', event, projection, error);
                    // TODO: handle errors on playback 
                }
            }
            offset = offset + events.length;
        } while (eventsCount > 0)

        return {
            lastOffset: offset
        };
    } catch (error) {
        console.error('_processJob failed with parameters and error', jobId, jobData, lastResult, error);
        throw error;
    }
};

/**
 * @param {import('./jobs-manager').Job} job the job that was completed
 * @returns {void} returns void
 */
EventstoreWithProjection.prototype._processCompletedJob = function(jobId, jobData) {
     // this is the polling code.
    // notify completion for clearing subscribe PUBSUB
    if (this.redisPub) {
        var data = {
            jobId: jobId
        };
        this.redisPub.publish(this.options.notifyCompletedJobRedisChannel, JSON.stringify(data));
    }
    // queue the same job again to poll with a delay
    // projection is just the jobData
    // jobData is the projection
    const jobProjection = jobData;
    // we need to get the one in _projections because it has the callback
    const projection = this._projections[jobProjection.projectionId];

    const projectionKey = `projection-group:${this.options.projectionGroup}:projection:${projection.projectionId}`;
    this._queueProjection(projectionKey, projection, this.options.pollingTimeout);
};

/**
 * @param {Event} event the event to process
 * @param {Projection} projection the object/interface to use to playback the event
 * @param {Number} timeout callback to notify that the job is done
 * @returns {Promise<void>} returns a Promise that resolves to a void
 */
EventstoreWithProjection.prototype._playbackEvent = function(event, projection, timeout) {
    return new Promise((resolve, reject) => {
        // TODO: fix timeout with promise based callback. how to have an async await timeout handling?
        // This code already becomes like a callback hell
        let timeoutHandle = setTimeout(() => {
            reject(new Error('timeout in calling the playbackInterface'));
        }, timeout);

        try {
            // TODO: evaluate if we still need this. we might but removing it for now
            const self = this;
            const funcs = {
                emit: function(targetQuery, event, done) {
                    self._saveEventAsync(targetQuery, event).then(done).catch(done);
                }
            };

            debug('event in playbackEvent', event);

            const eventName = event.payload[this.options.eventNameFieldName];
            let eventHandlerFunc = projection.playbackInterface[eventName];
            
            if (!eventHandlerFunc) {
                // if no handler for this event, then check if there is an $any event handler.
                // if yes then use that
                const anyEventHandlerFunc = projection.playbackInterface['$any'];
                if (anyEventHandlerFunc) {
                    eventHandlerFunc = anyEventHandlerFunc;
                }
            }

            if (eventHandlerFunc) {
                if (projection.outputState === 'true') {
                    this._getProjectionState(projection, event).then((projectionState) => {
                        debug('got projectionState', projectionState);
                        const mutableState = _.cloneDeep(projectionState.state);

                        eventHandlerFunc(mutableState, event, funcs, (error) => {
                            clearTimeout(timeoutHandle);
                            timeoutHandle = null;
                            if (error) {
                                console.error('error in playbackFunction with params and error', event, projection, error);
                                reject(error)
                            } else {
                                // if the old state and the mutableState is not equal (meaning the playback updated it) then output a state
                                if (!_.isEqual(projectionState.state, mutableState)) {
                                    debug('state changed, saving new state', projectionState.state, mutableState);
                                    this._saveProjectionState(projection.projectionId, projectionState.id, mutableState).then(resolve).catch(reject);
                                } else {
                                    debug('state did not change, continuing', )
                                    resolve();
                                }
                            }
                        });
                    }).catch(reject);
                } else {
                    eventHandlerFunc(null, event, funcs, (error) => {
                        clearTimeout(timeoutHandle);
                        timeoutHandle = null;
                        if (error) {
                            console.error('error in playbackFunction with params and error', event, projection, error);
                            reject(error)
                        } else {
                            resolve();
                        }
                    });
                }
            } else {
                debug('eventHandlerFunc is undefined. no function handler for this sevent');
                resolve();
            }
        } catch (error) {
            if (timeoutHandle) {
                clearTimeout(timeoutHandle);
            }
            console.error('error in _playbackEvent with params and error', event, projection, error);
            reject(error);
        }
    })
};

/**
 * @param {AggregateQuery} targetQuery the query to emit the event
 * @param {Object} event the event to emit
 * @param {DoneCallbackFunction} done the last event that built this projection state
 * @returns {void} returns a void promise
 */
EventstoreWithProjection.prototype._emit = function(targetQuery, event, done) {

    this._saveEventAsync(targetQuery, event).then(done).catch(done);
};

/**
 * @param {AggregateQuery} targetQuery the query to emit the event
 * @param {Object} event the event to emit
 * @param {DoneCallbackFunction} done the last event that built this projection state
 * @returns {Promise<void>} returns a Promise that resolves to a void
 */
EventstoreWithProjection.prototype._saveEventAsync = async function(targetQuery, event) {
    try {
        const stream = await this._getEventStreamAsync(targetQuery);

        // TODO: add eventLink and eventSource
        await this._addEventToStream(stream, event);
        await this._commitStream(stream);
    } catch (error) {
        console.error('error in _emitAsync', targetQuery, event, error);
        throw error;
    }
};

/**
 * @param {String} projectionId the projectionid
 * @param {String} projectionStateId the projection state to save
 * @param {Object} newState the last event that built this projection state
 * @returns {Promise<void>} returns a Promise that resolves to a void
 */
EventstoreWithProjection.prototype._saveProjectionState = async function(projectionId, projectionStateId, newState) {
    const query = {
        aggregateId: projectionStateId,
        aggregate: projectionId,
        context: 'states'
    };

    await this._saveEventAsync(query, newState);
};


/**
 * @param {Projection} projection the projection
 * @param {Event} lastProjectionEvent the last event got from the projection 
 * @returns {Promise<ProjectionState>} returns a Promise that resolves to a void
 */
EventstoreWithProjection.prototype._getProjectionState = async function(projection, lastProjectionEvent) {
    // default is no partition format: <projectionid>-result
    let streamId = this._getProjectionStateStreamId(projection, lastProjectionEvent);

    const query = {
        aggregateId: streamId,
        aggregate: projection.projectionId,
        context: 'states'
    }

    debug('getting projection last state of streamId', streamId);
    const lastEvent = await this._getLastEventAsync(query);

    debug('got projection last state of streamId', streamId, lastEvent);

    let state = {};
    if (lastEvent) {
        // payload is the last state
        state = lastEvent.payload;
    } else {
        // if we have no event yet given the streamId, then initialize it
        if (typeof projection.playbackInterface.$init === 'function') {
            try {
                const initState = projection.playbackInterface.$init();
                if (initState) {
                    state = initState;
                }
            } catch (error) {
                // NOTE: just log for now. and not let errors stop the playback loop
                console.error('error in playbackInterface.$init with params and error', projection, lastProjectionEvent, error)
            }
        }
    }

    return {
        id: streamId,
        state: state
    }
};

/**
 * @param {Projection} projection the projection
 * @param {Event} lastProjectionEvent the query 
 * @returns {String} returns a Promise that resolves to a void
 */
EventstoreWithProjection.prototype._getProjectionStateStreamId = function(projection, lastProjectionEvent) {
    // default is no partition format: <projectionid>-result

    let streamId = `${projection.projectionId}-result`;
    if (projection.partitionBy === 'stream') {
        let streamPartitionedStreamId = `${projection.projectionId}`;
        // format: <projectionid>-result[-<context>][-<aggregate>]-<aggregateId>
        if (lastProjectionEvent.context)
            streamPartitionedStreamId += `-${lastProjectionEvent.context}`;
        if (lastProjectionEvent.aggregate)
            streamPartitionedStreamId += `-${lastProjectionEvent.aggregate}`;
        streamPartitionedStreamId += `-${lastProjectionEvent.aggregateId || lastProjectionEvent.streamId}`;
        streamPartitionedStreamId += '-result';

        streamId = streamPartitionedStreamId;
    } else if (typeof projection.partitionBy === 'function') {
        try {
            const partitionId = projection.partitionBy(lastProjectionEvent);

            // if partitionId is defined then use it, else just use the default partititon which is by projection
            if (partitionId) {
                // format: <projectionid>-<paritionId>-result
                streamId = `${projection.projectionId}-${partitionId}-result`;
            }

        } catch (error) {
            // NOTE: log for now and use the default as the streamId (no partition)
            console.error('error in calling projection.partitionBy with params and error', projection, lastProjectionEvent, error);
        }
    }

    debug('_getProjectionStateStreamId streamId', lastProjectionEvent);

    return streamId;
};

/**
 * @returns {String} returns the projection job group name
 */
EventstoreWithProjection.prototype._getProjectionJobGroup = function() {
    return `projection-group:${this.options.projectionGroup}`;
};


/**
 * @param {EventCallback} onEventCallback the callback to be called whenever an event is available for the subscription
 * @param {Event} event the event to emit
 * @param {Number} callbackTimeout the number of milliseconds to wait for the callback to call the iterator callback before throwing an error
 * @returns {Promise<void>} returns a Promise that is void
 */
EventstoreWithProjection.prototype._emitEventToCallback = function(onEventCallback, event, callbackTimeout) {
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
 * @param {Number} minRev minimum revision
 * @returns {Promise<Bool>} returns a Promise that resolves to an event 
 */
EventstoreWithProjection.prototype._sleepUntilQueryIsNotified = async function(query, sleepUntil, minRev) {
    const self = this;
    return new Promise((resolve, reject) => {
        try {
            let token;
            const timeoutHandler = setTimeout(() => {
                if(token) {
                    PubSub.unsubscribe(token);
                }
                resolve(false);
            }, sleepUntil);

            const channel = self._getChannel(query);
            token = PubSub.subscribe(channel, function(redisChannel, data) {
                if (!isNaN(data.revision) && data.revision >= minRev) {
                    clearTimeout(timeoutHandler);
                    PubSub.unsubscribe(token);
                    resolve(true);
                }
            });
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
 * @param {Number} offset how many events to skip
 * @param {Number} limit max items to return
 * @returns {Promise<Event[]>} returns a Promise that resolves to an array of Events
 */
EventstoreWithProjection.prototype._getEventsAsync = async function(query, offset, limit) {
    return new Promise((resolve, reject) => {
        try {
            this.getEvents(query, offset, limit, (err, events) => {
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
 * @param {Projection} projection parameters for the projection
 * @returns {Promise<void>} returns a Promise of type void
 */
EventstoreWithProjection.prototype._project = async function(projection) {
    if (!projection) {
        throw new Error('projection is required');
    }

    if (!projection.projectionId) {
        throw new Error('projectionId is required');
    }

    if (!projection.query) {
        throw new Error('query is required');
    }

    if (!projection.query.aggregate && !projection.query.context && !projection.query.aggregateId && !projection.query.streamId) {
        throw new Error('at least an aggregate, context or aggregateId/streamId is required');
    }

    // use a distributed lock to make sure that we only create one event for the projection stream
    const projectionKey = `projection-group:${this.options.projectionGroup}:projection:${projection.projectionId}`;

    // set projection to private list of projections
    this._projections[projection.projectionId] = projection;

    let lockToken;
    if (this.options.distributedLock) {
        const lockKey = projectionKey;
        lockToken = await this.options.distributedLock.lock(lockKey);
    }

    const projectionId = projection.projectionId;
    const stream = await this._getProjectionStream(projectionId);

    let projectionExists = stream && stream.events && stream.events.length > 0;
    if (projectionExists) {
        // existing already. do nothing
        // TODO: check for failed/stalled polling job, same query check
        debug('projection already exists', projectionId);
    } else {
        // not exists go add
        debug('projection does not exist adding', projectionId);
        await this._addProjectionStream(stream, projection);
        await this._queueProjection(projectionKey, projection);
    }

    // unlock
    if (this.options.distributedLock && lockToken) {
        await this.options.distributedLock.unlock(lockToken);
    }
};

/**
 * @param {String} projection projectionKey the key for this projection
 * @param {Projection} projection parameters for the projection
 * @param {Number} delay delay before processing the projection
 * @returns {Promise<void>} returns a Promise of type void
 */
EventstoreWithProjection.prototype._queueProjection = async function(projectionKey, projection, delay) {
    // send to jobsManager if it does not exist
    const jobsManager = this.options.jobsManager;
    const jobId = projectionKey;

    const jobGroup = this._getProjectionJobGroup();
    if (jobsManager) {
        const job = {
            id: jobId,
            group: jobGroup,
            payload: projection
        };

        debug('queueing a job', job);

        await jobsManager.queueJob(job, {
            delay: delay
        });
        await this._waitUntilNotifyProject(jobId, projection.query, delay);
    }
};

/**
 * @param {String} jobId jobId of being waited
 * @param {AggregateQuery} query aggregate query
 * @param {Number} delay determines if there is a need to wait
 * @returns {Promise<void>} returns a Promise of type void
 */
EventstoreWithProjection.prototype._waitUntilNotifyProject = async function(jobId, query, delay) {
    try {
        // should match with 
        // context
        // context.aggregate
        // context.aggregate.aggregateId
        // streamId
        // aggregateId
        if(delay > 0) {
            const self = this;
            const _query = self._getQuery(query);
            const channel = self._getChannel(_query);
            const token = PubSub.subscribe(channel, function(redisChannel, data) {
                // Loop through known jobs, then  promote
                const jobGroup = self._getProjectionJobGroup();
                _.forEach(self._jobs, function(value, key) {
                    const jobQuery = value.query;
                    const notifiedQuery = data.query;
                    if(jobQuery) {
                        let notify = false;
                        if(jobQuery.context && jobQuery.context === notifiedQuery.context) {
                            if(jobQuery.aggregate && jobQuery.aggregate === notifiedQuery.aggregate) {
                                if(jobQuery.aggregateId && jobQuery.aggregateId === notifiedQuery.aggregateId) {
                                    // NOTE: match on context, aggregate and aggregateId
                                    notify = true;
                                } else if(!jobQuery.aggregateId) {
                                    // NOTE: match on context and aggregate
                                    notify = true;
                                }
                            } else if(!jobQuery.aggregate) {
                                // NOTE: match only in context
                                notify = true;
                            }
                        } else if(jobQuery.streamId || jobQuery.aggregateId) {
                            if(jobQuery.aggregateId && jobQuery.aggregateId === notifiedQuery.aggregateId) {
                                // NOTE: match on aggregateId
                                notify = true;
                            }
                            if(jobQuery.streamId && jobQuery.streamId === notifiedQuery.streamId) {
                                // NOTE: match on streamId
                                notify = true;
                            }
                        }
                        if(notify) {
                            const jobToken = value.token;
                            const jobId = value.jobId;
        
                            PubSub.unsubscribe(jobToken);
                            self.options.jobsManager.promoteJob(jobId, jobGroup);
                        }
                    }
                });
            });
            self._jobs[jobId] = {
                jobId: jobId,
                channel: channel,
                query: _query,
                token: token
            }
        }
    } catch(error) {
        console.error('error in _waitUntilNotifyProject:', query, error);
        throw error;
    }
}

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
        const stream = await this._getLastEventAsStreamAsync(queryProjection);
        return stream;
    } catch (error) {
        console.error('error in _getProjection:', projectionId, error);
        throw error;
    }
}

/**
 * @param {EventStream} stream the stream into which to add the projection
 * @param {Projection} projection the projection to add
 * @returns {Promise<void>} returns a Promise of type void
 */
EventstoreWithProjection.prototype._addProjectionStream = async function(stream, projection) {
    try {
        const event = {
            name: 'PROJECTION_CREATED',
            payload: {
                projectionId: projection.projectionId,
                query: projection.query,
                partitionBy: projection.partitionBy,
                projectionGroup: this.options.projectionGroup,
                meta: projection.meta
            }
        };
        await this._addEventToStream(stream, event);
        await this._commitStream(stream);
    } catch (error) {
        console.error('error in _addProjectionStream:', projection, error);
        throw error;
    }
}

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
}

/**
 * @param {EventStream} stream the stream into which to add the projection
 * @returns {Promise<void>} returns a Promise of type void
 */
EventstoreWithProjection.prototype._commitStream = async function(stream) {
    debug('_commitStream called', stream);
    return new Promise((resolve, reject) => {
        try {
            stream.commit(function(err) {
                if (err) {
                    reject(err);
                } else {
                    resolve();
                }
            });
        } catch (error) {
            console.error('error in _commitStream: ', stream, error);
            reject(error);
        }
    });
}

/**
 * @param {AggregateQuery} query - aggregate query
 * @returns {Promise<EventStream>} - returns a stream as promise
 */
EventstoreWithProjection.prototype._getLastEventAsStreamAsync = async function(query) {
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
            console.error('error in _getLastEventAsStreamAsync: ', query, error);
            reject(error);
        }
    });
}

module.exports = EventstoreWithProjection;