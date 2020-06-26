const shortid = require('shortid');
const _ = require('lodash');
const Eventstore = require('../eventstore');
const debug = require('debug')('eventstore:projection-playback-functions')

/**
 * EventstoreProjectionConfig
 * @typedef {Object} EventstoreProjectionPlaybackFunctionsOptions
 */

/**
 * @param {Eventstore} eventstore additional options for the Eventstore projection playback functions
 * @param {import('./eventstore-projection').Projection} projection the projection for the playback
 * 
 * @constructor
 */
function EventstoreProjectionPlaybackFunctions(eventstore, projection) {
    this._eventstore = eventstore;
    this._projection = projection;
}

/**
 * @type {Eventstore}
 */
EventstoreProjectionPlaybackFunctions.prototype._eventstore;

/**
 * @type {import('./eventstore-projection').Projection}
 */
EventstoreProjectionPlaybackFunctions.prototype._projection;

/**
 * @param {String} key parameters for the projection
 * @returns {Promise<String>} - returns a Promise of type RedisLock
 */
EventstoreProjectionPlaybackFunctions.prototype.emit = async function(key) {
    try {
        debug('emit called with params:', key);

        if (!key) {
            throw new Error('key is required');

        }
        const lockToken = await this._lock(key, this.options.lockTimeToLive);
        return lockToken;
    } catch (error) {
        console.error('error in lock with params and error:', key, error);
        throw error;
    }
};

/**
 * @param {import('./eventstore-projection').AggregateQuery} query query to use to get the state of a stream. if no query param 
 * is passed and just a function is passed, then it will get the state of the projection depending if you pass partitionBy "instance" or "" (projection)
 * @param {import('./eventstore-projection').DoneCallbackFunction} callback 
 * @returns {Promise<String>} - returns a Promise of type RedisLock
 */
EventstoreProjectionPlaybackFunctions.prototype.getState = function(query, callback) {
    try {
        debug('getState called with params:', query);

        if (typeof(query) === 'function') {
            callback = query;
            query = undefined;
        }

        this._getState(query).then(callback).catch(function(error) {
            if (callback) {
                callback(error);
            }
        });
    } catch (error) {
        console.error('error in getState with params and error:', query, error);
        throw error;
    }
};

/**
 * @param {import('./eventstore-projection').AggregateQuery} query query to use to get the state of a stream. if no query param 
 * is passed and just a function is passed, then it will get the state of the projection depending if you pass partitionBy "instance" or "" (projection)
 * @param {import('./eventstore-projection').DoneCallbackFunction} done 
 * @returns {Promise<String>} - returns a Promise of type RedisLock
 */
EventstoreProjectionPlaybackFunctions.prototype._getState = async function(query) {
    let targetQuery = query;
    if (!targetQuery) {
        targetQuery = this._projection.query;
    }

    const streamId = this._getProjectionStateStreamId(this._projection, targetQuery);

};

/**
 * @param {import('./eventstore-projection').Projection} projection the projection used by the playback functions
 * @param {import('./eventstore-projection').AggregateQuery} query the query to use 
 * @returns {String} - returns the streamid of the projection's state. streamid will depend on the partitionby set in the projection
 */
EventstoreProjectionPlaybackFunctions.prototype._getProjectionStateStreamId = async function(projection, query) {
    let streamId = `${projection.projectionId}-result`;
    // check partitionBy and if any event set (format '<projectionid>-result[-<context>][-<aggregate>]-<aggregateId>)
    if (projection.partitionBy === 'instance') {
        if (query.context)
            streamId += `-${query.context}`;
        if (evt.aggregate)
            streamId += `-${query.aggregate}`;
        streamId += `-${query.aggregateId || query.streamId}`;
    }
    return streamId;
};

module.exports = EventstoreProjectionPlaybackFunctions;