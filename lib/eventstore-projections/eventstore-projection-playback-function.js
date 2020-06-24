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
 * @constructor
 */
function EventstoreProjectionPlaybackFunctions(eventstore) {
    this._eventstore = eventstore;
}


/**
 * @type {Eventstore}
 */
EventstoreProjectionPlaybackFunctions.prototype._eventstore;

/**
 * @param {String} key parameters for the projection
 * @returns {Promise<String>} - returns a Promise of type RedisLock
 */
EventstoreProjectionPlaybackFunctions.prototype.emit = async function(key) {
    try {
        debug('lock called with params:', key);

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
 * @param {String} key parameters for the projection
 * @returns {Promise<String>} - returns a Promise of type RedisLock
 */
EventstoreProjectionPlaybackFunctions.prototype.getState = async function(key) {
    try {
        debug('lock called with params:', key);

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

module.exports = EventstoreProjectionPlaybackFunctions;