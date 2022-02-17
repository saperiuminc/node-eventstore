const shortid = require('shortid');
const _ = require('lodash');
const debug = require('debug')('eventstore:distributed-lock');

/**
 * EventstoreProjectionConfig
 * @typedef {Object} DistributedLockOptions
 * @property {RedisLock} redis the redislock library
 * @property {Number} lockTimeToLive time to live of a lock
 * @property {Number} acquireLockTimeout number of milliseconds to acquire a lock before timing out and returning an error timeout
 * @property {Number} releaseLockTimeout number of milliseconds to release a lock before timing out and returning an error timeout
 */

/**
 * Redis
 * @typedef {Object} RedisLock
 * @property {Function} lock the redis library
 */

/**
 * DistributedLock
 * @typedef {Object} DistributedLock
 * @property {Function} lock pass also the lock key
 * @property {Function} unlock pass also the lock key
 */

/**
 * @param {DistributedLockOptions} options additional options for the Eventstore projection extension
 * @constructor
 */
function DistributedLock(options) {
    options = options || {};
    var defaults = {};

    const lockDefaults = {
        driftFactor: 0.01, // time in ms
        retryCount: 10,
        retryDelay: 200, // time in ms
        retryJitter: 200 // time in ms
    }

    options.lock = Object.assign(_.clone(lockDefaults), options.lock);

    this.options = Object.assign(_.clone(defaults), options);

    if (!this.options.redis) {
        throw new Error('redis parameter is required');
    }

    const redis = options.redis;

    const RedisLock = require('redlock');
    const redLock = new RedisLock([redis], options.lock);

    this._redLock = redLock;
    this._redis = redis;

    this._locks = {}
}

/**
 * @type {Object.<string, RedisLock>}
 */
DistributedLock.prototype._locks;

/**
 * @type {DistributedLockOptions}
 */
DistributedLock.prototype.options;

/**
 * @param {String} key 
 * @param {String} lockTimeToLive
 * @returns {Promise<String>} - returns a Promise of lock token as string
 */
DistributedLock.prototype.lock = async function(key, lockTimeToLive) {
    try {
        debug('lock called with params:', key);

        if (!key) {
            throw new Error('key is required');
        }

        if (!lockTimeToLive) {
            throw new Error('lockTimeToLive is required');
        }

        const lockToken = shortid.generate();
        const lock = await this._redLock.lock(key, lockTimeToLive);

        this._locks[lockToken] = lock;
        return lockToken;
    } catch (error) {
        if (error.name == 'LockError') {
            debug('error in lock with params and error', key, lockTimeToLive, error)
        } else {
            console.error('error in lock with params and error', key, lockTimeToLive, error);
        }
        
        throw error;
    }
};

/**
 * @param {String} lockToken the lock token we got from lock()
 * @returns {void} - returns a Promise of type void
 */
DistributedLock.prototype.unlock = async function(lockToken) {
    debug('unlock called with params:', lockToken);
    if (!lockToken) {
        throw new Error('lockToken is required');
    }

    try {
        const lock = this._locks[lockToken];
        if (lock) {
            await lock.unlock();
            delete this._locks[lockToken];
            return true;
        }

        return false;
    } catch (error) {
        console.error('error in unlock with params and error:', lockToken, error);
        throw error;
    }
};


/**
 * @param {String} lockToken the lock token we got from lock()
 * @returns {Promise<Boolean>} - returns a Promise of type void
 */
DistributedLock.prototype.extend = async function(lockToken, lockTimeToLive) {
    try {
        if (!lockToken) {
            throw new Error('lockToken is required');
        } 

        if (!lockTimeToLive) {
            throw new Error('lockTimeToLive is required');
        } 
        
        const lock = this._locks[lockToken];
        if (lock) {
            await lock.extend(lockTimeToLive);
            return true;
        }

        return false;
    } catch (error) {
        console.error('error in extend with params and error', lockToken, lockTimeToLive, error);
        throw error;
    }
};

module.exports = DistributedLock;