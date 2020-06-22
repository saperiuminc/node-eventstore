const shortid = require('shortid');
const _ = require('lodash');
const debug = require('debug')('eventstore:distributed-lock')

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
    var defaults = {
        lockTimeToLive: 5000,
        acquireLockTimeout: 5000,
        releaseLockTimeout: 5000
    };

    this.options = _.defaults(options, defaults);
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
 * @param {String} key parameters for the projection
 * @returns {Promise<String>} - returns a Promise of type RedisLock
 */
DistributedLock.prototype.lock = async function(key) {
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
 * @returns {Promise<String>} - returns a Promise of lock token as string
 */
DistributedLock.prototype._lock = async function(key, lockTimeToLive) {
    return new Promise((resolve, reject) => {
        try {
            const timeoutHandle = setTimeout(() => {
                reject(new Error('timeout in _lock'));
            }, this.options.acquireLockTimeout);

            const lockToken = shortid.generate();
            this.options.redis.lock(key, lockTimeToLive, (err, lock) => {
                if (err) {
                    reject(err);
                } else {
                    this._locks[lockToken] = lock;
                    resolve(lockToken);
                }

                clearTimeout(timeoutHandle);
            });
        } catch (error) {
            console.error('error in _lock with params and error', key, lockTimeToLive);
            reject(error);
        }
    })
};

/**
 * @param {String} lockToken the lock token we got from lock()
 * @returns {void} - returns a Promise of type void
 */
DistributedLock.prototype.unlock = async function(lockToken) {
    try {
        debug('unlock called with params:', lockToken);
        if (!lockToken) {
            throw new Error('lockToken is required');
        }
        await this._unlock(lockToken);
    } catch (error) {
        console.error('error in unlock with params and error:', lockToken, error);
        throw error;
    }
};

/**
 * @param {String} lockToken the lock token we got from lock()
 * @returns {void} - returns a Promise of type void
 */
DistributedLock.prototype._unlock = async function(lockToken) {
    return new Promise((resolve, reject) => {
        try {
            const lock = this._locks[lockToken];
            if (lock) {
                const timeoutHandle = setTimeout(() => {
                    reject(new Error('timeout in _unlock'));
                }, this.options.releaseLockTimeout);
                lock.unlock((err) => {
                    debug('this.options._locks', this._locks, lockToken);
                    clearTimeout(timeoutHandle);
                    delete this._locks[lockToken];
                    if (err) {
                        reject(err);
                    } else {
                        resolve();
                    }
                });
            } else {
                resolve();
            }
        } catch (error) {
            console.error('error in _unlock with params and error', lockToken, error);
            reject(error);
        }
    })
};

module.exports = DistributedLock;