const _ = require('lodash');
// const debug = require('debug')('eventstore:distributed-signal');
const Waitables = require('@saperiuminc/waitables');

/**
 * EventstoreProjectionConfig
 * @typedef {Object} DistributedLockOptions
 * @property {RedisLock} redis the redislock library
 * @property {Number} ttlDuration time to live of a lock
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
function DistributedSignal(options) {
    options = options || {};

    if (!options.createClient) {
        throw new Error('createClient should be passed as an option');
    }

    var defaults = {};

    this.options = Object.assign(defaults, options);

    this._groupConsumers = {};
    this._groupProducers = {};
    this._fanoutSignalProducer = null;
    this._fanoutWaitableConsumer = null;
    this._ttlDuration = options.ttlDuration;
    this._processingTimeout = options.processingTimeout;
}

/**
 * @type {DistributedLockOptions}
 */
DistributedSignal.prototype.options;


/**
 * @param {String} topic 
 * @param {String} group
 * @param {Function} onSignal
 * @returns {Promise<String>} - returns a Promise of lock token as string
 */
 DistributedSignal.prototype.groupSubscribe = async function(topic, group, pollingTimeout, onSignal) {
    try {
        if (!topic) {
            throw new Error('topic is required');
        }

        if (!group) {
            throw new Error('group is required');
        }

        let consumer = this._getGroupedWaitableConsumer(group);
        const groupedWaitable = await consumer.waitForSignal(topic);
        groupedWaitable.setTime(pollingTimeout);
        groupedWaitable.waitRecursive(onSignal);
        return groupedWaitable.id;
    } catch (error) {
        console.error('error in waitForSignal with params and error', topic, error);
        throw error;
    }
};

/**
 * @param {String} topic 
 * @param {Function} onSignal
 * @returns {Promise<String>} - returns a Promise of lock token as string
 */
DistributedSignal.prototype.subscribe = async function(topic, onSignal) {
    try {
        if (!topic) {
            throw new Error('topic is required');
        }

        let consumer = this._getFanoutWaitableConsumer();
        const waitable = await consumer.waitForSignal(topic);
        waitable.waitRecursive(onSignal);
        return waitable.id;
    } catch (error) {
        console.error('error in waitForSignal with params and error', topic, error);
        throw error;
    }
};

/**
 * @param {String} topic
 * @returns {Promise<String>} - returns a Promise of lock token as string
 */
 DistributedSignal.prototype.subscribeWithWaitable = async function(topic) {
  try {
      if (!topic) {
          throw new Error('topic is required');
      }

      let consumer = this._getFanoutWaitableConsumer();
      const waitable = await consumer.waitForSignal(topic);
      return waitable;
  } catch (error) {
      console.error('error in waitForSignal with params and error', topic, error);
      throw error;
  }
};

/**
 * @param {String} waitableId 
 * @param {Function} group
 * @returns {Promise<String>} - returns a Promise of lock token as string
 */
DistributedSignal.prototype.unsubscribe = async function(waitableId, group) {
    try {
        if (!waitableId) {
            throw new Error('waitableId is required');
        }

        let consumer = group ? this._getGroupedWaitableConsumer(group) : this._getFanoutWaitableConsumer();
        await consumer.stopWaitingForSignal(waitableId);
    } catch (error) {
        console.error('error in stopWaitingForSignal with params and error', waitableId, group, error);
        throw error;
    }
};


/**
 * @param {String} waitableId 
 * @param {Function} group
 * @returns {Promise<String>} - returns a Promise of lock token as string
 */
 DistributedSignal.prototype.groupUnsubscribe = async function(waitableId, group) {
    try {
        if (!waitableId) {
            throw new Error('waitableId is required');
        }

        if (!group) {
            throw new Error('waitableId is required');
        }

        let consumer = this._getGroupedWaitableConsumer(group);
        await consumer.stopWaitingForSignal(waitableId);
    } catch (error) {
        console.error('error in stopWaitingForSignal with params and error', waitableId, group, error);
        throw error;
    }
};


/**
 * @param {String} waitableId 
 * @param {Function} group
 * @returns {Promise<String>} - returns a Promise of lock token as string
 */
 DistributedSignal.prototype.unsubscribe = async function(waitableId) {
    try {
        if (!waitableId) {
            throw new Error('waitableId is required');
        }

        let consumer = this._getFanoutWaitableConsumer();
        await consumer.stopWaitingForSignal(waitableId);
    } catch (error) {
        console.error('error in stopWaitingForSignal with params and error', waitableId, error);
        throw error;
    }
};

/**
 * @param {String} topic 
 * @returns {Promise<String>} - returns a Promise of lock token as string
 */
DistributedSignal.prototype.signal = async function(topic, group) {
    try {
        if (!topic) {
            throw new Error('topic is required');
        }

        const producer = group ? this._getGroupedSignalProducer(group) : this._getFanoutSignalProducer();
        await producer.signal(topic);
    } catch (error) {
        console.error('error in waitForSignal with params and error', topic, error);
        throw error;
    }
};

DistributedSignal.prototype._getGroupedWaitableConsumer = function(groupId) {
    const self = this;
    if (!this._groupConsumers[groupId]) {
        // initialize waitables library
        this._groupConsumers[groupId] = new Waitables.GroupedWaitableConsumer({
            groupId: groupId,
            ttlDuration: this._ttlDuration,
            processingTimeout: this._processingTimeout,
            createClient: function(type) {
                return self.options.createClient(type);
            }
        });
    }

    return this._groupConsumers[groupId];
}


DistributedSignal.prototype._getGroupedSignalProducer = function(groupId) {
    const self = this;
    if (!this._groupProducers[groupId]) {
        // initialize waitables library
        this._groupProducers[groupId] = new Waitables.GroupedSignalProducer({
            createClient: function(type) {
                return self.options.createClient(type);
            },
            groupId: groupId
        })
    }

    return this._groupProducers[groupId];
}

DistributedSignal.prototype._getFanoutSignalProducer = function() {
    const self = this;
    if (!this._fanoutSignalProducer) {
        // initialize waitables library
        this._fanoutSignalProducer = new Waitables.FanoutSignalProducer({
            createClient: function(type) {
                return self.options.createClient(type);
            }
        });
    }

    return this._fanoutSignalProducer;
}

DistributedSignal.prototype._getFanoutWaitableConsumer = function() {
    const self = this;
    if (!this._fanoutWaitableConsumer) {
        // initialize waitables library
        this._fanoutWaitableConsumer = new Waitables.FanoutWaitableConsumer({
            createClient: function(type) {
                return self.options.createClient(type);
            }
        });
    }

    return this._fanoutWaitableConsumer;
}



module.exports = DistributedSignal;
