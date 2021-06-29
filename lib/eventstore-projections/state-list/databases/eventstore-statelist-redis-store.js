const _ = require('lodash');
const debug = require('debug')('eventstore:state-list');
const BaseStateListStore = require('./base.eventstore-statelist-store');
const util = require('util');
const Bluebird = require('bluebird');

/**
 * @param {EventstoreStateListOptions} options additional options for the Eventstore state list
 * @constructor
 */
function EventstoreStateListRedisStore(options) {
    options = options || {};

    if (!options.createClient) {
        throw new Error('createClient should be passed as an option');
    }

    this.options = options;

    this._configs = {};
    this._redisClient = options.createClient('client');
}

util.inherits(EventstoreStateListRedisStore, BaseStateListStore);

EventstoreStateListRedisStore.prototype._configs;

EventstoreStateListRedisStore.prototype._redisClient;

EventstoreStateListRedisStore.prototype.init = async function() {
  try {
      debug('init called');

      if (!this.options.connection.host) {
          throw new Error('host is required to be passed as part of the options');
      }

      if (!this.options.connection.port) {
          throw new Error('port is required to be passed as part of the options');
      }

      this._redisClient = this.options.createClient({
        host: this.options.connection.host,
        port: this.options.connection.port
      });
      Bluebird.promisifyAll(this._redisClient);
    } catch (error) {
      console.error('error in _init with error:', error);
      throw error;
    }
};

EventstoreStateListRedisStore.prototype.createList = async function(stateListConfig) {
    this._configs[stateListConfig.name] = stateListConfig;
}

 EventstoreStateListRedisStore.prototype.push = async function(listName, state, meta) {
    const stateListConfig = this._configs[listName];

    const lastIndex = await this._redisClient.incr(`eventstore-state-lists:${listName}:last_index`);

    const pipeline = this._redisClient.pipeline();
    if (stateListConfig.secondaryKeys) {
        for (const key in stateListConfig.secondaryKeys) {
            if (Object.prototype.hasOwnProperty.call(stateListConfig.secondaryKeys, key)) {
                const keyObject = stateListConfig.secondaryKeys[key];
                
                for (const field in keyObject) {
                    const fieldValue = state[field.name];
                    if (fieldValue) {
                        pipeline.zadd(`eventstore-state-lists:${listName}:index:${field.name}`, 0, `${fieldValue}:${lastIndex}`);
                    }
                }
            }
        }
    }
    
    pipeline.set(`eventstore-state-lists:${listName}:item:${lastIndex}`, {
        state: state,
        meta: meta
    });

    await pipeline.exec();
};

EventstoreStateListRedisStore.prototype.delete = async function(listName, index) {
  const stateListConfig = this._configs[listName];
    const pipeline = this._redisClient.pipeline();
    if (stateListConfig.secondaryKeys) {
        for (const key in stateListConfig.secondaryKeys) {
            if (Object.prototype.hasOwnProperty.call(stateListConfig.secondaryKeys, key)) {
                const keyObject = stateListConfig.secondaryKeys[key];
                
                for (const field in keyObject) {
                    pipeline.zremrangebylex(`eventstore-state-lists:${listName}:index:${field.name}`, '-', `(:${index}`);
                }
            }
        }
    }
    pipeline.delete(`eventstore-state-lists:${listName}:item:${index}`);

    await pipeline.exec();
};

EventstoreStateListRedisStore.prototype.set = async function(listName, index, state, meta) {
    const stateListConfig = this._configs[listName];

    const pipeline = this._redisClient.pipeline();
    if (stateListConfig.secondaryKeys) {
        for (const key in stateListConfig.secondaryKeys) {
            if (Object.prototype.hasOwnProperty.call(stateListConfig.secondaryKeys, key)) {
                const keyObject = stateListConfig.secondaryKeys[key];
                
                for (const field in keyObject) {
                    const fieldValue = state[field.name];
                    if (fieldValue) {
                        pipeline.zremrangebylex(`eventstore-state-lists:${listName}:index:${field.name}`, '-', `(:${index}`);
                        pipeline.zadd(`eventstore-state-lists:${listName}:index:${field.name}`, 0, `${fieldValue}:${index}`);
                    }
                }
            }
        }
    }
    
    pipeline.set(`eventstore-state-lists:${listName}:item:${index}`, {
        state: state,
        meta: meta
    });

    await pipeline.exec();
};

/**
 * @param {Number} startRowIndex the start index to get the list
 * @param {Number} limit the number of items to get
 * @param {Object} filters not yet implemented
 * @param {EventstoreStateListDoneCallback} cb the callback to call after the operation
 * @returns {void} - returns void. use the cb to get results
 */
EventstoreStateListRedisStore.prototype.find = async function(listName, lastId, filters) {
    // TODO: probably better if we use lua here to make it faster
    const redis = this._redisClient;

    let firstItem;
    if (filters.length > 1) {
        throw new Error('not yet supported multiple filters');
    }

    for (let i = 0; i < filters.length; i++) {
        const filter = filters[i];
        const results = await this._redisClient.zrangebylex(`eventstore-state-lists:${listName}:index:${filter.field}`, `[${filter.value}:`, '+', 'LIMIT', 0, 1);
        
        for (let j = 0; j < results.length; j++) {
            const item = results[j];
            const itemId = item.split(':')[1];
            firstItem = this._redisClient.get(itemId);
            return firstItem;
        }
    }
    
    return firstItem;
};

/**
 * @param {Number} startRowIndex the start index to get the list
 * @param {Number} limit the number of items to get
 * @param {Object} filters not yet implemented
 * @param {EventstoreStateListDoneCallback} cb the callback to call after the operation
 * @returns {void} - returns void. use the cb to get results
 */
EventstoreStateListRedisStore.prototype.filter = async function(listName, lastId, filters) {
    throw new Error('not implemented');
};


/**
 * @param {String} listName the name of the list
 * @returns {Promise}
 */
EventstoreStateListRedisStore.prototype.truncate = async function(listName) {
    throw new Error('not implemented');
};

/**
 * @param {String} listName the name of the list
 * @returns {Promise}
 */
EventstoreStateListRedisStore.prototype.destroy = async function(listName) {
    throw new Error('not implemented');
};

/**
 * @param {String} listName the name of the list
 * @returns {Promise}
 */
EventstoreStateListRedisStore.prototype.deleteList = async function(listName) {
    throw new Error('not implemented');
};

module.exports = EventstoreStateListRedisStore;
