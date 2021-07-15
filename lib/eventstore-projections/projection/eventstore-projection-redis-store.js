const _ = require('lodash');
const debug = require('debug')('eventstore:projection-store');
const serialize = require('serialize-javascript');
const util = require('util');
const BaseEventstoreProjectionStore = require('./base.eventstore-projection-store');


/**
 * @param {BaseEventstoreProjectionStore.EventstoreProjectionStoreOptions} options additional options for the Eventstore projection extension
 * @constructor
 */
function EventstoreProjectionRedisStore(options) {
  options = options || {};

  if (!options.createClient) {
      throw new Error('createClient should be passed as an option');
  }

  this.options = options;

  if (!this.options.name) {
    this.options.name = 'projections'
  }

  this._configs = {};
}
util.inherits(EventstoreProjectionRedisStore, BaseEventstoreProjectionStore);

EventstoreProjectionRedisStore.prototype._configs;

EventstoreProjectionRedisStore.prototype._redisClient;

/**
 * @returns {Promise<void>} - returns a Promise of type void
 */
EventstoreProjectionRedisStore.prototype.init = async function() {
    // for reentrant.
    // dont init if init is called once
    if (!this._isInit) {
        this._redisClient = this.options.createClient('client');
        this._isInit = true;
    }
};


/**
 * Stores the projection if it does not exist. Ignores if exists
 * @param {import('../eventstore-projection').Projection} projection the projection to add
 * @returns {Promise<void>} - returns a Promise of type void
 */
 EventstoreProjectionRedisStore.prototype.updateProjection = async function(projection) {
    const config = serialize(projection.configuration);
    const pipeline = this._redisClient.pipeline();
    // change only specific fields
    pipeline.hset(`eventstore-projection-stores:${this.options.name}:projectionId:${projection.projectionId}`, `config`, config);
    pipeline.hset(`eventstore-projection-stores:${this.options.name}:projectionId:${projection.projectionId}`, `projectionName`, projection.configuration.projectionName);
    await pipeline.exec();
};

/**
 * Stores the projection if it does not exist. Ignores if exists
 * @param {import('../eventstore-projection').Projection} projection the projection to add
 * @returns {Promise<void>} - returns a Promise of type void
 */
EventstoreProjectionRedisStore.prototype.createProjection = async function(projection) {
    const config = serialize(projection.configuration);

    const pipeline = this._redisClient.pipeline();
    const itemId = `eventstore-projection-stores:${this.options.name}:projectionId:${projection.projectionId}`;
    const obj = {
      projectionId: projection.projectionId,
      projectionName: projection.projectionName,
      config: config,
      context: projection.context,
      offset: null,
      processedDate: null,
      state: 'paused'
    };
    pipeline.hmset(itemId, obj);

    // add in zrange for searching by projectionName
    pipeline.zadd(`eventstore-projection-stores:${this.options.name}:index:projectionName`, 0, `${projection.projectionName}:${projection.projectionId}`);
    // add in zrange for searching by context
    pipeline.zadd(`eventstore-projection-stores:${this.options.name}:index:context`, 0, `${projection.context}:${projection.projectionId}`);

    // add in zrange for listing
    const lastIndexForList = await this._redisClient.incr(`eventstore-projection-stores:${this.options.name}:last_index`);
    
    pipeline.zadd(`eventstore-projection-stores:${this.options.name}`, lastIndexForList, itemId);

    await pipeline.exec();

    debug('createProjectionIfNotExists query', obj);
};

/**
 * Clears all data in the store
 * @returns {Promise<void>} - returns a Promise of type void
 */
EventstoreProjectionRedisStore.prototype.clearAll = async function() {
  const pipeline = this._redisClient.pipeline();
  pipeline.del(`eventstore-projection-stores:${this.options.name}:index:projectionName:last_index`);
  pipeline.del(`eventstore-projection-stores:${this.options.name}:last_index`);
  await pipeline.exec();
  await this._delKeysWithPrefix(`eventstore-projection-stores:${this.options.name}:projectionId:`);
};

/**
 * @param {String} projectionId the projection to id to use
 * @returns {Promise<import('../eventstore-projection').Projection>} returns the projection
 */
EventstoreProjectionRedisStore.prototype.getProjection = async function(projectionId) {
    const row = await this._redisClient.hgetall(`eventstore-projection-stores:${this.options.name}:projectionId:${projectionId}`);
    if(row) {
      return this._createProjectionPayload(row);
    }

    return null;
};


/**
 * @param {String} projectionName the projection to id to use
 * @returns {Promise<import('../eventstore-projection').Projection>} returns the projection
 */
 EventstoreProjectionRedisStore.prototype.getProjectionByName = async function(projectionName) {
  const results = await this._redisClient.zrangebylex(`eventstore-projection-stores:${this.options.name}:index:projectionName`, `[${projectionName}:`, '+', 'LIMIT', 0, 1);
  
  for (let j = 0; j < results.length; j++) {
      const item = results[j];
      const projectionId = item.split(':')[1];
      const firstItem = await this._redisClient.hgetall(`eventstore-projection-stores:${this.options.name}:projectionId:${projectionId}`);
      if(firstItem) {
        return this._createProjectionPayload(firstItem);
      }
  }

  return null;
};


/**
 * @param {String} context
 * @returns {Promise<Array<import('../eventstore-projection').Projection>>} returns the projections
 */
 EventstoreProjectionRedisStore.prototype.getProjections = async function(context) {
  const projections = [];
  const results = await this._redisClient.zrangebylex(`eventstore-projection-stores:${this.options.name}:index:context`, `[${context}:`, '+', 'LIMIT', 0, -1);
  
  for (let j = 0; j < results.length; j++) {
      const item = results[j];
      const projectionId = item.split(':')[1];
      const itemResult = await this._redisClient.hgetall(`eventstore-projection-stores:${this.options.name}:projectionId:${projectionId}`);
      if(itemResult) {
        projections.push(this._createProjectionPayload(itemResult));
      }
  }


  return projections;
};

/**
 * @param {String} projectionId the projectionId to update
 * @param {Number} processedDate the date when the projection is processed
 * @param {Number} newOffset optional. also sets the new offset
 * @param {Boolean} isIdle optional. also sets the new offset
 * @returns {Promise<void>} - returns a Promise of type void
 */
EventstoreProjectionRedisStore.prototype.setProcessed = async function(projectionId, processedDate, newOffset, isIdle) {
    const pipeline = this._redisClient.pipeline();
    const itemId = `eventstore-projection-stores:${this.options.name}:projectionId:${projectionId}`; 
    if (newOffset) {
      // change only specific fields
      pipeline.hset(itemId, `processedDate`, processedDate);
      pipeline.hset(itemId, `offset`, newOffset);
      pipeline.hset(itemId, `isIdle`, isIdle == true ? 1 : 0);
    } else {
      // change only specific fields
      pipeline.hset(itemId, `processedDate`, processedDate);
      pipeline.hset(itemId, `isIdle`, isIdle == true ? 1 : 0);
    }

    debug('setProcessed query', itemId);
    await pipeline.exec();
};

/**
 * @param {String} projectionId the projectionId to update
 * @param {"running|paused|faulted"} state the projectionId state
 * @returns {Promise<void>} - returns a Promise of type void
 */
 EventstoreProjectionRedisStore.prototype.setState = async function(projectionId, state) {
  const pipeline = this._redisClient.pipeline();
  const itemId = `eventstore-projection-stores:${this.options.name}:projectionId:${projectionId}`; 
  // change only specific fields
  pipeline.hset(itemId, `state`, state);

  debug('setState query', itemId, state);
  await pipeline.exec();

};


/**
 * @param {String} projectionId the projectionId to update
 * @param {Error} error the error that happened
 * @param {Object} errorEvent the event that has an error
 * @param {Number} errorOffset the event that has an error
 * @returns {Promise<void>} - returns a Promise of type void
 */
 EventstoreProjectionRedisStore.prototype.setError = async function(projectionId, error, errorEvent, errorOffset) {

  const pipeline = this._redisClient.pipeline();
  const itemId = `eventstore-projection-stores:${this.options.name}:projectionId:${projectionId}`; 
  // change only specific fields
  const errorDetails = error ? JSON.stringify(error, Object.getOwnPropertyNames(error)) : null;
  pipeline.hset(itemId, `error`, errorDetails);
  pipeline.hset(itemId, `errorEvent`, errorEvent ? JSON.stringify(errorEvent) : null);
  pipeline.hset(itemId, `errorOffset`, errorOffset || null);
  

  debug('setError query', itemId);
  await pipeline.exec();
};


EventstoreProjectionRedisStore.prototype.setOffset = async function(projectionId, offset) {
  const pipeline = this._redisClient.pipeline();
  const itemId = `eventstore-projection-stores:${this.options.name}:projectionId:${projectionId}`; 
  // change only specific fields
  pipeline.hset(itemId, `offset`, offset);

  debug('setOffset query', itemId, offset);
  await pipeline.exec();
};

EventstoreProjectionRedisStore.prototype.delete = async function(projectionId) {

  // get projection first so we can delete it from other indexes
  const itemId = `eventstore-projection-stores:${this.options.name}:projectionId:${projectionId}`; 
  const projectionResult = await this._redisClient.hgetall(itemId);
  const pipeline = this._redisClient.pipeline();
  if (projectionResult) {
    const projection = this._createProjectionPayload(projectionResult);
    // remove from projectionName index
    pipeline.zrem(`eventstore-projection-stores:${this.options.name}:index:projectionName`, `${projectionResult.projectionName}:${projectionId}`);
    // remove from context index
    pipeline.zrem(`eventstore-projection-stores:${this.options.name}:index:context`, `${projection.context}:${projectionId}`);

  }
  pipeline.del(itemId);
  pipeline.zrem(`eventstore-projection-stores:${this.options.name}`, itemId);

  debug('delete query', itemId);
  await pipeline.exec();

};

/**
 * @param {String} prefix the starting string of the keys to delete
 * @returns {Promise<void>} - returns Promise of type void
 */
 EventstoreProjectionRedisStore.prototype._delKeysWithPrefix = async function(prefix) {
  const keysWithPrefix = this._redisClient.keys(`${prefix}*`);
  const pipeline = this._redisClient.pipeline();
  _.forEach(keysWithPrefix, (key) => {
    pipeline.del(key);
  });
  await pipeline.exec();
};


/**
 * @param {Object} redisPayload the starting string of the keys to delete
 * @returns {import('../eventstore-projection').Projection} - returns type Projection
 */
EventstoreProjectionRedisStore.prototype._createProjectionPayload = function(redisPayload) {
  const projectionConfig = eval('(' + redisPayload.config + ')');
  return {
    projectionId: redisPayload.projectionId,
    configuration: projectionConfig,
    context: redisPayload.context,
    offset: parseInt(redisPayload.offset || 0),
    processedDate: redisPayload.processedDate ? parseInt(redisPayload.processedDate) : null,
    error: redisPayload.error || null,
    errorEvent: redisPayload.errorEvent || null,
    errorOffset: redisPayload.errorOffset || null,
    isIdle: parseInt(redisPayload.isIdle || 0),
    state: redisPayload.state || null
  }
};

module.exports = EventstoreProjectionRedisStore;
