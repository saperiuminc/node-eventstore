const _ = require('lodash');
const debug = require('debug')('eventstore:playback-list-redis-store');
const util = require('util');
const BaseEventStorePlaybacklistStore = require('./base.eventstore-playbacklist-store');

/**
 * @param {BaseEventStorePlaybacklistStore.EventstorePlaybackListOptions} options additional options for the Eventstore playback list
 * @constructor
 */
function EventstorePlaybackListRedisStore(options) {
  options = options || {};

  if (!options.createClient) {
      throw new Error('createClient should be passed as an option');
  }

  this.options = options;

  this._configs = {};
}


util.inherits(EventstorePlaybackListRedisStore, BaseEventStorePlaybacklistStore);

EventstorePlaybackListRedisStore.prototype._configs;

EventstorePlaybackListRedisStore.prototype._redisClient;

/**
 * @param {BaseEventStorePlaybacklistStore.EventstorePlaybackListQueryDoneCallback} cb callback to be called when the query is done retrieving data
 * @returns {void} - returns void. use the callback for the result (cb)
 */
EventstorePlaybackListRedisStore.prototype.init = async function() {
    try {
        debug('init called');
        this._redisClient = this.options.createClient('client');
    } catch (error) {
        console.error('error in _init with error:', error);
        throw error;
    }
};


/**
 * @returns {void} - returns void Promise
 */
EventstorePlaybackListRedisStore.prototype.close = async function() {
    try {
        debug('close called');
        // TODO ioredis check end function
        await this._redisClient.quit();
    } catch (error) {
        console.error('error in close with error:', error);
        throw error;
    }
};

/**
 * @param {Object} listOptions varies depending on the need of the store. implementors outside of the library can send any options to the playbacklist
 * and everything will just be forwarded to the createList as part of the listOptions
 * @returns {void} - returns void. use the callback for the result (cb)
 */
// eslint-disable-next-line no-unused-vars
EventstorePlaybackListRedisStore.prototype.createList = async function(listOptions) {
    // undefined callbacks are ok for then() and catch() because it checks for undefined functions
    debug('createList called. no need to create list for redis');
};

/**
 * @param {String} listName the name of the list
 * @returns {Promise}
 */
EventstorePlaybackListRedisStore.prototype.deleteList = async function(listName) {
  try {
    await this._delKeysWithPrefix(`eventstore-playback-lists:${listName}:rowId:`);
  } catch (error) {
    console.error('error in deleteList with params and error:', listName, error);
    throw error;
  }
};

/**
 * @param {String} listName the name of the list
 * @param {Number} start zero-index start position of the rows to get
 * @param {Number} limit how many rows we want to get
 * @param {BaseEventStorePlaybacklistStore.EventstorePlaybackListFilter[]} filters filter parameters for the query
 * @param {BaseEventStorePlaybacklistStore.EventstorePlaybackListSort[]} sort sort parameters for the query
 * @returns {Promise<EventstorePlaybackListQueryResult} - returns void. use the callback for the result (cb)
 */
// eslint-disable-next-line no-unused-vars
EventstorePlaybackListRedisStore.prototype.query = async function(listName, start, limit, filters, sort) {
    throw new Error('not implemented!');
};


/**
 * @param {String} listName the name of the list
 * @param {String} rowId the unique id for this row
 * @param {Number} revision the row revision
 * @param {Object} data the data to add
 * @param {Object} meta optional metadata
 * @returns {Promise<void>} - returns Promise of type void
 */
EventstorePlaybackListRedisStore.prototype.add = async function(listName, rowId, revision, data, meta) {
    try {
      debug('add called with params:', rowId, revision, data, meta);

      const pipeline = this._redisClient.pipeline();
      const itemId = `eventstore-playback-lists:${listName}:rowId:${rowId}`;
      pipeline.hmset(itemId, {
      revision: revision,
        data: data ? JSON.stringify(data) : null,
        meta: meta ? JSON.stringify(meta) : null
      });
      await pipeline.exec();
    } catch (error) {
        console.error('error in add with params and error:', rowId, revision, data, meta, error);
        throw error;
    }
};


/**
 * @param {String} listName the name of the list
 * @param {String} rowId the unique id for this row
 * @param {Number} revision the row revision
 * @param {Object} oldData Old data that we got from get
 * @param {Object} newData New data to persist
 * @param {Object} meta optional metadata
 * @returns {Promise<void>} - returns Promise of type void
 */
EventstorePlaybackListRedisStore.prototype.update = async function(listName, rowId, revision, data, meta) {
    try {
        debug('update called with params:', rowId, revision, data, meta);
        const pipeline = this._redisClient.pipeline();
        const itemId = `eventstore-playback-lists:${listName}:rowId:${rowId}`;
        pipeline.hmset(itemId, {
          revision: revision,
          data: data ? JSON.stringify(data) : null,
          meta: meta ? JSON.stringify(meta) : null
        });
        await pipeline.exec();
    } catch (error) {
        console.error('error in update with params and error:', rowId, revision, data, meta, error);
        throw error;
    }
};


/**
 * @param {String} listName the name of the list
 * @param {String} rowId the unique id for this row
 * @returns {Promise<void>} - returns Promise of type void
 */
EventstorePlaybackListRedisStore.prototype.delete = async function(listName, rowId) {
    try {
        debug('delete called with params:', rowId);
        const pipeline = this._redisClient.pipeline();
        pipeline.del(`eventstore-playback-lists:${listName}:rowId:${rowId}`);
        await pipeline.exec();
    } catch (error) {
        console.error('error in delete with params and error:', rowId, error);
        throw error;
    }
};


 /**
  * 
  * @param {string} listName 
  */
 EventstorePlaybackListRedisStore.prototype.truncate = async function(listName) {
    try {
        debug('truncate called');
        await this._delKeysWithPrefix(`eventstore-playback-lists:${listName}:rowId:`);
    } catch (error) {
        console.error('error in truncate with params and error:', error);
        throw error;
    }
};


 /**
  * 
  * @param {string} listName 
  */
  EventstorePlaybackListRedisStore.prototype.destroy = async function(listName) {
    try {
        debug('destroy called');
        await this._delKeysWithPrefix(`eventstore-playback-lists:${listName}:rowId:`);
    } catch (error) {
        console.error('error in destroy with params and error:', error);
        throw error;
    }
};

/**
 * @param {String} listName the name of the list
 * @param {String} rowId the unique id for this row
 * @param {BaseEventStorePlaybacklistStore.EventstorePlaybackListDoneCallback} cb callback to call when operation is done
 * @returns {Promise<void>} - returns Promise of type void
 */
EventstorePlaybackListRedisStore.prototype.get = async function(listName, rowId) {
    try {
        debug('get called with params:', rowId);
        const row = await this._redisClient.hgetall(`eventstore-playback-lists:${listName}:rowId:${rowId}`);
        if (row) {
          return {
            data: row.data ? JSON.parse(row.data) : null,
            meta: row.meta ? JSON.parse(row.meta) : null,
            revision: parseInt(row.revision),
            rowId: rowId
          };
        }
        

        return null;
    } catch (error) {
        console.error('error in get with params and error:', rowId, error);
        throw error;
    }
};

/**
 * @param {String} prefix the starting string of the keys to delete
 * @returns {Promise<void>} - returns Promise of type void
 */
EventstorePlaybackListRedisStore.prototype._delKeysWithPrefix = async function(prefix) {
  const keysWithPrefix = this._redisClient.keys(`${prefix}*`);
  const pipeline = this._redisClient.pipeline();
  _.forEach(keysWithPrefix, (key) => {
    pipeline.del(key);
  });
  await pipeline.exec();
};

module.exports = EventstorePlaybackListRedisStore;
