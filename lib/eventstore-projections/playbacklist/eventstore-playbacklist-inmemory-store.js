const _ = require('lodash');
const debug = require('debug')('eventstore:playback-list-redis-store');
const util = require('util');
const BaseEventStorePlaybacklistStore = require('./base.eventstore-playbacklist-store');

/**
 * @param {BaseEventStorePlaybacklistStore.EventstorePlaybackListOptions} options additional options for the Eventstore playback list
 * @constructor
 */
function EventstorePlaybackListInMemoryStore(options) {
  options = options || {};

  this.options = options;

}


util.inherits(EventstorePlaybackListInMemoryStore, BaseEventStorePlaybacklistStore);

EventstorePlaybackListInMemoryStore.prototype._lists;

/**
 * @param {BaseEventStorePlaybacklistStore.EventstorePlaybackListQueryDoneCallback} cb callback to be called when the query is done retrieving data
 * @returns {void} - returns void. use the callback for the result (cb)
 */
EventstorePlaybackListInMemoryStore.prototype.init = async function() {
    try {
        debug('init called');
        this._lists = {};
    } catch (error) {
        console.error('error in _init with error:', error);
        throw error;
    }
};


/**
 * @returns {void} - returns void Promise
 */
EventstorePlaybackListInMemoryStore.prototype.close = async function() {
    try {
        debug('close called');
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
EventstorePlaybackListInMemoryStore.prototype.createList = async function(listOptions) {
    // undefined callbacks are ok for then() and catch() because it checks for undefined functions
    this._lists[listOptions.name] = {};
    debug('createList called');
};

/**
 * @param {String} listName the name of the list
 * @returns {Promise}
 */
EventstorePlaybackListInMemoryStore.prototype.deleteList = async function(listName) {
  try {
    delete this._lists[listName];
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
EventstorePlaybackListInMemoryStore.prototype.query = async function(listName, start, limit, filters, sort) {
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
EventstorePlaybackListInMemoryStore.prototype.add = async function(listName, rowId, revision, data, meta) {
    try {
      debug('add called with params:', rowId, revision, data, meta);
      this._lists[listName][rowId] = {
          revision: revision,
          data: data ? JSON.stringify(data) : null,
          meta: meta ? JSON.stringify(meta) : null
      }
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
EventstorePlaybackListInMemoryStore.prototype.update = async function(listName, rowId, revision, data, meta) {
    try {
        debug('update called with params:', rowId, revision, data, meta);
        this._lists[listName][rowId] = {
          revision: revision,
          data: data ? JSON.stringify(data) : null,
          meta: meta ? JSON.stringify(meta) : null
        }
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
EventstorePlaybackListInMemoryStore.prototype.delete = async function(listName, rowId) {
    try {
        debug('delete called with params:', rowId);
        delete this._lists[listName][rowId];
    } catch (error) {
        console.error('error in delete with params and error:', rowId, error);
        throw error;
    }
};


 /**
  * 
  * @param {string} listName 
  */
 EventstorePlaybackListInMemoryStore.prototype.truncate = async function(listName) {
    try {
        debug('truncate called');
        this._lists[listName] = {};
    } catch (error) {
        console.error('error in truncate with params and error:', error);
        throw error;
    }
};


 /**
  * 
  * @param {string} listName 
  */
  EventstorePlaybackListInMemoryStore.prototype.destroy = async function(listName) {
    try {
        debug('destroy called');
        delete this._lists[listName];
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
EventstorePlaybackListInMemoryStore.prototype.get = async function(listName, rowId) {
    try {
        debug('get called with params:', rowId);
        const row = this._lists[listName][rowId];
        if (!_.isEmpty(row)) {
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

module.exports = EventstorePlaybackListInMemoryStore;
