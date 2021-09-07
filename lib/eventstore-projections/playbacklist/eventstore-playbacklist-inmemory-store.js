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
    this._lists = {};
};


/**
 * @returns {void} - returns void Promise
 */
EventstorePlaybackListInMemoryStore.prototype.close = async function() {
    debug('close called');
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
    delete this._lists[listName];
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
    this._lists[listName][rowId] = {
        revision: revision,
        data: data,
        meta: meta
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
    this._lists[listName][rowId] = {
        revision: revision,
        data: data,
        meta: meta
    }
};


/**
 * @param {String} listName the name of the list
 * @param {String} rowId the unique id for this row
 * @returns {Promise<void>} - returns Promise of type void
 */
EventstorePlaybackListInMemoryStore.prototype.delete = async function(listName, rowId) {
    delete this._lists[listName][rowId];
};


 /**
  * 
  * @param {string} listName 
  */
 EventstorePlaybackListInMemoryStore.prototype.truncate = async function(listName) {
    this._lists[listName] = {};
};


 /**
  * 
  * @param {string} listName 
  */
EventstorePlaybackListInMemoryStore.prototype.destroy = async function(listName) {
    this._lists[listName] = {};
};

/**
 * @param {String} listName the name of the list
 * @param {String} rowId the unique id for this row
 * @param {BaseEventStorePlaybacklistStore.EventstorePlaybackListDoneCallback} cb callback to call when operation is done
 * @returns {Promise<void>} - returns Promise of type void
 */
EventstorePlaybackListInMemoryStore.prototype.get = async function(listName, rowId) {
    const row = this._lists[listName][rowId];
    if (row) {
        return {
        data: row.data,
        meta: row.meta,
        revision: parseInt(row.revision),
        rowId: rowId
        };
    }
    

    return null;
};

module.exports = EventstorePlaybackListInMemoryStore;
