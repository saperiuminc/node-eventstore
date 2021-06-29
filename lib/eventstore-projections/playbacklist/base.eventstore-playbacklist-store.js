/* eslint-disable no-unused-vars */
/**
 * EventstorePlaybackListQueryResultRow
 * @typedef {Object} EventstorePlaybackListQueryResultRow
 * @property {String} rowId The uniqueid of this row
 * @property {Number} revision The row revision
 * @property {Object} data The data at this revision
 * @property {Object} meta Some user metadata
 */

/**
 * EventstorePlaybackListQueryResult
 * @typedef {Object} EventstorePlaybackListQueryResult
 * @property {Number} count The total number of rows in the result
 * @property {EventstorePlaybackListQueryResultRow[]} rows The rows in the query
 */

/**
 * EventstorePlaybackListQueryDoneCallback
 * @callback EventstorePlaybackListQueryDoneCallback
 * @param {Error} error The event to playback
 * @param {EventstorePlaybackListQueryResult} result Callback to tell that playback is done consuming the event
 * @returns {void} Returns void
 */

/**
 * EventstorePlaybackListDoneCallback
 * @callback EventstorePlaybackListDoneCallback
 * @param {Error} error The error if any
 * @param {Object} result Result of this callback
 * @returns {void} Returns void
 */


 /**
 * EventstorePlaybackListFilter
 * @typedef {Object} EventstorePlaybackListFilter
 * @property {String} field the field to filter
 * @property {String?} group the group of the filter
 * @property {("or"|"and"|null)} groupBooleanOperator the operator for the group
 * @property {("is"|"any"|"range"|"dateRange"|"contains"|"arrayContains"|"startsWith"|"endsWith"|"exists"|"notExists")} operator The operator to use. 
 * @property {Object?} value The value of the field. valid only for "is", "any" and "contains" operators
 * @property {Object?} from The lower limit value of the field. valid only for "range" operator
 * @property {Object?} to The upper limit value of the field. valid only for "range" operator
 */

/**
 * EventstorePlaybackListSort
 * @typedef {Object} EventstorePlaybackListSort
 * @property {String} field the field to sort
 * @property {("ASC"|"DESC")} sort the direction to sort
 */

/**
 * EventstorePlaybackListSecondaryKey
 * @typedef {Object} EventstorePlaybackListSecondaryKey
 * @property {String} name The name of the field
 * @property {("ASC"|"DESC")} sort The sort directioon of the key. Default is ASC
 */

/**
 * EventstorePlaybackListField
 * @typedef {Object} EventstorePlaybackListField
 * @property {String} type The type of the field
 * @property {String} name The field name
 */

/**
 * EventstorePlaybackListOptions
 * @typedef {Object} EventstorePlaybackListOptions
 * @property {Object} mysql the mysql library
 * @property {String} host the mysql host
 * @property {Number} port the mysql port
 * @property {String} user the mysql user
 * @property {String} password the mysql password
 * @property {String} database the mysql database name
 * @property {Number} connectionLimit the max coonnections in the pool. default is 1
 */



/**
 * @param {EventstorePlaybackListOptions} options additional options for the Eventstore playback list
 * @constructor
 */
const BaseEventStorePlaybacklistStore = function(options) {
    if (this.constructor === BaseEventStorePlaybacklistStore) {
      throw new Error('Cant instantiate abstract class!');
    }
    this.options = options;

    return this;
};


/**
 * @type {EventstorePlaybackListOptions}
 */
BaseEventStorePlaybacklistStore.prototype.options;


BaseEventStorePlaybacklistStore.prototype = {
  /**
 * @param {EventstorePlaybackListQueryDoneCallback} cb callback to be called when the query is done retrieving data
 * @returns {void} - returns void. use the callback for the result (cb)
 */
  init: async function() {
    throw new Error('init must be implemented!');
  },
  /**
   * @returns {void} - returns void Promise
   */
  close: async function() {
    throw new Error('close must be implemented!');
  },
  /**
   * @param {Object} listOptions varies depending on the need of the store. implementors outside of the library can send any options to the playbacklist
   * and everything will just be forwarded to the createList as part of the listOptions
   * @returns {void} - returns void. use the callback for the result (cb)
  */
  createList: async function(listOptions) {
    throw new Error('createList must be implemented!');
  },
  /**
   * @param {String} listName the name of the list
   * @returns {Promise}
   */
  deleteList: async function(listName) {
    throw new Error('deleteList must be implemented!');
  },
  /**
   * @param {String} listName the name of the list
   * @param {Number} start zero-index start position of the rows to get
   * @param {Number} limit how many rows we want to get
   * @param {EventstorePlaybackListFilter[]} filters filter parameters for the query
   * @param {EventstorePlaybackListSort[]} sort sort parameters for the query
   * @returns {Promise<EventstorePlaybackListQueryResult} - returns void. use the callback for the result (cb)
  */
  query: async function(listName, start, limit, filters, sort) {
    throw new Error('query must be implemented!');
  },
  /**
   * @param {String} listName the name of the list
   * @param {String} rowId the unique id for this row
   * @param {Number} revision the row revision
   * @param {Object} data the data to add
   * @param {Object} meta optional metadata
   * @returns {Promise<void>} - returns Promise of type void
  */
  add: async function(listName, rowId, revision, data, meta) {
    throw new Error('add must be implemented!');
  },
  /**
   * @param {String} listName the name of the list
   * @param {String} rowId the unique id for this row
   * @param {Number} revision the row revision
   * @param {Object} oldData Old data that we got from get
   * @param {Object} newData New data to persist
   * @param {Object} meta optional metadata
   * @returns {Promise<void>} - returns Promise of type void
   */
  update: async function(listName, rowId, revision, data, meta) {
    throw new Error('update must be implemented!');
  },
  /**
   * @param {String} listName the name of the list
   * @param {String} rowId the unique id for this row
   * @returns {Promise<void>} - returns Promise of type void
  */
  delete: async function(listName, rowId) {
    throw new Error('delete must be implemented!');
  },
  /**
  * 
  * @param {string} listName 
  */
  truncate: async function(listName) {
    throw new Error('truncate must be implemented!');
  },
  /**
  * 
  * @param {string} listName 
  */
   destroy: async function(listName) {
    throw new Error('destroy must be implemented!');
  },
  /**
   * @param {String} listName the name of the list
   * @param {String} rowId the unique id for this row
   * @param {EventstorePlaybackListDoneCallback} cb callback to call when operation is done
   * @returns {Promise<void>} - returns Promise of type void
  */
  get: async function(listName, rowId) {
    throw new Error('get must be implemented!');
  }

};

module.exports = BaseEventStorePlaybacklistStore;
