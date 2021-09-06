const _ = require('lodash');
const debug = require('debug')('eventstore:playback-list');
const nodeify = require('nodeify');
const EventstorePlaybackList = require('./eventstore-playback-list');
const util = require('util');
// const queries = require('debug')('eventstore:show-queries');

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
 * @property {String} listName the name of this list
 */

/**
 * @param {EventstorePlaybackListOptions} options additional options for the Eventstore playback list
 * @constructor
 */
function EventstorePlaybackListQueryOnly(options, store) {
    EventstorePlaybackList.call(this, options, store);
}

util.inherits(EventstorePlaybackListQueryOnly, EventstorePlaybackList);

EventstorePlaybackListQueryOnly.prototype = Object.create(EventstorePlaybackList.prototype);
EventstorePlaybackListQueryOnly.prototype.constructor = EventstorePlaybackListQueryOnly;

/**
 * @param {Number} start zero-index start position of the rows to get
 * @param {Number} limit how many rows we want to get
 * @param {EventstorePlaybackListFilter[]} filters filter parameters for the query
 * @param {EventstorePlaybackListSort[]} sort sort parameters for the query
 * @param {EventstorePlaybackListQueryDoneCallback} cb optional callback to be called when the query is done retrieving data
 * @returns {void} - returns void if cb is undefined else returns a Promise
 */
EventstorePlaybackListQueryOnly.prototype.query = function(start, limit, filters, sort, cb) {
    return nodeify(this._query(start, limit, filters, sort), cb);
    // if (cb) {
    //     this._query(start, limit, filters, sort).then((results) => cb(null, results)).catch(cb);
    // } else {
    //     return this._query(start, limit, filters, sort);
    // }
};


/**
 * @param {Number} start zero-index start position of the rows to get
 * @param {Number} limit how many rows we want to get
 * @param {EventstorePlaybackListFilter[]} filters filter parameters for the query
 * @param {EventstorePlaybackListSort[]} sort sort parameters for the query
 * @returns {Promise<EventstorePlaybackListQueryResult} - returns void. use the callback for the result (cb)
 */
EventstorePlaybackListQueryOnly.prototype._query = async function(start, limit, filters, sort) {
    try {
        debug('query called with params:', start, limit, filters, sort);

        return this._store.query(this.options.listName, start, limit, filters, sort);
    } catch (error) {
        console.error('error in query with params and error:', start, limit, filters, sort, error);
        throw error;
    }
};

/**
 * @param {String} rowId the unique id for this row
 * @param {Number} revision the row revision
 * @param {Object} data the data to add
 * @param {Object} meta optional metadata
 * @param {EventstorePlaybackListDoneCallback} cb optional callback to call when operation is done
 * @returns {void} - returns void if cb is undefined else returns a Promise
 */
EventstorePlaybackListQueryOnly.prototype.add = function(rowId, revision, data, meta, cb) {
    throw new Error('this method is not allowed');
};

/**
 * @param {String} rowId the unique id for this row
 * @param {Number} revision the row revision
 * @param {Object} oldData Old data that we got from get
 * @param {Object} newData New data to persist
 * @param {Object} meta optional metadata
 * @param {EventstorePlaybackListDoneCallback} cb optional callback to call when operation is done
 * @returns {void} - returns void if cb is undefined else returns a Proimse
 */
EventstorePlaybackListQueryOnly.prototype.update = function(rowId, revision, oldData, newData, meta, cb) {
    throw new Error('this method is not allowed');
};

/**
 * @param {String} rowId the unique id for this row
 * @param {EventstorePlaybackListDoneCallback} cb optional callback to call when operation is done
 * @returns {void} - returns void if cb is undefined else returns a Promise
 */
EventstorePlaybackListQueryOnly.prototype.delete = function(rowId, cb) {
    throw new Error('this method is not allowed');
};

/**
 * @param {String} rowId the unique id for this row
 * @param {EventstorePlaybackListDoneCallback} cb optional callback to call when operation is done
 * @returns {void} - returns void if cb is undefined else returns a Promise
 */
 EventstorePlaybackListQueryOnly.prototype.get = function(rowId, cb) {
    throw new Error('this method is not allowed');
};

/**
 * @param {EventstorePlaybackListDoneCallback} cb optional callback to call when operation is done
 * @returns {void} - returns void if cb is undefined else returns a Promise
 */
 EventstorePlaybackListQueryOnly.prototype.reset = function(cb) {
    throw new Error('this method is not allowed');
};

module.exports = EventstorePlaybackListQueryOnly;
