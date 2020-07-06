const shortid = require('shortid');
const _ = require('lodash');
const debug = require('debug')('eventstore:playback-list')

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
 * EventstorePlaybackListFilters
 * @typedef {Object} EventstorePlaybackListFilters
 */

 /**
 * EventstorePlaybackListSort
 * @typedef {Object} EventstorePlaybackListSort
 */

/**
 * EventstorePlaybackListOptions
 * @typedef {Object} EventstorePlaybackListOptions
 * @property {String} host the mysql host
 * @property {Number} port the mysql port
 * @property {String} user the mysql user
 * @property {String} password the mysql password
 * @property {String} database the mysql database name
 */

/**
 * @param {EventstorePlaybackListOptions} options additional options for the Eventstore playback list
 * @constructor
 */
function EventstorePlaybackList(options) {
    options = options || {};
    var defaults = {
        host: '127.0.0.1',
        port: 3306,
        user: 'root',
        password: 'root',
        database: 'eventstore',
    };

    this.options = _.defaults(options, defaults);
}

/**
 * @type {EventstorePlaybackListOptions}
 */
EventstorePlaybackList.prototype.options;

/**
 * @param {Number} start zero-index start position of the rows to get
 * @param {Number} limit how many rows we want to get
 * @param {EventstorePlaybackListFilters} filters filter parameters for the query
 * @param {EventstorePlaybackListSort} sort sort parameters for the query
 * @param {EventstorePlaybackListQueryDoneCallback} cb callback to be called when the query is done retrieving data
 * @returns {void} - returns void. use the callback for the result (cb)
 */
EventstorePlaybackList.prototype.query = function(start, limit, filters, sort, cb) {
    try {
        debug('query called with params:', start, limit, filters, sort);

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
 * @param {EventstorePlaybackListDoneCallback} cb callback to call when operation is done
 * @returns {void} - returns void. use the callback for the result (cb)
 */
EventstorePlaybackList.prototype.add = function(rowId, revision, data, meta, cb) {
    try {
        debug('add called with params:', rowId, revision, data, meta);
    } catch (error) {
        console.error('error in add with params and error:', rowId, revision, data, meta, error);
        throw error;
    }
};

/**
 * @param {String} rowId the unique id for this row
 * @param {Number} revision the row revision
 * @param {Object} oldData Old data that we got from get
 * @param {Object} newData New data to persist
 * @param {Object} meta optional metadata
 * @param {EventstorePlaybackListDoneCallback} cb callback to call when operation is done
 * @returns {void} - returns void. use the callback for the result (cb)
 */
EventstorePlaybackList.prototype.update = function(rowId, revision, oldData, newData, meta, cb) {
    try {
        debug('update called with params:', rowId, revision, data, meta);
    } catch (error) {
        console.error('error in update with params and error:', rowId, revision, oldData, newData, meta, error);
        throw error;
    }
};

/**
 * @param {String} rowId the unique id for this row
 * @param {EventstorePlaybackListDoneCallback} cb callback to call when operation is done
 * @returns {void} - returns void. use the callback for the result (cb)
 */
EventstorePlaybackList.prototype.delete = function(rowId, cb) {
    try {
        debug('delete called with params:', rowId);
    } catch (error) {
        console.error('error in delete with params and error:', rowId, error);
        throw error;
    }
};

/**
 * @param {String} rowId the unique id for this row
 * @param {EventstorePlaybackListDoneCallback} cb callback to call when operation is done
 * @returns {void} - returns void. use the callback for the result (cb)
 */
EventstorePlaybackList.prototype.get = function(start, limit, filters, sort, cb) {
    try {
        debug('delete called with params:', rowId);
    } catch (error) {
        console.error('error in delete with params and error:', rowId, error);
        throw error;
    }
};

module.exports = EventstorePlaybackList;