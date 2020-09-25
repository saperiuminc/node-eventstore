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
 * EventstorePlaybackListFilter
 * @typedef {Object} EventstorePlaybackListFilter
 * @property {String} field the field to filter
 * @property {String?} group the group of the filter
 * @property {("or"|"and"|null)} groupBooleanOperator the operator for the group
 * @property {("is"|"any"|"range"|"contains"|"arrayContains")} operator The operator to use. 
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
 * @property {String} listName the name of this list
 * @property {Object.<string, EventstorePlaybackListSecondaryKey[]>} secondaryKeys the secondary keys that make up the non clustered index. A key value pair were key is the secondaryKey name and value an array of fields
 * @property {EventstorePlaybackListField[]} fields the secondary keys that make up the non clustered index
 */

/**
 * @param {EventstorePlaybackListOptions} options additional options for the Eventstore playback list
 * @constructor
 */
function EventstorePlaybackList(options, store) {
    options = options || {};

    // NOTE: add options as necessary in the future
    var defaults = {
        listName: 'list_default'
    };

    this.options = _.defaults(options, defaults);

    this._store = store;
}

/**
 * @type {EventstorePlaybackListOptions}
 */
EventstorePlaybackList.prototype.options;

EventstorePlaybackList.prototype._pool;

/**
 * @param {Object} listOptions options to create the list. this can vary per store that is why the paramerter can be any
 * @returns {void} - returns void. use the callback for the result (cb)
 */
EventstorePlaybackList.prototype.createList = async function(listOptions) {
    // undefined callbacks are ok for then() and catch() because it checks for undefined functions
    try {
        debug('createList called');
        await this._store.createList(listOptions);
    } catch (error) {
        console.error('error in _init with error:', error);
        throw error;
    }
};


/**
 * @param {Number} start zero-index start position of the rows to get
 * @param {Number} limit how many rows we want to get
 * @param {EventstorePlaybackListFilter[]} filters filter parameters for the query
 * @param {EventstorePlaybackListSort[]} sort sort parameters for the query
 * @param {EventstorePlaybackListQueryDoneCallback} cb callback to be called when the query is done retrieving data
 * @returns {void} - returns void. use the callback for the result (cb)
 */
EventstorePlaybackList.prototype.query = function(start, limit, filters, sort, cb) {
    this._query(start, limit, filters, sort).then((results) => cb(null, results)).catch(cb);
};


/**
 * @param {Number} start zero-index start position of the rows to get
 * @param {Number} limit how many rows we want to get
 * @param {EventstorePlaybackListFilter[]} filters filter parameters for the query
 * @param {EventstorePlaybackListSort[]} sort sort parameters for the query
 * @returns {Promise<EventstorePlaybackListQueryResult} - returns void. use the callback for the result (cb)
 */
EventstorePlaybackList.prototype._query = async function(start, limit, filters, sort) {
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
 * @param {EventstorePlaybackListDoneCallback} cb callback to call when operation is done
 * @returns {void} - returns void. use the callback for the result (cb)
 */
EventstorePlaybackList.prototype.add = function(rowId, revision, data, meta, cb) {
    this._add(rowId, revision, data, meta).then(cb).catch(cb);
};

/**
 * @param {String} rowId the unique id for this row
 * @param {Number} revision the row revision
 * @param {Object} data the data to add
 * @param {Object} meta optional metadata
 * @returns {Promise<void>} - returns Promise of type void
 */
EventstorePlaybackList.prototype._add = async function(rowId, revision, data, meta) {
    try {
        debug('add called with params:', rowId, revision, data, meta);

        const newData = _.clone(data);
        _.forOwn(newData, (value, key) => {
            if (_.isNull(value) || _.isUndefined(value)) {
                delete newData[key];
            }
        })

        return this._store.add(this.options.listName, rowId, revision, newData, meta);
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
    this._update(rowId, revision, oldData, newData, meta).then(cb).catch(cb);
};

/**
 * @param {String} rowId the unique id for this row
 * @param {Number} revision the row revision
 * @param {Object} oldData Old data that we got from get
 * @param {Object} newData New data to persist
 * @param {Object} meta optional metadata
 * @returns {Promise<void>} - returns Promise of type void
 */
EventstorePlaybackList.prototype._update = async function(rowId, revision, oldData, newData, meta) {
    try {
        debug('update called with params:', rowId, revision, data, meta);

        var data = Object.assign(oldData, newData);
        
        const toAddData = _.clone(data);
        _.forOwn(toAddData, (value, key) => {
            if (_.isNull(value) || _.isUndefined(value)) {
                delete toAddData[key];
            }
        })

        return this._store.update(this.options.listName, rowId, revision, toAddData, meta);

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
    this._delete(rowId).then(cb).catch(cb);
};

/**
 * @param {String} rowId the unique id for this row
 * @returns {Promise<void>} - returns Promise of type void
 */
EventstorePlaybackList.prototype._delete = async function(rowId) {
    try {
        debug('delete called with params:', rowId);
        return this._store.delete(this.options.listName, rowId);
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
EventstorePlaybackList.prototype.get = function(rowId, cb) {
    this._get(rowId).then((item) => {
        cb(null, item);
    }).catch(cb);
};

/**
 * @param {String} rowId the unique id for this row
 * @param {EventstorePlaybackListDoneCallback} cb callback to call when operation is done
 * @returns {Promise<void>} - returns Promise of type void
 */
EventstorePlaybackList.prototype._get = async function(rowId) {
    try {
        debug('get called with params:', rowId);

        return this._store.get(this.options.listName, rowId);
    } catch (error) {
        console.error('error in get with params and error:', rowId, error);
        throw error;
    }
};

module.exports = EventstorePlaybackList;
