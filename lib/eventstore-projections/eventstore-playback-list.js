const _ = require('lodash');
const debug = require('debug')('eventstore:playback-list');
const nodeify = require('nodeify');
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
 * @param {EventstorePlaybackListQueryDoneCallback} cb optional callback to be called when the query is done retrieving data
 * @returns {void} - returns void if cb is undefined else returns a Promise
 */
EventstorePlaybackList.prototype.query = function(start, limit, filters, sort, cb) {
    // return nodeify(this._query(start, limit, filters, sort), cb);
    if (cb) {
        this._query(start, limit, filters, sort).then((results) => cb(null, results)).catch(cb);
    } else {
        return this._query(start, limit, filters, sort);
    }
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
EventstorePlaybackList.prototype.add = function(rowId, revision, data, meta, cb) {
    // return nodeify(this._add(rowId, revision, data, meta), cb);
    if (cb) {
        this._add(rowId, revision, data, meta).then(() => cb(null)).catch(cb);
    } else {
        return this._add(rowId, revision, data, meta);
    }
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
 * @param {EventstorePlaybackListDoneCallback} cb optional callback to call when operation is done
 * @returns {void} - returns void if cb is undefined else returns a Proimse
 */
EventstorePlaybackList.prototype.update = function(rowId, revision, oldData, newData, meta, cb) {
    // return nodeify(this._update(rowId, revision, oldData, newData, meta), cb);
    if (cb) {
        this._update(rowId, revision, oldData, newData, meta).then((data) => {
            cb(null, data);
        }).catch(cb);
    } else {
        return this._update(rowId, revision, oldData, newData, meta);
    }
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
 * 
 * @param {EventstorePlaybackListFilter[]} filters 
 * @param {Object} newData 
 * @param {Object} meta 
 * @param {EventstorePlaybackListDoneCallback} cb 
 * @returns 
 */
EventstorePlaybackList.prototype.batchUpdate = function(filters, newData, meta, cb) {
    if (cb) {
        this._batchUpdate(filters, newData || {}, meta || {}).then((data) => {
            cb(null, data);
        }).catch(cb);
    } else {
        return this._batchUpdate(filters, newData, meta);
    }
};

/**
 * 
 * @param {EventstorePlaybackListFilter[]} filters 
 * @param {Object} newData 
 * @param {Object} meta 
 * @returns 
 */
EventstorePlaybackList.prototype._batchUpdate = async function(filters, newData, meta) {
    try {
        return this._store.batchUpdate(this.options.listName, filters, newData, meta);
    } catch (error) {
        console.error('error in _batchUpdate with params and error:', filters, newData, meta, error);
        throw error;
    }
};

/**
 * @param {String} rowId the unique id for this row
 * @param {EventstorePlaybackListDoneCallback} cb optional callback to call when operation is done
 * @returns {void} - returns void if cb is undefined else returns a Promise
 */
EventstorePlaybackList.prototype.delete = function(rowId, cb) {
    // return nodeify(this._delete(rowId), cb);
    if (cb) {
        this._delete(rowId).then(() => cb(null)).catch(cb);
    } else {
        return this._delete(rowId);
    }
};

/**
 * @param {String} rowId the unique id for this row
 * @returns {Promise<void>} - returns Promise of type void
 */
EventstorePlaybackList.prototype._delete = async function(rowId) {
    try {
        return this._store.delete(this.options.listName, rowId);
    } catch (error) {
        console.error('error in delete with params and error:', rowId, error);
        throw error;
    }
};


/**
 * @param {EventstorePlaybackListDoneCallback} cb optional callback to call when operation is done
 * @returns {void} - returns void if cb is undefined else returns a Promise
 */
EventstorePlaybackList.prototype.reset = function(cb) {
    // return nodeify(this._reset(), cb);
    if (cb) {
        this._reset().then(() => cb(null)).catch(cb);
    } else {
        return this._reset();
    }
};

/**
 * @returns {Promise<void>} - returns Promise of type void
 */
EventstorePlaybackList.prototype._reset = async function() {
    try {
        return this._store.truncate(this.options.listName);
    } catch (error) {
        console.error('error in _reset with params and error:', error);
        throw error;
    }
};

/**
 * @param {String} rowId the unique id for this row
 * @param {EventstorePlaybackListDoneCallback} cb optional callback to call when operation is done
 * @returns {void} - returns void if cb is undefined else returns a Promise
 */
EventstorePlaybackList.prototype.get = function(rowId, cb) {
    // return nodeify(this._get(rowId), cb);
    if (cb) {
        this._get(rowId).then((data) => cb(null, data)).catch(cb);
    } else {
        return this._get(rowId);
    }
};

/**
 * @param {String} rowId the unique id for this row
 * @param {EventstorePlaybackListDoneCallback} cb callback to call when operation is done
 * @returns {Promise<void>} - returns Promise of type void
 */
EventstorePlaybackList.prototype._get = async function(rowId) {
    try {
        return this._store.get(this.options.listName, rowId);
    } catch (error) {
        console.error('error in get with params and error:', rowId, error);
        throw error;
    }
};

module.exports = EventstorePlaybackList;