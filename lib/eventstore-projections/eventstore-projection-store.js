const shortid = require('shortid');
const _ = require('lodash');
const debug = require('debug')('eventstore:projection-store')

/**
 * EventstoreProjectionStoreOptions
 * @typedef {Object} EventstoreProjectionStoreOptions
 * @property {String} projectionGroup
 */

 /**
 * EventstorePlaybackListDoneCallback
 * @callback EventstorePlaybackListDoneCallback
 * @param {Error} error The error if any
 * @param {Object} result Result of this callback
 * @returns {void} Returns void
 */

/**
 * @param {EventstoreProjectionStoreOptions} options additional options for the Eventstore projection extension
 * @constructor
 */
function EventstoreProjectionStore(options) {
    options = options || {};

    var defaults = {
        projectionGroup: 'default',
        host: '127.0.0.1',
        port: 3306,
        user: 'root',
        password: 'root',
        database: 'eventstore',
        listName: 'list_name',
        fields: [],
        secondaryKeys: {}
    };

    this.options = _.defaults(options, defaults);
}

/**
 * @returns {Promise<void>} - returns a Promise of type void
 */
EventstoreProjectionStore.prototype.init = async function() {
    const projectionGroup = this.options.projectionGroup;

    // initialize the store here
    // you can use whatever store you want here (playbacklist, redis, mysql, etc.)
    const EventstorePlaybackList = require('./eventstore-playback-list');
    const playbackList = new EventstorePlaybackList({
        host: this.options.host,
        port: this.options.port,
        database: this.options.database,
        user: this.options.user,
        password: this.options.password,
        listName: this.options.listName,
        fields: this.options.fields,
        secondaryKeys: this.options.secondaryKeys
    });
    this.list = playbackList;

    return await this.list.init();
};

/**
 * @param {String} rowId the unique id for this row
 * @param {Number} revision the row revision
 * @param {Object} data the data to add
 * @param {Object} meta optional metadata
 * @param {EventstorePlaybackListDoneCallback} cb callback to call when operation is done
 * @returns {void} - returns void. use the callback for the result (cb)
 */
EventstoreProjectionStore.prototype.add = function(rowId, revision, data, meta, cb) {
    const projectionGroup = this.options.projectionGroup;
    this.list.add(rowId, revision, data, meta, cb);
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
EventstoreProjectionStore.prototype.update = function(rowId, revision, oldData, newData, meta, cb) {
    const projectionGroup = this.options.projectionGroup;
    this.list.update(rowId, revision, oldData, newData, meta, cb);
};

/**
 * @param {String} rowId the unique id for this row
 * @param {EventstorePlaybackListDoneCallback} cb callback to call when operation is done
 * @returns {void} - returns void. use the callback for the result (cb)
 */
EventstoreProjectionStore.prototype.delete = function(rowId, cb) {
    const projectionGroup = this.options.projectionGroup;
    this.list.delete(rowId, cb);
};

/**
 * @param {String} rowId the unique id for this row
 * @param {EventstorePlaybackListDoneCallback} cb callback to call when operation is done
 * @returns {void} - returns void. use the callback for the result (cb)
 */
EventstorePlaybackList.prototype.get = function(rowId, cb) {
    const projectionGroup = this.options.projectionGroup;
    this.list.get(rowId, cb);
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
    const projectionGroup = this.options.projectionGroup;
    this.list.query(start, limit, filters, sort, cb);
};

module.exports = EventstoreProjectionStore;