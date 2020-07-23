const shortid = require('shortid');
const _ = require('lodash');
const debug = require('debug')('eventstore:projection-store')

/**
 * EventstoreProjectionStoreOptions
 * @typedef {Object} EventstoreProjectionStoreOptions
 * @property {String} projectionGroup
 */

/**
 * @param {EventstoreProjectionStoreOptions} options additional options for the Eventstore projection extension
 * @constructor
 */
function EventstoreProjectionStore(options) {
    options = options || {};
    var defaults = {
        projectionGroup: 'default'
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
};

/**
 * @param {import('./eventstore-projection').Projection} projection the projection to add
 * @returns {Promise<void>} - returns a Promise of type void
 */
EventstoreProjectionStore.prototype.add = async function(projection) {
    const projectionGroup = this.options.projectionGroup;
};

/**
 * @param {import('./eventstore-projection').Projection} projection the projection to update
 * @returns {Promise<void>} - returns a Promise of type void
 */
EventstoreProjectionStore.prototype.update = async function(projection) {
    const projectionGroup = this.options.projectionGroup;
};

/**
 * @param {import('./eventstore-projection').Projection} projection the projection to delete
 * @returns {Promise<void>} - returns a Promise of type void
 */
EventstoreProjectionStore.prototype.delete = async function(projection) {
    const projectionGroup = this.options.projectionGroup;
};

/**
 * @param {String} projectionId the projectionId
 * @returns {Promise<import('./eventstore-projection').Projection>} - returns a Promise of type void
 */
EventstoreProjectionStore.prototype.get = async function(projectionId) {
    const projectionGroup = this.options.projectionGroup;
};

/**
 * @param {String} projectionGroup the projection group
 * @param {Number} start the start of the query
 * @param {Number} limit the number of items to return
 * @param {Object} filters the filters
 * @param {Object} sorting the sorting
 * @returns {Promise<Array<import('./eventstore-projection').Projection>>} - returns a Promise of type void
 */
EventstoreProjectionStore.prototype.query = async function(start, limit, filters, sorting) {
    const projectionGroup = this.options.projectionGroup;
};

module.exports = EventstoreProjectionStore;