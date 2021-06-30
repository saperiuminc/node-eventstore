/* eslint-disable no-unused-vars */

/**
 * EventstoreProjectionStoreOptions
 * @typedef {Object} EventstoreProjectionStoreOptions
 * @property {String} host the mysql host
 * @property {Number} port the mysql port
 * @property {String} user the mysql user
 * @property {String} password the mysql password
 * @property {String} database the mysql database name
 * @property {Number} connectionLimit the max coonnections in the pool. default is 1
 * @property {String} name the name of the store. for mysql, this is the table name
 */


/**
 * @param {EventstoreProjectionStoreOptions} options additional options for the Eventstore state list
 * @constructor
 */
function BaseEventstoreProjectionStore(options) {
    this.options = options || {};
}

/**
 * @returns {Promise<void>} - returns a Promise of type void
 */
BaseEventstoreProjectionStore.prototype.init = async function() {
    throw new Error('init not implemented');
};

/**
 * Stores the projection if it does not exist. Ignores if exists
 * @param {import('../../eventstore-projections/eventstore-projection').Projection} projection the projection to add
 * @returns {Promise<void>} - returns a Promise of type void
 */
BaseEventstoreProjectionStore.prototype.updateProjection = async function(projection) {
    throw new Error('updateProjection not implemented');
}

/**
 * Stores the projection if it does not exist. Ignores if exists
 * @param {import('../../eventstore-projections/eventstore-projection').Projection} projection the projection to add
 * @returns {Promise<void>} - returns a Promise of type void
 */
 BaseEventstoreProjectionStore.prototype.createProjection = async function(projection) {
    throw new Error('createProjection not implemented');
};

/**
 * Clears all data in the store
 * @returns {Promise<void>} - returns a Promise of type void
 */
 BaseEventstoreProjectionStore.prototype.clearAll = async function() {
    throw new Error('not implemented');
};

/**
 * @param {String} projectionId the projection to id to use
 * @returns {Promise<import('../../eventstore-projections/eventstore-projection').Projection>} returns the projection
 */
 BaseEventstoreProjectionStore.prototype.getProjection = async function(projectionId) {
    throw new Error('getProjection not implemented');
};

/**
 * @param {String} context
 * @returns {Promise<Array<import('../../eventstore-projections/eventstore-projection').Projection>>} returns the projections
 */
 BaseEventstoreProjectionStore.prototype.getProjections = async function(context) {
    throw new Error('getProjections not implemented');
};

/**
 * @param {String} projectionId the projectionId to update
 * @param {Number} processedDate the date when the projection is processed
 * @param {Number} newOffset optional. also sets the new offset
 * @param {Boolean} isIdle optional. also sets the new offset
 * @returns {Promise<void>} - returns a Promise of type void
 */
 BaseEventstoreProjectionStore.prototype.setProcessed = async function(projectionId, processedDate, newOffset, isIdle) {
    throw new Error('setProcessed not implemented');
};


/**
 * @param {String} projectionId the projectionId to update
 * @param {"running|paused|faulted"} state the projectionId state
 * @returns {Promise<void>} - returns a Promise of type void
 */
 BaseEventstoreProjectionStore.prototype.setState = async function(projectionId, state) {
    throw new Error('setState not implemented');
};

/**
 * @param {String} projectionId the projectionId to update
 * @param {Error} error the error that happened
 * @param {Object} errorEvent the event that has an error
 * @param {Number} errorOffset the event that has an error
 * @returns {Promise<void>} - returns a Promise of type void
 */
 BaseEventstoreProjectionStore.prototype.setError = async function(projectionId, error, errorEvent, errorOffset) {
    throw new Error('setError not implemented');
};

/**
 * @param {String} projectionId the projectionId to update
 * @param {Error} offset
 * @returns {Promise}
 */
 BaseEventstoreProjectionStore.prototype.setOffset = async function(projectionId, offset) {
    throw new Error('setOffset not implemented');
};

/**
 * @param {String} projectionId the projectionId to update
 * @returns {Promise}
 */

 BaseEventstoreProjectionStore.prototype.delete = async function(projectionId) {
  throw new Error('delete not implemented');
}

module.exports = BaseEventstoreProjectionStore;
