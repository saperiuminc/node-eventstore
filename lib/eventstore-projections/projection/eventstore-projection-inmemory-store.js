const _ = require('lodash');
const debug = require('debug')('eventstore:projection-store');
const serialize = require('serialize-javascript');
const util = require('util');
const BaseEventstoreProjectionStore = require('./base.eventstore-projection-store');


/**
 * @param {BaseEventstoreProjectionStore.EventstoreProjectionStoreOptions} options additional options for the Eventstore projection extension
 * @constructor
 */
function EventstoreProjectionInMemoryStore(options) {
  options = options || {};
  this.options = options;

}
util.inherits(EventstoreProjectionInMemoryStore, BaseEventstoreProjectionStore);

EventstoreProjectionInMemoryStore.prototype._projections;

/**
 * @returns {Promise<void>} - returns a Promise of type void
 */
EventstoreProjectionInMemoryStore.prototype.init = async function() {
    // for reentrant.
    // dont init if init is called once
    if (!this._isInit) {
      this._projections = {}
      this._isInit = true;
    }
};


/**
 * Stores the projection if it does not exist. Ignores if exists
 * @param {import('../eventstore-projection').Projection} projection the projection to add
 * @returns {Promise<void>} - returns a Promise of type void
 */
 EventstoreProjectionInMemoryStore.prototype.updateProjection = async function(projection) {
    const projectionId = projection.projectionId;

    if (this._projections[projectionId]) {
      this._projections[projectionId].config = projection.configuration;
      this._projections[projectionId].projectionName = projection.configuration.projectionName;
    }
};

/**
 * Stores the projection if it does not exist. Ignores if exists
 * @param {import('../eventstore-projection').Projection} projection the projection to add
 * @returns {Promise<void>} - returns a Promise of type void
 */
EventstoreProjectionInMemoryStore.prototype.createProjection = async function(projection) {
    const obj = {
      projectionId: projection.projectionId,
      projectionName: projection.projectionName,
      config: projection.configuration,
      context: projection.context,
      offset: null,
      processedDate: null,
      state: 'paused'
    };
    this._projections[projection.projectionId] = obj;
    debug('createProjectionIfNotExists query', obj);
};

/**
 * Clears all data in the store
 * @returns {Promise<void>} - returns a Promise of type void
 */
EventstoreProjectionInMemoryStore.prototype.clearAll = async function() {
  this._projections = {};
};

/**
 * @param {String} projectionId the projection to id to use
 * @returns {Promise<import('../eventstore-projection').Projection>} returns the projection
 */
EventstoreProjectionInMemoryStore.prototype.getProjection = async function(projectionId) {
    const row = this._projections[projectionId];
    if(row) {
      return this._createProjectionPayload(row);
    }

    return null;
};


/**
 * @param {String} projectionName the projection to id to use
 * @returns {Promise<import('../eventstore-projection').Projection>} returns the projection
 */
 EventstoreProjectionInMemoryStore.prototype.getProjectionByName = async function(projectionName) {
  const projectionRow = _.find(this._projections, (p) => p.projectionName === projectionName);
  if (projectionRow) {
    return this._createProjectionPayload(projectionRow);
  }

  return null;
};


/**
 * @param {String} context
 * @returns {Promise<Array<import('../eventstore-projection').Projection>>} returns the projections
 */
 EventstoreProjectionInMemoryStore.prototype.getProjections = async function(context) {
  const projections = [];
  _.forEach(Object.keys(this._projections), (key) => {
    const projection = this._projections[key];
    if(projection && (!context || projection.context === context)) {
      projections.push(this._createProjectionPayload(projection));
    }
  });


  return projections;
};

/**
 * @param {String} projectionId the projectionId to update
 * @param {Number} processedDate the date when the projection is processed
 * @param {Number} newOffset optional. also sets the new offset
 * @param {Boolean} isIdle optional. also sets the new offset
 * @returns {Promise<void>} - returns a Promise of type void
 */
EventstoreProjectionInMemoryStore.prototype.setProcessed = async function(projectionId, processedDate, newOffset, isIdle) {
    const newProjection = this._projections[projectionId];
    if (newProjection) {
      if (newOffset) {
        newProjection.processedDate = processedDate;
        newProjection.offset = newOffset;
        newProjection.isIdle = isIdle == true ? 1 : 0;
      } else {
        newProjection.processedDate = processedDate;
        newProjection.isIdle = isIdle == true ? 1 : 0;
      }
  
      debug('setProcessed query', projectionId);
  
      this._projections[projectionId] = newProjection;
    }
};

/**
 * @param {String} projectionId the projectionId to update
 * @param {"running|paused|faulted"} state the projectionId state
 * @returns {Promise<void>} - returns a Promise of type void
 */
 EventstoreProjectionInMemoryStore.prototype.setState = async function(projectionId, state) {
  const newProjection = this._projections[projectionId];
  if (newProjection) {
    newProjection.state = state;
    debug('setState query', projectionId, state);
    this._projections[projectionId] = newProjection;
  }
};


/**
 * @param {String} projectionId the projectionId to update
 * @param {Error} error the error that happened
 * @param {Object} errorEvent the event that has an error
 * @param {Number} errorOffset the event that has an error
 * @returns {Promise<void>} - returns a Promise of type void
 */
 EventstoreProjectionInMemoryStore.prototype.setError = async function(projectionId, error, errorEvent, errorOffset) {
  const newProjection = this._projections[projectionId];
  if (newProjection) {
    const errorDetails = error ? JSON.stringify(error, Object.getOwnPropertyNames(error)) : null;
    newProjection.error = errorDetails;
    newProjection.errorEvent = errorEvent ? JSON.stringify(errorEvent) : null;
    newProjection.errorOffset = errorOffset || null;

    debug('setError query', projectionId);
    this._projections[projectionId] = newProjection;
  }
};


EventstoreProjectionInMemoryStore.prototype.setOffset = async function(projectionId, offset) {
  const newProjection = this._projections[projectionId];
  if (newProjection) {
    newProjection.offset = offset;

    debug('setOffset query', projectionId, offset);
    this._projections[projectionId] = newProjection;
  }
};

EventstoreProjectionInMemoryStore.prototype.delete = async function(projectionId) {
  delete this._projections[projectionId];

  debug('delete query', projectionId);
};


/**
 * @param {Object} inmemoryPayload the starting string of the keys to delete
 * @returns {import('../eventstore-projection').Projection} - returns type Projection
 */
EventstoreProjectionInMemoryStore.prototype._createProjectionPayload = function(inmemoryPayload) {
  return {
    projectionId: inmemoryPayload.projectionId,
    configuration: inmemoryPayload.config,
    context: inmemoryPayload.context,
    offset: parseInt(inmemoryPayload.offset || 0),
    processedDate: inmemoryPayload.processedDate ? parseInt(inmemoryPayload.processedDate) : null,
    error: inmemoryPayload.error || null,
    errorEvent: inmemoryPayload.errorEvent || null,
    errorOffset: inmemoryPayload.errorOffset || null,
    isIdle: inmemoryPayload.isIdle || 0,
    state: inmemoryPayload.state || null
  }
};

module.exports = EventstoreProjectionInMemoryStore;
