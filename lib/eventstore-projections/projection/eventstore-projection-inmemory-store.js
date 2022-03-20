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
      this._projections = {};
      this._projectionTasks = {};
      this._projectionCheckpoint = {};
      this._isInit = true;
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
* Stores the projection task if it does not exist. Ignores if exists
* @param {import('../../eventstore-projections/eventstore-projection').ProjectionTask} projectionTask the projection task to add
* @returns {Promise<void>} - returns a Promise of type void
*/
EventstoreProjectionInMemoryStore.prototype.createProjectionTask = async function (projectionTask) {
    const obj = {
        projectionTaskId: projectionTask.projectionTaskId,
        projectionId: projectionTask.projectionId,
        shard: projectionTask.shard,
        partition: projectionTask.partition,
        offset: projectionTask.offset,
        processedDate: null
    };
    this._projectionTasks[projectionTask.projectionTaskId] = obj;
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
 * Clears all data in the store
 * @returns {Promise<void>} - returns a Promise of type void
 */
EventstoreProjectionInMemoryStore.prototype.clearAll = async function() {
  this._projections = {};
  this._projectionTasks = {};
  this._projectionCheckpoint = {};
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
* Gets a specific projection task given a projection task id
* @param {String} projectionTaskId the projection task id to use
* @returns {Promise<import('../../eventstore-projections/eventstore-projection').ProjectionTask>} returns the projection task
*/
EventstoreProjectionInMemoryStore.prototype.getProjectionTask = async function (projectionTaskId) {
  const projectionTask = this._projectionTasks[projectionTaskId];
  if (projectionTask) {
    const projection = this._projections[projectionTask.projectionId];
    if (projection) {
      const obj = {
        projectionTaskId: projectionTask.projectionTaskId,
        projectionId: projectionTask.projectionId,
        shard: projectionTask.shard,
        partition: projectionTask.partition,
        offset: projectionTask.offset,
        processedDate: projectionTask.processedDate,
        isIdle: projectionTask.isIdle,
        error: projectionTask.error,
        errorEvent: projectionTask.errorOffset,
        errorOffset: projectionTask.errorOffset,
        config: projection.config,
        context: projection.context,
        state: projection.state
      }

      const finalProjectionTask = this._createProjectionTaskPayload(obj);
      return finalProjectionTask;
    }
  }
  return null;
};

/**
 * @param {String} context
 * @returns {Promise<Array<import('../eventstore-projection').Projection>>} returns the projections
 */
 EventstoreProjectionInMemoryStore.prototype.getProjections = async function(context) {
  const projections = [];
  for (const key in this._projections) {
      if (Object.prototype.hasOwnProperty.call(this._projections, key)) {
        const projection = this._projections[key];
        if (projection && (!context || projection.context === context)) {
          projections.push(this._createProjectionPayload(projection));
        }
      }
  }
  return projections;
};

/**
* Gets a lists of projection tasks
* @param {String} projectionId the projection id to use. optional
* @param {String} shard the shard to use. optional
* @returns {Promise<Array<import('../../eventstore-projections/eventstore-projection').ProjectionTask>>} returns the projection tasks
*/
EventstoreProjectionInMemoryStore.prototype.getProjectionTasks = async function (projectionId, shard) {
    let projectionTasks = [];
    for (const key in this._projectionTasks) {
        if (Object.prototype.hasOwnProperty.call(this._projectionTasks, key)) {
            const projectionTask = this._projectionTasks[key];
            const projection = this._projections[projectionTask.projectionId];
            
            if (projectionTask && projection &&
                (!projectionId || projectionTask.projectionId === projectionId) &&
                ((_.isNil(shard) && shard !== 0) || projectionTask.shard === parseInt(shard))) {
                    const obj = {
                      projectionTaskId: projectionTask.projectionTaskId,
                      projectionId: projectionTask.projectionId,
                      shard: projectionTask.shard,
                      partition: projectionTask.partition,
                      offset: projectionTask.offset,
                      processedDate: projectionTask.processedDate,
                      isIdle: projectionTask.isIdle,
                      error: projectionTask.error,
                      errorEvent: projectionTask.errorOffset,
                      errorOffset: projectionTask.errorOffset,
                      config: projection.config,
                      context: projection.context,
                      state: projection.state
                    }
                    projectionTasks.push(this._createProjectionTaskPayload(obj));
            }
        }
    }

    return projectionTasks;
};


/**
* Gets a lists of projection tasks
* @param {String} context the context to use
* @param {String} shard the shard to use. optional
* @returns {Promise<Array<import('../../eventstore-projections/eventstore-projection').ProjectionTask>>} returns the projection tasks
*/
EventstoreProjectionInMemoryStore.prototype.getProjectionTasksByContext = async function (context, shard) {
    let projectionTasks = [];
    for (const key in this._projectionTasks) {
        if (Object.prototype.hasOwnProperty.call(this._projectionTasks, key)) {
            const projectionTask = this._projectionTasks[key];
            const projection = this._projections[projectionTask.projectionId];

            if (projectionTask && projection &&
                (!context || projection.context === context) &&
                (!_.isNumber(parseInt(shard)) || projectionTask.shard === parseInt(shard))) {
                    const obj = {
                      projectionTaskId: projectionTask.projectionTaskId,
                      projectionId: projectionTask.projectionId,
                      shard: projectionTask.shard,
                      partition: projectionTask.partition,
                      offset: projectionTask.offset,
                      processedDate: projectionTask.processedDate,
                      isIdle: projectionTask.isIdle,
                      error: projectionTask.error,
                      errorEvent: projectionTask.errorOffset,
                      errorOffset: projectionTask.errorOffset,
                      config: projection.config,
                      context: projection.context,
                      state: projection.state
                    }
                    projectionTasks.push(this._createProjectionTaskPayload(obj));
            }
        }
    }

    return projectionTasks;
};

/**
 * @param {String} projectionId the projectionId to update
 * @param {Number} processedDate the date when the projection is processed
 * @param {Number} newOffset optional. also sets the new offset
 * @param {Boolean} isIdle optional. also sets the new offset
 * @returns {Promise<void>} - returns a Promise of type void
 */
EventstoreProjectionInMemoryStore.prototype.setProcessed = async function(projectionTaskId, processedDate, newOffset, isIdle) {
    const projectionTask = this._projectionTasks[projectionTaskId];
    if (projectionTask) {
      if (newOffset) {
        projectionTask.processedDate = processedDate;
        projectionTask.offset = newOffset;
        projectionTask.isIdle = isIdle == true ? 1 : 0;
      } else {
        projectionTask.processedDate = processedDate;
        projectionTask.isIdle = isIdle == true ? 1 : 0;
      }

  
      this._projectionTasks[projectionTaskId] = projectionTask;
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
 EventstoreProjectionInMemoryStore.prototype.setError = async function(projectionTaskId, error, errorEvent, errorOffset) {
  const newProjectionTask = this._projectionTasks[projectionTaskId];
  if (newProjectionTask) {
    const errorDetails = error ? JSON.stringify(error, Object.getOwnPropertyNames(error)) : null;
    newProjectionTask.error = errorDetails;
    newProjectionTask.errorEvent = errorEvent ? JSON.stringify(errorEvent) : null;
    newProjectionTask.errorOffset = errorOffset || null;

    this._projectionTasks[projectionTaskId] = newProjectionTask;
  }
};


EventstoreProjectionInMemoryStore.prototype.setOffset = async function(projectionTaskId, offset) {
  const newProjectionTask = this._projectionTasks[projectionTaskId];
  if (newProjectionTask) {
    newProjectionTask.offset = offset;

    debug('setOffset query', projectionTaskId, offset);
    this._projectionTasks[projectionTaskId] = newProjectionTask;
  }
};

/**
* Deletes a projection and all associated projection tasks
* @param {String} projectionId the projection id of the projection to delete.
* @returns {Promise<void>} - returns a Promise of type void
*/
EventstoreProjectionInMemoryStore.prototype.deleteProjection = async function (projectionId) {
  delete this._projections[projectionId];
  const newProjectionTasksList = [];
  for (const key in this._projectionTasks) {
      if (Object.prototype.hasOwnProperty.call(this._projectionTasks, key)) {
        const projectionTask = this._projectionTasks[key];
        if (projectionTask.projectionId !== projectionId) {
          newProjectionTasksList.push(projectionTask); 
        } else {
          delete this._projectionCheckpoint[projectionTask.projectionTaskId];
        }
      }
  }
  this._projectionTasks = newProjectionTasksList;
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
    state: inmemoryPayload.state || null
  }
};

/**
 * @param {Object} inmemoryPayload the starting string of the keys to delete
 * @returns {import('../eventstore-projection').Projection} - returns type Projection
 */
 EventstoreProjectionInMemoryStore.prototype._createProjectionTaskPayload = function(row) {
  const projection = this._createProjectionPayload(row);
    
  /**
  * @type {import('../eventstore-projection').ProjectionTask}
  */
  const projectionTask = {
      projectionTaskId: row.projectionTaskId,
      projectionId: row.projectionId,
      shard: parseInt(row.shard),
      partition: row.partition,
      offset: row.offset,
      processedDate: row.processedDate,
      isIdle: row.isIdle,
      error: row.error,
      errorEvent: row.errorEvent,
      errorOffset: row.errorOffset,
      projection: projection
  };
  return projectionTask;
};

/**
 * @param {String} projectionTaskId the projectionTaskId that processed the stream
 * @param {String} context the stream's context
 * @param {String} aggregate the stream's aggregate
 * @param {String} aggregateId the stream's aggregateId
 * @returns {Promise<number>} the latest processed version, per stream and per projection
 */
 EventstoreProjectionInMemoryStore.prototype.getProjectionStreamVersion = async function(projectionTaskId, context, aggregate, aggregateId) {
  const projectionCheckpoint = this._projectionCheckpoint[projectionTaskId][context][aggregate][aggregateId];
  let streamVersion = null;
  if (projectionCheckpoint) {
    streamVersion = projectionCheckpoint.projectionStreamVersion;
  }

  return streamVersion;
}


/**
* @param {String} projectionTaskId the projectionTaskId to update
* @param {String} streamId the streamId to update
* @returns {Promise}
*/

// Maybe pass offset instead of incrementing? 
EventstoreProjectionInMemoryStore.prototype.updateProjectionStreamVersion = async function(projectionTaskId, context, aggregate, aggregateId, version) {
  const newProjectionCheckpoint = this._projectionCheckpoint[projectionTaskId][context][aggregate][aggregateId];
  if (newProjectionCheckpoint) {
    newProjectionCheckpoint.projectionStreamVersion = version;
  }
  this._projectionCheckpoint[projectionTaskId][context][aggregate][aggregateId] = newProjectionCheckpoint;
}

/**
* @param {String} projectionTaskId the projectionTaskId of the projectionTask that processed the stream
* @param {String} projectionId the projectionId of the projectionTask that processed the stream
* @param {String} context the stream's context
* @param {String} aggregate the stream's aggregate
* @param {String} aggregateId the stream's aggregateId
* @param {String} version the version to set as processed
* @returns {Promise}
*/
EventstoreProjectionInMemoryStore.prototype.setProjectionStreamVersion = async function(projectionTaskId, projectionId, context, aggregate, aggregateId, version) {
  if (!this._projectionCheckpoint[projectionTaskId]) {
    this._projectionCheckpoint[projectionTaskId] = {};
  }
  if (!this._projectionCheckpoint[projectionTaskId][context]) {
    this._projectionCheckpoint[projectionTaskId][context] = {};
  }
  if (!this._projectionCheckpoint[projectionTaskId][context][aggregate]) {
    this._projectionCheckpoint[projectionTaskId][context][aggregate] = {};
  }
  this._projectionCheckpoint[projectionTaskId][context][aggregate][aggregateId] = {
      projectionTaskId: projectionTaskId,
      context: context,
      aggregate: aggregate,
      aggregateId: aggregateId,
      projectionStreamVersion: version
  };
}


module.exports = EventstoreProjectionInMemoryStore;
