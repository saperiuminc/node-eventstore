/* eslint-disable no-unused-vars */
const _ = require('lodash');

/**
 * DeduplicationManager constructor
 * @class
 * @constructor
 */
function DeduplicationManager(projectionStore, offsetManager) {
    this._projectionStore = projectionStore;
    this._offsetManager = offsetManager;
}

DeduplicationManager.prototype = {
    isEventProcessed: async function (projectionTask, event) {
        const projectionStreamVersion = await this._projectionStore.getProjectionStreamVersion(projectionTask.projectionTaskId, event.context, event.aggregate, event.aggregateId);
        return !_.isNil(projectionStreamVersion) && event.streamRevision <= projectionStreamVersion;
    },

    setEventAsProcessed: async function (projectionTask, event) {
        await this._projectionStore.setProjectionStreamVersion(projectionTask.projectionTaskId, projectionTask.projectionId, event.context, event.aggregate, event.aggregateId, event.streamRevision);
    }
}

module.exports = DeduplicationManager;