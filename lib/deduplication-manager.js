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
    getLastProcessedOffset: function (projectionTask, signaledEvents) {
        const hasPartitions = _.isNil(projectionTask.partition) || projectionTask.partition === '__unpartitioned' ? false : true;
        let offset = projectionTask.offset ? projectionTask.offset : this._offsetManager.getStartingSerializedOffset(hasPartitions);

        if (signaledEvents && signaledEvents.length > 0) {
            const events = _.cloneDeep(signaledEvents);
            events.sort((event1, event2) => {
                return this._offsetManager.compareEventOffset(event1, event2);
            });

            const earliestEvent = events[0];
            if (!_.isNil(projectionTask.offset) && this._offsetManager.compareEventWithOffset(earliestEvent, projectionTask.offset) < 0) {
                offset = this._offsetManager.computeEarliestOffset(earliestEvent, offset);
            }
        }

        return offset;
    },

    isEventProcessed: async function (projectionTask, event) {
        const projectionStreamVersion = await this._projectionStore.getProjectionStreamVersion(projectionTask.projectionTaskId, event.context, event.aggregate, event.aggregateId);
        return !_.isNil(projectionStreamVersion) && event.streamRevision <= projectionStreamVersion;
    },

    setEventAsProcessed: async function (projectionTask, event) {
        await this._projectionStore.setProjectionStreamVersion(projectionTask.projectionTaskId, projectionTask.projectionId, event.context, event.aggregate, event.aggregateId, event.streamRevision);
    }
}

module.exports = DeduplicationManager;