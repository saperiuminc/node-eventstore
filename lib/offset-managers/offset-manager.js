/* eslint-disable no-unused-vars */
/**
 * OffsetManagerOptions
 * @typedef {Object} OffsetManagerOptions
 */

/**
 * OffsetManager constructor
 * @class
 * @constructor
 */
function OffsetManager() {
}

OffsetManager.prototype = {
    deserializeOffset: function (serializedOffset) {
        throw new Error('not implemented');
    },

    serializeOffset: function (offset) {
        throw new Error('not implemented');
    },

    getStartingSerializedOffset(hasPartitions) {
        throw new Error('not implemented');
    },

    compareOffset(offset1, offset2) {
        throw new Error('not implemented');
    },

    compareEventWithOffset(event, offset) {
        throw new Error('not implemented');
    },

    compareEventOffset(event1, event2) {
        throw new Error('not implemented');
    },

    computeEarliestOffset(event, projectionOffset) {
        throw new Error('not implemented');
    }
}

module.exports = OffsetManager;