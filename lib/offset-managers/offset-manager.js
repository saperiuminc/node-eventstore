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

    getStartingSerializedOffset() {
        throw new Error('not implemented');
    },

    compareOffset(offset1, offset2) {
        throw new Error('not implemented');
    }
}

module.exports = OffsetManager;