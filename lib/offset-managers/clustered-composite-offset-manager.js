const util = require('util');
const OffsetManager = require('./offset-manager');
const _ = require('lodash');

/**
 * ClusteredCompositeOffsetManager constructor
 * @class
 * @constructor
 */
function ClusteredCompositeOffsetManager(clusterCount, leafOffsetManager) {
    this._clusterCount = clusterCount;
    this._leafOffsetManager = leafOffsetManager;
    OffsetManager.call(this);
}

util.inherits(ClusteredCompositeOffsetManager, OffsetManager);

ClusteredCompositeOffsetManager.prototype = {
    deserializeOffset: function (serializedOffset) {
        if (serializedOffset) {
            return JSON.parse(serializedOffset);
        }
        return serializedOffset;
    },

    serializeOffset: function (offset) {
        if (offset) {
            return JSON.stringify(offset);
        }
        return offset;
    },

    getStartingSerializedOffset(hasPartitions) {
        if (!hasPartitions) {
            const startingOffsets = [];
            for (let i = 0; i < this._clusterCount; i++ ) {
                startingOffsets.push(this._leafOffsetManager.getStartingSerializedOffset());
            }
            return this.serializeOffset(startingOffsets);
        } else {
            return this._leafOffsetManager.getStartingSerializedOffset(hasPartitions);
        }
    },

    compareOffset(offset1, offset2) {
        const deserializedOffset1 = this.deserializeOffset(offset1);
        const deserializedOffset2 = this.deserializeOffset(offset2);

        if (Array.isArray(deserializedOffset1) && Array.isArray(deserializedOffset2)) {
            const maxOffset1 = this._getMaxOffset(deserializedOffset1);
            const maxOffset2 = this._getMaxOffset(deserializedOffset2);
    
            return this._leafOffsetManager.compareOffset(maxOffset1, maxOffset2);
        } else {
            return this._leafOffsetManager.compareOffset(offset1, offset2);
        }
    },

    _getMaxOffset(clusteredOffset) {
        let maxOffset = this._leafOffsetManager.getStartingSerializedOffset();
        if (clusteredOffset && clusteredOffset.length > 0) {
            for (let i = 0; i < clusteredOffset.length; i++) {
                if (i === 0) {
                    maxOffset = clusteredOffset[0];
                } else {
                    if (this._leafOffsetManager.compareOffset(clusteredOffset[i], maxOffset) > 0) {
                        maxOffset = clusteredOffset[i];
                    }
                }
            }
        }
        return maxOffset;
    },

    compareEventWithOffset(event, offset) {
        const deserializedOffset = this.deserializeOffset(offset);

        if (Array.isArray(deserializedOffset)) {
            const maxOffset = this._getMaxOffset(deserializedOffset);
            return this._leafOffsetManager.compareEventWithOffset(event, maxOffset);
        } else {
            return this._leafOffsetManager.compareEventWithOffset(event, offset);
        }
    },

    compareEventOffset(event1, event2) {
        return this._leafOffsetManager.compareEventOffset(event1, event2);
    },

    computeEarliestOffset(event, projectionOffset) {
        const deserializedOffset = this.deserializeOffset(projectionOffset);

        if (Array.isArray(deserializedOffset)) {
            const newOffset = [];
            for (let i = 0; i < deserializedOffset.length; i++) {
                newOffset.push(this._leafOffsetManager.computeEarliestOffset(event, deserializedOffset[i]));
            }
            return newOffset;
        } else {
            return this._leafOffsetManager.computeEarliestOffset(event, projectionOffset);
        }
    }
}

module.exports = ClusteredCompositeOffsetManager;