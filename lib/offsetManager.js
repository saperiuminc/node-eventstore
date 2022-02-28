const _ = require('lodash');

/**
 * Offset
 * @typedef {Object} Offset
 * @property {Number} commitStamp the timestamp of the event
 * @property {String} eventId event id identifier
 * @property {Number} streamRevision stream revision
 */

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
        if (serializedOffset) {
            return JSON.parse(Buffer.from(serializedOffset, 'base64').toString('utf8'));
        }
        return serializedOffset;
    },

    serializeOffset: function (offset) {
        if (offset) {
            return Buffer.from(JSON.stringify(offset)).toString('base64');
        }
        return offset;
    },

    getStartingOffset() {
        return this.serializeOffset({
            commitStamp: 0,
            streamRevision: -1,
            eventId: ''
        });
    },

    compareOffset(offset1, offset2) {
        const deserializedOffset1 = this.deserializeOffset(offset1);
        const deserializedOffset2 = this.deserializeOffset(offset2);

        if (deserializedOffset1.commitStamp === deserializedOffset2.commitStamp) {
            if (deserializedOffset1.streamRevision === deserializedOffset2.streamRevision) {
                return this._compare(deserializedOffset1.eventId, deserializedOffset2.eventId);
            } else {
                return this._compare(deserializedOffset1.streamRevision, deserializedOffset2.streamRevision);
            }
        } else {
            return this._compare(deserializedOffset1.commitStamp, deserializedOffset2.commitStamp);
        }
    },

    _compare(field1, field2) {
        if (field1 === field2) {
            return 0;
        } else if (field1 > field2 || _.isNil(field2)) {
            return 1;
        } else if (field1 < field2 || _.isNil(field1)) {
            return -1;
        }
    }
}

module.exports = OffsetManager;