const util = require('util');
const OffsetManager = require('./offset-manager');
const _ = require('lodash');

/**
 * CompositeOffsetManager constructor
 * @class
 * @constructor
 */
function CompositeOffsetManager() {
    OffsetManager.call(this);
}

util.inherits(CompositeOffsetManager, OffsetManager);

CompositeOffsetManager.prototype = {
    deserializeOffset: function (serializedOffset) {
        if (serializedOffset) {
            const minifiedOffset = JSON.parse(serializedOffset);
            return {
                commitStamp: minifiedOffset.c,
                eventId: minifiedOffset.e,
                streamRevision: minifiedOffset.s,
            }
        }
        return serializedOffset;
    },

    serializeOffset: function (offset) {
        if (offset) {
            const minifiedOffset = {
                c: offset.commitStamp,
                e: offset.eventId,
                s: offset.streamRevision
            }
            return JSON.stringify(minifiedOffset);
        }
        return offset;
    },

    getStartingSerializedOffset(hasPartitions) {
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
    },

    compareEventWithOffset(event, offset) {
        const deserializedOffset = this.deserializeOffset(offset);

        if (event.commitStamp === deserializedOffset.commitStamp) {
            if (event.streamRevision === deserializedOffset.streamRevision) {
                return this._compare(event.eventId, deserializedOffset.eventId);
            } else {
                return this._compare(event.streamRevision, deserializedOffset.streamRevision);
            }
        } else {
            return this._compare(event.commitStamp, deserializedOffset.commitStamp);
        }
    }
}

module.exports = CompositeOffsetManager;