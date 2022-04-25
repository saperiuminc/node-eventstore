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
                commitStamp1: minifiedOffset.c1 || 0,
                eventId1: minifiedOffset.e1 || '',
                streamRevision1: minifiedOffset.s1 !== undefined ? minifiedOffset.s1 : -1,
                commitStamp2: minifiedOffset.c2 || 0,
                eventId2: minifiedOffset.e2 || '',
                streamRevision2: minifiedOffset.s2 !== undefined ? minifiedOffset.s2 : -1
            }
        }
        return serializedOffset;
    },

    serializeOffset: function (offset) {
        if (offset) {
            const minifiedOffset = {
                c1: 0,
                s1: -1,
                e1: '',
                c2: 0,
                s2: -1,
                e2: ''
            };

            if (offset.commitStamp1) {
                minifiedOffset.c1 = offset.commitStamp1;
                minifiedOffset.e1 = offset.eventId1;
                minifiedOffset.s1 = offset.streamRevision1;
            }

            if (offset.commitStamp2) {
                minifiedOffset.c2 = offset.commitStamp2;
                minifiedOffset.e2 = offset.eventId2;
                minifiedOffset.s2 = offset.streamRevision2;
            }
            return JSON.stringify(minifiedOffset);
        }
        return offset;
    },

    // eslint-disable-next-line no-unused-vars
    getStartingSerializedOffset(hasPartitions) {
        return this.serializeOffset({
            commitStamp1: 0,
            streamRevision1: -1,
            eventId1: '',
            commitStamp2: 0,
            streamRevision2: -1,
            eventId2: ''
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

        const event1Stamp = new Date(event.commitStamp).getTime();
        if (event1Stamp === deserializedOffset.commitStamp) {
            if (event.streamRevision === deserializedOffset.streamRevision) {
                return this._compare(event.eventId, deserializedOffset.eventId);
            } else {
                return this._compare(event.streamRevision, deserializedOffset.streamRevision);
            }
        } else {
            return this._compare(event1Stamp, deserializedOffset.commitStamp);
        }
    },

    compareEventOffset(event1, event2) {
        const event1Stamp = new Date(event1.commitStamp).getTime();
        const event2Stamp = new Date(event2.commitStamp).getTime();
        if (event1Stamp === event2Stamp) {
            if (event1.streamRevision === event2.streamRevision) {
                return this._compare(event1.eventId, event2.eventId);
            } else {
                return this._compare(event1.streamRevision, event2.streamRevision);
            }
        } else {
            return this._compare(event1Stamp, event2Stamp);
        }
    },

    // eslint-disable-next-line no-unused-vars
    computeEarliestOffset(event, projectionOffset) {
        if (!event.eventId && event.id) {
            event.eventId = event.id;
        }
        if (this.compareEventWithOffset(event, projectionOffset) < 0) {
            const offset = {
                commitStamp: new Date(event.commitStamp).getTime(),
                streamRevision: event.streamRevision,
                eventId: event.eventId.substring(0, event.eventId.length -1)
            };
            return this.serializeOffset(offset);
        } else {
            return projectionOffset;
        }
    }
}

module.exports = CompositeOffsetManager;
