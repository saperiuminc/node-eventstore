/* eslint-disable require-jsdoc */

class Snapshot {
    constructor(snapshotId, snapshotInfo) {
        this.aggregateId = snapshotInfo.aggregateId;
        this.aggregate = snapshotInfo.aggregate;
        this.context = snapshotInfo.context;
        this.revision = snapshotInfo.revision;
        this.snapshotId = snapshotId;
        this.snapshotAt = snapshotInfo.snapshotAt;
        this.data = snapshotInfo.data;
    }
}

module.exports = Snapshot;