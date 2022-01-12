const mysql2 = require('mysql2/promise');

class ClusteredMappingStore {
    constructor(options) {
        this._options = options;

        this.pool.execute('CREATE TABLE mapping(aggregateId, shard, partition)');
    }

    async getShardAndPartition (aggregateId) {
        const result = await mysql2.execute('SELECT shard, partition FROM mapping WHERE aggregate_id = ? LIMIT 1', [aggregateId]);

        return result.rows[0];
    }

    async setShardAndPartition (aggregateId, shardId, partition) {
        const payload = {
            aggregateId: aggregateId,
            shardId: shardId,
            partition: partition
        };
        return await mysql2.execute('INSERT INTO mapping VALUES (?)', payload);
    }
}

module.exports = ClusteredMappingStore;