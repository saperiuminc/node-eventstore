const util = require('util');
const murmurhash = require('murmurhash');

class ClusteredEventStore {
    constructor(options) {
        this._mappingStore = null;
        this._options = options;
        this._eventstores = [];
    }

    async getShardAndPartition(aggregateId) {
        let shard = await this._mappingStore.getShard(aggregateId);
        return shard;
    }

    getPartition(aggregateId, numberOfPartitions) {
        const partition = murmurhash(aggregateId) % numberOfPartitions;
        return partition;
    }

    getEventstream(query, revMin, revMax, callback) {
        this._getEventStream(query, revMin, revMax).then((data) => callback(data)).catch(callback);
    }

    async _getEventStream(query, revMin, revMax) {
        // NOTE: our usage always have aggregateId in query
        let shardPartition = await this.getShardAndPartition(aggregateId);

        if (!shardPartition) {
            // numberOfPartitions = 80
            // 0 - 79
            const partition = this.getPartition(aggregateId, numberOfPartitions);

            // 10 % 4 == 2
            const shard = partition % this._options.numberOfShards;

            shardPartition = {
                shard: shard,
                partition: partition
            }

            await this._mappingStore.addShardPartition(aggregateId, shardPartition);
        }

        const getEventStream = util.promisify(this._eventstores[shardPartition.shard].getEventstream);

        return getEventStream(query, revMin, revMax);
    }

}

module.exports = ClusteredEventStore;