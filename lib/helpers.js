const murmurhash = require('murmurhash');

module.exports.getShard = (aggregateId, numOfShards) => {
    let shard = murmurhash(aggregateId, 1) % numOfShards;
    return shard;
};
