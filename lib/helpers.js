const murmurhash = require('murmurhash');

module.exports.getShard = (partitionById, numOfShards) => {
    let shard = murmurhash(partitionById, 1) % numOfShards;
    return shard;
};
