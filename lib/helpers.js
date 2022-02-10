const murmurhash = require('murmurhash');

module.exports.getShard = (aggregateId, numOfShards) => {
  // TODO: need to sync with mark
    let shard = murmurhash(aggregateId, 1) % numOfShards;
    return shard;
}
