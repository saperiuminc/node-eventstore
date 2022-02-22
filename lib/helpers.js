const murmurhash = require('murmurhash');

module.exports.getShard = (aggregateId, numOfShards) => {
  // TODO: need to sync with mark
    let shard = murmurhash(aggregateId, 1) % numOfShards;
    return shard;
}


module.exports.serializeProjectionOffset = (projectionOffset) => {
  // return Buffer.from(JSON.stringify(projectionOffset)).toString('base64');
  // TODO: REVIEW: to reduce column length
  return JSON.stringify(projectionOffset);
}
module.exports.deserializeProjectionOffset = (serializedProjectionOffset) => {
  // return JSON.parse(Buffer.from(serializedProjectionOffset, 'base64').toString('utf8'));
  // TODO: REVIEW: to reduce column length
  return JSON.parse(serializedProjectionOffset);
}
