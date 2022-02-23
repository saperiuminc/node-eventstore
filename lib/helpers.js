const murmurhash = require('murmurhash');

module.exports.getShard = (aggregateId, numOfShards) => {
    let shard = murmurhash(aggregateId, 1) % numOfShards;
    return shard;
}

// TODO: make an offset.serializer
module.exports.serializeProjectionOffset = (projectionOffset) => {
  if (projectionOffset) {
    return Buffer.from(JSON.stringify(projectionOffset)).toString('base64');
  }
  return projectionOffset;
}
module.exports.deserializeProjectionOffset = (serializedProjectionOffset) => {
  if (serializedProjectionOffset) {
    return JSON.parse(Buffer.from(serializedProjectionOffset, 'base64').toString('utf8'));
  }
  return serializedProjectionOffset;
}
