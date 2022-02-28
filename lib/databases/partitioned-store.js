/* eslint-disable require-jsdoc */
const util = require('util');
const Store = require('../base');
const _ = require('lodash');

function PartitionedStore(options, offsetManager) {
    this.offsetManager = offsetManager;
    Store.call(this, options);
}

util.inherits(PartitionedStore, Store);

_.extend(PartitionedStore.prototype, {
    getOffsetManager: function() {
        return this.offsetManager;
    },

    getPartitionId: function(aggregateId) {
        throw new Error('not implemented');
    },

    getPartitions: function() {
        throw new Error('not implemented');
    },

    getLatestOffset: function(shard, partition, callback) {
        throw new Error('not implemented');
    }
});

module.exports = PartitionedStore;
