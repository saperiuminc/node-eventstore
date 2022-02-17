/* eslint-disable require-jsdoc */
const util = require('util');
const Store = require('../base');
const _ = require('lodash');

function ClusteredEventStore(options) {
    this._options = options || {};
    Store.call(this, options);
}

util.inherits(ClusteredEventStore, Store);

_.extend(ClusteredEventStore.prototype, {
    getStores: function() {
      throw new Error('This must be implemented by subclass');
    }
});

module.exports = ClusteredEventStore;
