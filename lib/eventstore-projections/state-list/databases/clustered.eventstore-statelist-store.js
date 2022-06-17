const _ = require('lodash');
const util = require('util');
const BaseStateListStore = require('./base.eventstore-statelist-store');

function ClusteredStateListStore(options) {
    this._options = options;
    this._stateListStores = [];
    
    if (!this._options.clusters || !Array.isArray(this._options.clusters)) {
        throw new Error('missing or invalid options.clusters')
    }

    BaseStateListStore.call(this, options);
}

util.inherits(ClusteredStateListStore, BaseStateListStore);

_.extend(ClusteredStateListStore.prototype, {
    /**
     * Initializes and connects to the mysql store.
     * @returns {Promise}
     */
    init: function() {
        this._stateListStores.forEach((store) => {
            store.init();
        });
    },

    /**
     * Creates a state list given the input configuration
     * @returns {Promise}
     */
    createList: async function(stateListConfig) {
        return this._doOnAllStores('createList', stateListConfig);
    },

    // Note: Signature change. Not a perfect composite pattern. Without shard param, there is no way to determine proper store to use.
    // Unless it's part of a wrapped query object. naming this: partitionBy

    /**
     * @param {String} listName the name of the list
     * @param {Number} index the index of the row to delete
     * @param {import('./base.eventstore-statelist-store').EventstoreStateListPartitionBy} partitionBy contains the partition of the row to delete
     * @returns {Promise}
     */
    delete: async function(listName, index, partitionBy) {
        return this._doOnShardedStore(partitionBy.shard, 'delete', listName, index, partitionBy);
    },
    
    /**
     * @param {String} listName the name of the list
     * @param {Number} index the index of the row to update
     * @param {Object} state the new state of the item to update
     * @param {Object} meta the new meta of the item to update
     * @param {import('./base.eventstore-statelist-store').EventstoreStateListPartitionBy} partitionBy contains the partition of the row to update
     * @returns {Promise}
     */
    set: async function(listName, index, state, meta, partitionBy) {
        return this._doOnShardedStore(partitionBy.shard, 'set', listName, index, state, meta, partitionBy);
    },

    /**
     * @param {String} listName the name of the list
     * @param {Object} state the state of the item to add
     * @param {Object} meta the meta of the item to add
     * @param {import('./base.eventstore-statelist-store').EventstoreStateListPartitionBy} partitionBy contains the partition of the row to add
     * @returns {Promise}
     */
    push: async function(listName, state, meta, partitionBy) {
        return this._doOnShardedStore(partitionBy.shard, 'push', listName, state, meta, partitionBy);
    },

    /**
     * @param {String} listName the name of the list
     * @param {String} lastId
     * @param {Object} filters
     * @param {import('./base.eventstore-statelist-store').EventstoreStateListPartitionBy} partitionBy contains the partition to find from
     * @returns {Promise}
     */
    find: async function(listName, lastId, filters, partitionBy) {
        return this._doOnShardedStore(partitionBy.shard, 'find', listName, lastId, filters, partitionBy);
    },

    /**
     * @param {String} listName the name of the list
     * @param {String} lastId
     * @param {Object} filters
     * @param {import('./base.eventstore-statelist-store').EventstoreStateListPartitionBy} partitionBy contains the partition to filter from
     * @returns {Promise<Array>} - returns an array of rows
     */
    filter: async function(listName, lastId, filters, partitionBy) {
        return this._doOnShardedStore(partitionBy.shard, 'filter', listName, lastId, filters, partitionBy);
    },

    /**
     * @param {String} listName the name of the list
     * @returns {Promise}
     */
    truncate: async function(listName) {
        return this._doOnAllStores('truncate', listName);
    },

    /**
     * @param {String} listName the name of the list
     * @returns {Promise}
     */
    destroy: async function(listName) {
        return this._doOnAllStores('destroy', listName);
    },

    /**
     * @param {String} listName the name of the list
     * @returns {Promise}
     */
    deleteList: async function(listName) {
        return this._doOnAllStores('deleteList', listName);
    },

    /**
     * @param {BaseStateListStore} store the state list store to add to the cluster
     * @returns {void}
     */
    addStore: function(store) {
        this._stateListStores.push(store);
    },

    /**
     * @param {BaseStateListStore} store the state list store to remove from the cluster
     * @returns {void}
     */
    removeStore: function(store) {
        const removedIndex = this._stateListStores.findIndex((item) => {
            return item === store;
        });
        if (removedIndex > -1) {
            this._stateListStores.splice(removedIndex, 1);
        }
    },

    _doOnShardedStore: async function(shard, methodName, ...args) {
        let stateListStore = null;
        if (_.isNil(shard) || !_.isNumber(shard)) {
            stateListStore = this._stateListStores[0];
        } else {
            stateListStore = this._stateListStores[shard];
        }
        return stateListStore[methodName](...args); 
    },
    _doOnAllStores: async function(methodName, ...args) {
        const promises = [];
        for (const store of this._stateListStores) {
            promises.push(store[methodName](...args));
        }
        return Promise.all(promises);
    }
});

module.exports = ClusteredStateListStore;
