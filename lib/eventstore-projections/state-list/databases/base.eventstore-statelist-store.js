const _ = require('lodash');
const debug = require('debug')('eventstore:state-list')

/**
 * @typedef {("CREATE"|"UPDATE"|"DELETE")} EventStoreStateListRowType
 **/

/**
 * EventstoreStateListDoneCallback
 * @callback EventstoreStateListDoneCallback
 * @param {Error} error The error if any
 * @param {Object} result Result of this callback
 * @returns {void} Returns void
 */

/**
 * EventstoreStateListFilters
 * @typedef {Object} EventstoreStateListFilters
 */

/**
 * EventstoreStateListSort
 * @typedef {Object} EventstoreStateListSort
 */

/**
 * EventstoreStateListPartitionBy
 * @typedef {Object} EventstoreStateListPartitionBy
 * @property {Number} shard The assigned shard
 * @property {Number} partition The assigned partition
 */

/**
 * EventstoreStateListSecondaryKey
 * @typedef {Object} EventstoreStateListSecondaryKey
 * @property {String} name The name of the field
 * @property {("ASC"|"DESC")} sort The sort directioon of the key. Default is ASC
 */

/**
 * EventstoreStateListField
 * @typedef {Object} EventstoreStateListField
 * @property {String} type The type of the field
 * @property {String} name The field name
 */

/**
 * EventstoreStateListData
 * @typedef {Object} EventstoreStateListData
 * @property {String} rowIndex The last rowIndex
 * @property {String} lastId The last id
 */

/**
 * @param {EventstoreStateListOptions} options additional options for the Eventstore state list
 * @constructor
 */
function EventstoreStateListStore(options) {
    options = options || {};
}

/**
 * Initializes and connects to the mysql store.
 * @returns {Promise}
 */
EventstoreStateListStore.prototype.init = async function() {
    throw new Error('not implemented');
};

/**
 * Creates a state list given the input configuration
 * @returns {Promise}
 */
EventstoreStateListStore.prototype.createList = async function(stateListConfig) {
    throw new Error('not implemented');
}

/**
 * @param {String} listName the name of the list
 * @param {Number} index the index of the row to delete
 * @param {EventstoreStateListPartitionBy} partitionBy contains the partition of the row to delete
 * @returns {Promise}
 */
EventstoreStateListStore.prototype.delete = async function(listName, index, partitionBy) {
    throw new Error('not implemented');
};

/**
 * @param {String} listName the name of the list
 * @param {Number} index the index of the row to update
 * @param {Object} state the new state of the item to update
 * @param {Object} meta the new meta of the item to update
 * @param {EventstoreStateListPartitionBy} partitionBy contains the partition of the row to update
 * @returns {Promise}
 */
EventstoreStateListStore.prototype.set = async function(listName, index, state, meta, partitionBy) {
    throw new Error('not implemented');
};

/**
 * @param {String} listName the name of the list
 * @param {Object} state the state of the item to add
 * @param {Object} meta the meta of the item to add
 * @param {EventstoreStateListPartitionBy} partitionBy contains the partition of the row to add
 * @returns {Promise}
 */
EventstoreStateListStore.prototype.push = async function(listName, state, meta, partitionBy) {
    throw new Error('not implemented');
};

/**
 * @param {String} listName the name of the list
 * @param {String} lastId
 * @param {Object} filters
 * @param {EventstoreStateListPartitionBy} partitionBy contains the partition to find from
 * @returns {Promise}
 */
EventstoreStateListStore.prototype.find = async function(listName, lastId, filters, partitionBy) {
    throw new Error('not implemented');
};

/**
 * @param {String} listName the name of the list
 * @param {String} lastId
 * @param {Object} filters
 * @param {EventstoreStateListPartitionBy} partitionBy contains the partition to filter from
 * @returns {Promise<Array>} - returns an array of rows
 */
EventstoreStateListStore.prototype.filter = async function(listName, lastId, filters, partitionBy) {
    throw new Error('not implemented');
};

/**
 * @param {String} listName the name of the list
 * @returns {Promise}
 */
EventstoreStateListStore.prototype.truncate = async function(listName) {
    throw new Error('not implemented');
};

/**
 * @param {String} listName the name of the list
 * @returns {Promise}
 */
EventstoreStateListStore.prototype.destroy = async function(listName) {
    throw new Error('not implemented');
};

/**
 * @param {String} listName the name of the list
 * @returns {Promise}
 */
EventstoreStateListStore.prototype.deleteList = async function(listName) {
    throw new Error('not implemented');
};

module.exports = EventstoreStateListStore;