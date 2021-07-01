const _ = require('lodash');
const debug = require('debug')('eventstore:state-list');
const StateListStore = require('./eventstore-statelist-store');
const util = require('util');

/**
 * @param {EventstoreStateListOptions} options additional options for the Eventstore state list
 * @constructor
 */
function EventstoreStateListInMemoryStore(options) {
    options = options || {};

    this.options = options;

    this._configs = {};
    this._lists = {};
    this._lastIndices = {};
    this._listIndices = {};
}

util.inherits(EventstoreStateListInMemoryStore, StateListStore);

EventstoreStateListInMemoryStore.prototype._configs;
EventstoreStateListInMemoryStore.prototype._lastIndices;
EventstoreStateListInMemoryStore.prototype._listIndices;
EventstoreStateListInMemoryStore.prototype._lists;

EventstoreStateListInMemoryStore.prototype.init = async function() {
    
};

EventstoreStateListInMemoryStore.prototype.createList = async function(stateListConfig) {
    this._lists[stateListConfig.name] = [];
    this._configs[stateListConfig.name] = stateListConfig;

    this._listIndices[stateListConfig.name] = {};
}

 EventstoreStateListInMemoryStore.prototype.push = async function(listName, state, meta) {
    let lastIndex = this._lastIndices[listName];
    if (!lastIndex) {
        lastIndex = -1;
    }

    this._lastIndices[listName] = lastIndex = lastIndex + 1;

    this._lists[listName][lastIndex] = {
        state: state,
        meta: meta
    };

    const stateListConfig = this._configs[listName];

    if (stateListConfig.secondaryKeys) {
        for (const key in stateListConfig.secondaryKeys) {
            if (Object.prototype.hasOwnProperty.call(stateListConfig.secondaryKeys, key)) {
                const keyObject = stateListConfig.secondaryKeys[key];
                
                for (const field in keyObject) {
                    const fieldValue = state[field.name];

                    let listIndexItems = this._listIndices[listName][field.name];

                    if (!listIndexItems) {
                        this._listIndices[listName][field.name] = listIndexItems = [];
                    }

                    const indexValue = `${fieldValue}:${lastIndex}`;
                    const shouldInsertToIndex = _.sortedIndex(listIndexItems, indexValue);
                    listIndexItems.splice(shouldInsertToIndex, 0, indexValue);
                }
            }
        }
    }

    return lastIndex;
};

EventstoreStateListInMemoryStore.prototype.delete = async function(listName, index) {
    delete this.lists[listName][index];
};

EventstoreStateListInMemoryStore.prototype.set = async function(listName, index, state, meta) {
    const stateListConfig = this._configs[listName];

    if (stateListConfig.secondaryKeys) {
        for (const key in stateListConfig.secondaryKeys) {
            if (Object.prototype.hasOwnProperty.call(stateListConfig.secondaryKeys, key)) {
                const keyObject = stateListConfig.secondaryKeys[key];
                
                for (const field in keyObject) {
                    const fieldValue = state[field.name];

                    let listIndexItems = this._listIndices[listName][field.name];

                    if (!listIndexItems) {
                        this._listIndices[listName][field.name] = listIndexItems = [];
                    }

                    const lastFieldValue = this._lists[listName][index]['state'][field.name];

                    if (lastFieldValue != fieldValue) {
                        // if it changes then remove and reinsert
                        const lastFieldIndexValue = `${lastFieldValue}:${index}`;
                        const indexToDelete = _.sortedIndexOf(listIndexItems, lastFieldIndexValue);
                        listIndexItems.splice(indexToDelete, 1);
                        
                        // reinsert
                        const indexValue = `${fieldValue}:${index}`;
                        const shouldInsertToIndex = _.sortedIndex(listIndexItems, indexValue);
                        listIndexItems.splice(shouldInsertToIndex, 0, indexValue);
                    }
                    
                }
            }
        }
    }

    this._lists[listName][index] = {
        state: state,
        meta: meta
    };
};

/**
 * @param {Number} startRowIndex the start index to get the list
 * @param {Number} limit the number of items to get
 * @param {Object} filters not yet implemented
 * @param {EventstoreStateListDoneCallback} cb the callback to call after the operation
 * @returns {void} - returns void. use the cb to get results
 */
EventstoreStateListInMemoryStore.prototype.find = async function(listName, lastId, filters) {
    let firstItem;
    if (filters.length > 1) {
        throw new Error('not yet supported multiple filters');
    }

    const list = this._lists[listName];
    if (list) {
        for (let i = 0; i < filters.length; i++) {
            const filter = filters[i];
    
            const indexToStart = _.sortedIndex(this._listIndices[listName][filter.field], filter.value);

            const keyValue = this._listIndices[listName][filter.field][indexToStart + 1];
            const itemKey = keyValue.split(':')[0];
            if (itemKey == filter.value) {
                const itemValue = keyValue.split(':')[1];
                firstItem = this._lists[listName][itemValue];
            }

            if (firstItem) {
                break;
            }
        }
    }

    return firstItem;
};

/**
 * @param {Number} startRowIndex the start index to get the list
 * @param {Number} limit the number of items to get
 * @param {Object} filters not yet implemented
 * @param {EventstoreStateListDoneCallback} cb the callback to call after the operation
 * @returns {void} - returns void. use the cb to get results
 */
EventstoreStateListInMemoryStore.prototype.filter = async function(listName, lastId, filters) {
    if (filters.length > 1) {
        throw new Error('not yet supported multiple filters');
    }

    const items = [];
    const list = this._lists[listName];
    if (list) {
        for (let i = 0; i < filters.length; i++) {
            const filter = filters[i];
    
            const indexToStart = _.sortedIndex(this._listIndices[listName][filter.field], filter.value);

            let counter = 1;
            let itemKey;
            do {
                const keyValue = this._listIndices[listName][filter.field][indexToStart + counter];
                itemKey = keyValue.split(':')[0];
                if (itemKey == filter.value) {
                    const itemValue = keyValue.split(':')[1];
                    items.push(this._lists[listName][itemValue]);
                }
            } while(itemKey == filter.value);
        }
    }

    return _.uniq(items);
}


/**
 * @param {String} listName the name of the list
 * @returns {Promise}
 */
EventstoreStateListInMemoryStore.prototype.truncate = async function(listName) {
    throw new Error('not implemented');
};

/**
 * @param {String} listName the name of the list
 * @returns {Promise}
 */
EventstoreStateListInMemoryStore.prototype.destroy = async function(listName) {
    throw new Error('not implemented');
};

/**
 * @param {String} listName the name of the list
 * @returns {Promise}
 */
EventstoreStateListInMemoryStore.prototype.deleteList = async function(listName) {
    throw new Error('not implemented');
};

module.exports = EventstoreStateListInMemoryStore;
