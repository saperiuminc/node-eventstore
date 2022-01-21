const _ = require('lodash');
const StateListStore = require('./base.eventstore-statelist-store');
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

    if (stateListConfig.secondaryKeys) {
        for (const key in stateListConfig.secondaryKeys) {
            if (Object.prototype.hasOwnProperty.call(stateListConfig.secondaryKeys, key)) {
                const keyObject = stateListConfig.secondaryKeys[key];

                for (let i = 0; i < keyObject.length; i++) {
                    const field = keyObject[i];
                    this._listIndices[stateListConfig.name][field.name] = {};
                }

            }
        }
    }
}

EventstoreStateListInMemoryStore.prototype.push = async function(listName, state, meta) {
    let lastIndex = this._lastIndices[listName];
    if (lastIndex == undefined) {
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

                for (let i = 0; i < keyObject.length; i++) {
                    const field = keyObject[i];
                    const fieldValue = state[field.name];
                    if (fieldValue !== undefined) {
                        let listIndexForField = this._listIndices[listName][field.name];

                        if (!listIndexForField) {
                            this._listIndices[listName][field.name] = listIndexForField = {};
                        }

                        let listIndexForFieldItems = listIndexForField[fieldValue];

                        if (!listIndexForFieldItems) {
                            listIndexForField[fieldValue] = listIndexForFieldItems = {}
                        }

                        listIndexForFieldItems[lastIndex] = lastIndex;
                    }
                }
            }
        }
    }

    return lastIndex;
};

EventstoreStateListInMemoryStore.prototype.delete = async function(listName, index) {
  this._lists[listName].splice(index, 1);
};

EventstoreStateListInMemoryStore.prototype.set = async function(listName, index, state, meta) {
    const stateListConfig = this._configs[listName];

    if (stateListConfig.secondaryKeys) {
        for (const key in stateListConfig.secondaryKeys) {
            if (Object.prototype.hasOwnProperty.call(stateListConfig.secondaryKeys, key)) {

                const keyObject = stateListConfig.secondaryKeys[key];

                for (let i = 0; i < keyObject.length; i++) {
                    const field = keyObject[i];
                    const fieldValue = state[field.name];
                    if (fieldValue !== undefined) {
                        const lastFieldValue = this._lists[listName][index]['state'][field.name];

                        if (lastFieldValue != fieldValue) {
                            let listIndexForField = this._listIndices[listName][field.name];

                            if (!listIndexForField) {
                                this._listIndices[listName][field.name] = listIndexForField = {};
                            }

                            let listIndexForFieldItems = listIndexForField[fieldValue];

                            if (!listIndexForFieldItems) {
                                listIndexForField[fieldValue] = listIndexForFieldItems = {}
                            }

                            // remove last record
                            if (listIndexForField[lastFieldValue]) {
                                delete listIndexForField[lastFieldValue][index];
                            }

                            // reinsert
                            listIndexForFieldItems[index] = index;
                        }
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

  const list = this._lists[listName];
  if (list) {
      let firstIndexList = {};
      let iterations = [];
      for (let i = 0; i < filters.length; i++) {
          const filter = filters[i];
          if (filter.operator !== 'is') {
              throw new Error(`operator ${filter.operator} is not supported`);
          }
          if (!this._listIndices[listName][filter.field] || !this._listIndices[listName][filter.field][filter.value]) {
              break;
          }

          const listIndexFieldItems = this._listIndices[listName][filter.field][filter.value];
          if (i === 0) {
              firstIndexList = listIndexFieldItems;
          } else {
              iterations.push(listIndexFieldItems)
          }
      }
      let finalIndex = -1;
      for (const key in firstIndexList) {
          if (Object.hasOwnProperty.call(firstIndexList, key)) {
              let countIndex = 0;
              let hasIntersected = true;
              while (countIndex < iterations.length) {
                  const iterationResult = iterations[countIndex];
                  if (!iterationResult[key]) {
                      hasIntersected = false;
                      break;
                  }
                  countIndex++;
              }
              if (hasIntersected) {
                  finalIndex = firstIndexList[key];
                  break; 
              }
          }
      }
      
      if (finalIndex > -1) {
          firstItem = {
              index: finalIndex,
              meta: this._lists[listName][finalIndex] ? this._lists[listName][finalIndex].meta : undefined,
              value: this._lists[listName][finalIndex] ? this._lists[listName][finalIndex].state : undefined
          }
      }
  }

  return firstItem
};

/**
* @param {Number} startRowIndex the start index to get the list
* @param {Number} limit the number of items to get
* @param {Object} filters not yet implemented
* @param {EventstoreStateListDoneCallback} cb the callback to call after the operation
* @returns {void} - returns void. use the cb to get results
*/
EventstoreStateListInMemoryStore.prototype.filter = async function(listName, lastId, filters) {
  const items = [];
  const list = this._lists[listName];
  if (list) {
      let firstIndexList = {};
      let iterations = [];
      for (let i = 0; i < filters.length; i++) {
          const filter = filters[i];
          if (filter.operator !== 'is') {
              throw new Error(`operator ${filter.operator} is not supported`);
          }
          if (!this._listIndices[listName][filter.field] || !this._listIndices[listName][filter.field][filter.value]) {
              break;
          }
          const listIndexFieldItems = this._listIndices[listName][filter.field][filter.value];
          if (i === 0) {
              firstIndexList = listIndexFieldItems;
          } else {
              iterations.push(listIndexFieldItems)
          }
      }
      for (const key in firstIndexList) {
          if (Object.hasOwnProperty.call(firstIndexList, key)) {
              let countIndex = 0;
              let existsOnAll = true;
              while (countIndex < iterations.length) {
                  const iterationResult = iterations[countIndex];
                  if (!iterationResult[key]) {
                      existsOnAll = false;
                      break;
                  }
                  countIndex++;
              }
              const keyValue = firstIndexList[key]
              if (existsOnAll && this._lists[listName][keyValue]) {
                  const rowValue = this._lists[listName][keyValue];
                  if (rowValue) {
                    items.push({
                        index: key,
                        meta: rowValue ? rowValue.meta : undefined,
                        value: rowValue ? rowValue.state : undefined
                    });
                  }
              }
          }
      }
  }

  return _.uniq(items);
}


/**
 * @param {String} listName the name of the list
 * @returns {Promise}
 */
// eslint-disable-next-line no-unused-vars
EventstoreStateListInMemoryStore.prototype.truncate = async function(listName) {
    throw new Error('not implemented');
};

/**
 * @param {String} listName the name of the list
 * @returns {Promise}
 */
EventstoreStateListInMemoryStore.prototype.destroy = async function(listName) {
    this._configs[listName] = {};
    this._lists[listName] = [];
    this._lastIndices[listName] = {};
    this._listIndices[listName] = {};
};

/**
 * @param {String} listName the name of the list
 * @returns {Promise}
 */
// eslint-disable-next-line no-unused-vars
EventstoreStateListInMemoryStore.prototype.deleteList = async function(listName) {
    throw new Error('not implemented');
};

module.exports = EventstoreStateListInMemoryStore;
