const debug = require('debug')('eventstore:playback-list-redis-store');
const util = require('util');
const _ = require('lodash');
const BaseEventStorePlaybacklistStore = require('./base.eventstore-playbacklist-store');

/**
 * @param {BaseEventStorePlaybacklistStore.EventstorePlaybackListOptions} options additional options for the Eventstore playback list
 * @constructor
 */
function EventstorePlaybackListInMemoryStore(options) {
  options = options || {};

  this.options = options;
  this._configs = {};
  this._listIndices = {};
}


util.inherits(EventstorePlaybackListInMemoryStore, BaseEventStorePlaybacklistStore);

EventstorePlaybackListInMemoryStore.prototype._lists;

/**
 * @param {BaseEventStorePlaybacklistStore.EventstorePlaybackListQueryDoneCallback} cb callback to be called when the query is done retrieving data
 * @returns {void} - returns void. use the callback for the result (cb)
 */
EventstorePlaybackListInMemoryStore.prototype.init = async function() {
    this._lists = {};
};


/**
 * @returns {void} - returns void Promise
 */
EventstorePlaybackListInMemoryStore.prototype.close = async function() {
    debug('close called');
};

/**
 * @param {Object} listOptions varies depending on the need of the store. implementors outside of the library can send any options to the playbacklist
 * and everything will just be forwarded to the createList as part of the listOptions
 * @returns {void} - returns void. use the callback for the result (cb)
 */
// eslint-disable-next-line no-unused-vars
EventstorePlaybackListInMemoryStore.prototype.createList = async function(listOptions) {
    // undefined callbacks are ok for then() and catch() because it checks for undefined functions
    this._lists[listOptions.name] = {};
    this._listIndices[listOptions.name] = {};
    this._configs[listOptions.name] = listOptions;
};

/**
 * @param {String} listName the name of the list
 * @returns {Promise}
 */
EventstorePlaybackListInMemoryStore.prototype.deleteList = async function(listName) {
    delete this._lists[listName];
    delete this._configs[listName];
    delete this._listIndices[listName];
    
};

/**
 * @param {String} listName the name of the list
 * @param {Number} start zero-index start position of the rows to get
 * @param {Number} limit how many rows we want to get
 * @param {BaseEventStorePlaybacklistStore.EventstorePlaybackListFilter[]} filters filter parameters for the query
 * @param {BaseEventStorePlaybacklistStore.EventstorePlaybackListSort[]} sort sort parameters for the query
 * @returns {Promise<EventstorePlaybackListQueryResult} - returns void. use the callback for the result (cb)
 */
// eslint-disable-next-line no-unused-vars
EventstorePlaybackListInMemoryStore.prototype.query = async function(listName, start, limit, filters, sort) {
    let limitedItems = [];
    const items = [];
    const list = this._lists[listName];
    if (list) {
      if (filters && filters.length) {
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
                        rowId: key,
                        revision: rowValue.revision,
                        data: (rowValue.data ? rowValue.data : undefined),
                        meta: (rowValue.meta ? rowValue.meta : undefined)
                    });
                  }
              }
          }
        }
      } else { // if no filters, just return all
        for (const rowId in list) {
          if (Object.hasOwnProperty.call(list, rowId)) {
            const row = list[rowId];
            items.push({
              rowId: rowId,
              revision: row.revision,
              data: (row.data ? row.data : undefined),
              meta: (row.meta ? row.meta : undefined)
            });
          }
        }
      }

      if (sort && sort.length) {
        const sortFields = _.map(sort, (s) => s.field);
        const finalSortFields = Object.values(sortFields);
        const sortType = _.map(sort, (s) => s.sort.toLowerCase());
        const finalSortType = Object.values(sortType);
        const sortedItems = _.orderBy(_.uniq(items), finalSortFields, finalSortType);

        limitedItems = sortedItems.slice(start, (start + limit));
      } else {
        limitedItems = items.slice(start, (start + limit));
      }
    }
    
    return {
      count: limitedItems.length,
      rows: limitedItems
    };
};


/**
 * @param {String} listName the name of the list
 * @param {String} rowId the unique id for this row
 * @param {Number} revision the row revision
 * @param {Object} data the data to add
 * @param {Object} meta optional metadata
 * @returns {Promise<void>} - returns Promise of type void
 */
EventstorePlaybackListInMemoryStore.prototype.add = async function(listName, rowId, revision, data, meta) {
    this._lists[listName][rowId] = {
        revision: revision,
        data: data,
        meta: meta
    }

    const playbackListConfig = this._configs[listName];
    if (playbackListConfig.secondaryKeys) {
      for (const key in playbackListConfig.secondaryKeys) {
          if (Object.prototype.hasOwnProperty.call(playbackListConfig.secondaryKeys, key)) {
              const keyObject = playbackListConfig.secondaryKeys[key];

              for (let i = 0; i < keyObject.length; i++) {
                  const field = keyObject[i];
                  const fieldValue = data[field.name];
                  if (fieldValue !== undefined) {
                      let listIndexForField = this._listIndices[listName][field.name];

                      if (!listIndexForField) {
                          this._listIndices[listName][field.name] = listIndexForField = {};
                      }

                      let listIndexForFieldItems = listIndexForField[fieldValue];

                      if (!listIndexForFieldItems) {
                          listIndexForField[fieldValue] = listIndexForFieldItems = {}
                      }

                      listIndexForFieldItems[rowId] = rowId;
                  }
              }
          }
      }
  }

};


/**
 * @param {String} listName the name of the list
 * @param {String} rowId the unique id for this row
 * @param {Number} revision the row revision
 * @param {Object} oldData Old data that we got from get
 * @param {Object} newData New data to persist
 * @param {Object} meta optional metadata
 * @returns {Promise<void>} - returns Promise of type void
 */
EventstorePlaybackListInMemoryStore.prototype.update = async function(listName, rowId, revision, data, meta) {
    this._lists[listName][rowId] = {
        revision: revision,
        data: data,
        meta: meta
    }

    this._updateIndices(listName, rowId, data);
};

EventstorePlaybackListInMemoryStore.prototype.batchUpdate = async function(listName, filters, newData, meta) {
  // merge new data with old data
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
                // update list row
                const oldRowValue = this._lists[listName][keyValue];
                const newRowData = Object.assign(oldRowValue.data, newData);
                this._lists[listName][keyValue].data = newRowData;
                if (meta) {
                  this._lists[listName][keyValue].meta = Object.assign(oldRowValue.meta, meta);
                }

                // update listIndices
                this._updateIndices(listName, keyValue, newRowData)
              }
          }
      }
  }
};

/**
 * @param {String} listName the name of the list
 * @param {String} rowId the unique id for this row
 * @param {BaseEventStorePlaybacklistStore.EventstorePlaybackListFilter[]} filters filter parameters for the query
 * @returns {String} - returns the filters in string
 */
 EventstorePlaybackListInMemoryStore.prototype._updateIndices = function(listName, rowId, rowData) {

  const playbackListConfig = this._configs[listName];
    
  if (playbackListConfig.secondaryKeys) {
    for (const key in playbackListConfig.secondaryKeys) {
        if (Object.prototype.hasOwnProperty.call(playbackListConfig.secondaryKeys, key)) {

            const keyObject = playbackListConfig.secondaryKeys[key];

            for (let i = 0; i < keyObject.length; i++) {
                const field = keyObject[i];
                const fieldValue = rowData[field.name];
                if (fieldValue !== undefined) {
                    const lastFieldValue = this._lists[listName][rowId]['data'][field.name];

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
                            delete listIndexForField[lastFieldValue][rowId];
                        }

                        // reinsert
                        listIndexForFieldItems[rowId] = rowId;
                    }
                }
            }
        }
    }
  }
};


/**
 * @param {String} listName the name of the list
 * @param {String} rowId the unique id for this row
 * @returns {Promise<void>} - returns Promise of type void
 */
EventstorePlaybackListInMemoryStore.prototype.delete = async function(listName, rowId) {
  const item = this._lists[listName][rowId];
  if (item && item.data) {
    const playbackListConfig = this._configs[listName];
  
    if (playbackListConfig.secondaryKeys) {
      for (const key in playbackListConfig.secondaryKeys) {
          if (Object.prototype.hasOwnProperty.call(playbackListConfig.secondaryKeys, key)) {
  
              const keyObject = playbackListConfig.secondaryKeys[key];
  
              for (let i = 0; i < keyObject.length; i++) {
                  const field = keyObject[i];
                  const fieldValue = item.data[field.name];
                  if (fieldValue !== undefined) {
                      if (this._listIndices[listName][field.name] &&
                          this._listIndices[listName][field.name][fieldValue] &&
                          this._listIndices[listName][field.name][fieldValue][rowId]) {
                          // delete from index
                          delete this._listIndices[listName][field.name][fieldValue][rowId];
                      }
                  }
              }
          }
      }
    }
  }
    
  delete this._lists[listName][rowId];
};


 /**
  * 
  * @param {string} listName 
  */
 EventstorePlaybackListInMemoryStore.prototype.truncate = async function(listName) {
    this._lists[listName] = {};
    this._listIndices[listName] = {};
};


 /**
  * 
  * @param {string} listName 
  */
EventstorePlaybackListInMemoryStore.prototype.destroy = async function(listName) {
    this._lists[listName] = {};
    this._listIndices[listName] = {};
};

/**
 * @param {String} listName the name of the list
 * @param {String} rowId the unique id for this row
 * @param {BaseEventStorePlaybacklistStore.EventstorePlaybackListDoneCallback} cb callback to call when operation is done
 * @returns {Promise<void>} - returns Promise of type void
 */
EventstorePlaybackListInMemoryStore.prototype.get = async function(listName, rowId) {
    const row = this._lists[listName][rowId];
    if (row) {
        return {
          data: row.data,
          meta: row.meta,
          revision: parseInt(row.revision),
          rowId: rowId
        };
    }
    

    return null;
};

module.exports = EventstorePlaybackListInMemoryStore;
