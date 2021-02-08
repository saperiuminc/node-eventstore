const _ = require('lodash');
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
 * EventstoreStateListOptions
 * @typedef {Object} EventstoreStateListOptions
 * @property {String} listName the name of this list
 */


/**
 * @param {EventstorePlaybackListOptions} options additional options for the Eventstore playback list
 * @constructor
 */
function EventstoreStateList(store, listName, state) {
    this._store = store;
    this._state = state;
    this._listName = listName;
}


/**
 * @type {EventstoreStateListStore}
 */
EventstoreStateList.prototype._store;

/**
 * @type {EventstoreStateListState}
 */
EventstoreStateList.prototype._state;

/**
 * @type {String}
 */
EventstoreStateList.prototype._listName;


/**
 * @param {Number} index the index of the row to delete
 * @param {EventstoreStateListDoneCallback} cb the callback to call after the operation
 * @returns {void} - returns void. use the cb to get results
 */
EventstoreStateList.prototype.delete = function(index, cb) {
    if (cb) {
        this._delete(index).then(cb).catch(cb);
    } else {
        return this._delete(index);
    }
};


/**
 * @param {Number} index the index of the row to delete
 * @param {EventstoreStateListDoneCallback} cb the callback to call after the operation
 * @returns {void} - returns void. use the cb to get results
 */
EventstoreStateList.prototype._delete = async function(index) {
    try {
        // push to end
        const revision = await this._store.delete(this._listName, index);
        this._setRevision(revision);
    } catch (error) {
        console.error('error in statelist.set', error);
        throw error;
    } 
};

/**
 * @param {Number} index the index of the row to delete
 * @param {Object} state the new state of the item to update
 * @param {EventstoreStateListDoneCallback} cb the callback to call after the operation
 * @returns {void} - returns void. use the cb to get results
 */
EventstoreStateList.prototype.set = function(index, state, meta, cb) {
    if (cb) {
        // push on end
        this._set(index, state, meta).then(cb).catch(cb);
    } else {
        return this._set(index, state);
    }
};

/**
 * @param {Number} index the index of the row to delete
 * @param {Object} state the new state of the item to update
 * @param {EventstoreStateListDoneCallback} cb the callback to call after the operation
 * @returns {void} - returns void. use the cb to get results
 */
EventstoreStateList.prototype._set = async function(index, state, meta) {
    try {
        // push on end
        const revision = await this._store.set(this._listName, index, state, meta);
        this._setRevision(revision);
    } catch (error) {
        console.error('error in statelist.set', error);
        throw error;
    }
};


/**
 * @param {Object} state the state of the item to add
 * @param {EventstoreStateListDoneCallback} cb the callback to call after the operation
 * @returns {void} - returns void. use the cb to get results
 */
EventstoreStateList.prototype.push = function(state, meta, cb) {
    if (cb) {
        // push on end
        this._push(state, meta).then(cb).catch(cb);
    } else {
        return this._push(state);
    }
};


/**
 * @param {Object} state the state of the item to add
 * @param {EventstoreStateListDoneCallback} cb the callback to call after the operation
 * @returns {void} - returns void. use the cb to get results
 */
EventstoreStateList.prototype._push = async function(state, meta) {
    try {
        // push on end
        const revision = await this._store.push(this._listName, state, meta);
        this._setRevision(revision);
    } catch (error) {
        console.error('error in statelist.push', error);
        throw error;
    }
};

/**
 * @param {Number} startRowIndex the start index to get the list
 * @param {Number} limit the number of items to get
 * @param {Object} filters not yet implemented
 * @param {Object} sorting not yet implemented
 * @param {EventstoreStateListDoneCallback} cb the callback to call after the operation
 * @returns {void} - returns void. use the cb to get results
 */
EventstoreStateList.prototype.find = function(filters, cb) {
    if (cb) {
        // push on end
        this._find(this._getRevision(), filters).then((row) => {
            cb(null, row)
        }).catch(cb);
    } else {
        return this._find(this._getRevision(), filters);
    }
};


/**
 * @param {Number} startRowIndex the start index to get the list
 * @param {Number} limit the number of items to get
 * @param {Object} filters not yet implemented
 * @param {Object} sorting not yet implemented
 * @param {EventstoreStateListDoneCallback} cb the callback to call after the operation
 * @returns {void} - returns void. use the cb to get results
 */
EventstoreStateList.prototype._find = async function(revision, filters) {
    try {
        // push on end
        // row has value, index, meta
        const row = await this._store.find(this._listName, revision, filters);
        return row;
    } catch (error) {
        console.error('error in statelist.find', error);
        throw error;
    }
};

/**
 * @param {Number} startRowIndex the start index to get the list
 * @param {Number} limit the number of items to get
 * @param {Object} filters not yet implemented
 * @param {Object} sorting not yet implemented
 * @param {EventstoreStateListDoneCallback} cb the callback to call after the operation
 * @returns {void} - returns void. use the cb to get results
 */
EventstoreStateList.prototype.filter = function(filters, cb) {
    if (cb) {
        // push on end
        this._filter(this._getRevision(), filters).then((rows) => {
            cb(null, rows)
        }).catch(cb);
    } else {
        return this._filter(this._getRevision(), filters);
    }
};


/**
 * @param {Number} startRowIndex the start index to get the list
 * @param {Number} limit the number of items to get
 * @param {Object} filters not yet implemented
 * @param {Object} sorting not yet implemented
 * @param {EventstoreStateListDoneCallback} cb the callback to call after the operation
 * @returns {void} - returns void. use the cb to get results
 */
EventstoreStateList.prototype._filter = async function(revision, filters) {
    try {
        // rows is an array of state and rowInex. example [{ value: 'state', index: 1, meta: {} }]
        const rows = await this._store._filter(this._listName, revision, filters);
        return rows;
    } catch (error) {
        console.error('error in statelist.filter', error);
        throw error;
    }
};

/**
 * @param {EventstoreStateListDoneCallback} cb callback to be called when the query is done retrieving data
 * @returns {void} - returns void. use the callback for the result (cb)
 */
EventstoreStateList.prototype._setRevision = function(revision) {
    if (this._state) {
        this._state[this._listName] = revision;
    }
};


/**
 * @param {EventstoreStateListDoneCallback} cb callback to be called when the query is done retrieving data
 * @returns {void} - returns void. use the callback for the result (cb)
 */
EventstoreStateList.prototype._getRevision = function() {
    if (this._state && this._state[this._listName] > 0) {
        return this._state[this._listName];
    }

    return 0;
};


/**
 * @param {EventstorePlaybackListFilter[]} filters filter parameters for the query
 * @returns {String} - returns the filters in string
 */
EventstoreStateList.prototype._listFiltersToFilterString = function(filters) {
    let filterString = '';
  
      const filterGroups = _.groupBy(filters, (filter) => {
        return filter.group;
      });
  
      _.forOwn(filterGroups, (filterGroup) => {
        const filtersGroupedByBooleanOperator = _.groupBy(filterGroup, (filter) => {
          return filter.groupBooleanOperator || 'and'
        });
  
        _.forOwn(filtersGroupedByBooleanOperator, (filtersByOperator, booleanOperator) => {
          if (filtersByOperator && filtersByOperator.length > 0) {
            filterString += ' AND ';
            filterString = this._appendFiltersToFilterString(filterString, filtersByOperator, booleanOperator);
          }
        });
      });
      return filterString;
  };
  
  /**
   * @param {EventstorePlaybackListSort[]} sorting sort parameters for the query
   * @returns {String} - the sorting in string
   */
  EventstoreStateList.prototype._listSortingToSortingString = function(sorting) {
      let sortingString = '';
  
      if (sorting && sorting.length > 0) {
          sortingString = 'ORDER BY ';
          sorting.forEach(function(sort, index) {
              sortingString += `${sort.field} ${sort.sortDirection}`;
              if (index < sorting.length - 1) {
                  sortingString += ',';
              }
          });
      }
  
      return sortingString;
  };
  
  /**
   * @param {String} filterString the filter string
   * @param {EventstorePlaybackListFilter[]} filters filter parameters for the query
   * @param {String} groupBooleanOperator group operator "or" or "and"
   * @returns {String} - returns the filters in string
   */
  EventstoreStateList.prototype._appendFiltersToFilterString = function(filterString, filters, groupBooleanOperator) {
    if (filters && filters.length > 0) {
      filters.forEach(function(filter, index) {
        let filterValue = filter.value;
        if (typeof filter.value === 'string') {
          filterValue = (filter.value || '').replace(/'/g, '\'\'');
        } else if (Array.isArray(filter.value)) {
          filterValue = (filter.value || []).map((value) => {
            if (typeof value === 'string') {
              return (value || '').replace(/'/g, '\'\'');
            } else {
              return value;
            }
          });
        }
  
          let prefix = groupBooleanOperator === 'or' ? 'OR' : 'AND';
          let suffix = '';
          if (index === 0) {
            prefix = '(';
          }
  
          if (index === filters.length - 1) {
            suffix = ')';
          }
          
          switch (filter.operator) {
              case 'is':
                  {
                    const inFilter = !isNaN(filterValue) ? `${filterValue}` : `'${filterValue}'`;
                    filterString += ` ${prefix} ${filter.field} = ${inFilter} ${suffix} `;
                  }
                  break;
              case 'any':
                  {
                      if (filterValue && Array.isArray(filterValue) && filterValue.length > 0) {
                          let inFilter = '';
                          filterValue.forEach((v, index) => {
                              inFilter += `'${v}'`;
  
                              if (index < filterValue.length - 1) {
                                  inFilter += ',';
                              }
                          })
                          filterString += ` ${prefix} ${filter.field} IN (${inFilter}) ${suffix}`;
                      }
                  }
                  break;
              case 'range':
                  {
                      if (filter.from || filter.to) {
                        filterString += ` ${prefix}`;
                      }
  
                      if (filter.from) {
                          filterString += ` ${filter.field} >= ${filter.from} `;
                      }
  
                      if (filter.from && filter.to) {
                        filterString += ' AND';
                      }
  
                      if (filter.to) {
                          filterString += ` ${filter.field} <= ${filter.to} `;
                      }
  
                      filterString += `${suffix} `;
                  }
                  break;
              case 'dateRange':
                  {
                      if (filter.from || filter.to) {
                          filterString += ` ${prefix}`;
                      }
  
                      if (filter.from) {
                          filterString += ` ${filter.field} >= "${filter.from}" `;
                      }
  
                      if (filter.from && filter.to) {
                          filterString += ' AND';
                      }
  
                      if (filter.to) {
                          filterString += ` ${filter.field} <= "${filter.to}" `;
                      }
  
                      filterString += `${suffix} `;
                  }
                  break;
              case 'contains':
              {
                  if (filterValue) {
                      filterString += ` ${prefix} ${filter.field} LIKE '%${filterValue}%' ${suffix} `;
                  }
                  break;
              }
              case 'arrayContains':
              {
                  if (filterValue) {
                      filterString += ` ${prefix} `;
                      if (Array.isArray(filterValue)) {
                          filterValue.forEach((value, index) => {
                              filterString += ` JSON_CONTAINS(${filter.field}, '"${value}"')`;
                              if (index !== filterValue.length - 1) {
                                  filterString += ' OR ';
                              }
                          });
                      } else {
                          filterString += ` JSON_CONTAINS(${filter.field}, '"${filterValue}"')`;
                      }
                      filterString += ` ${suffix} `;
                  }
                  break;
              }
              case 'startsWith':
              {
                  if (filterValue) {
                      filterString += ` ${prefix} ${filter.field} LIKE '${filterValue}%' ${suffix} `;
                  }
                  break;
              }
              case 'endsWith':
              {
                  if (filterValue) {
                      filterString += ` ${prefix} ${filter.field} LIKE '%${filterValue}' ${suffix} `;
                  }
                  break;
              }
              case 'exists':
              {
                  filterString += ` ${prefix} ${filter.field} IS NOT NULL ${suffix} `;
                  break;
              }
              case 'notExists':
              {
                  filterString += ` ${prefix} ${filter.field} IS NULL ${suffix} `;
                  break;
              }
            }
      });
    }
    return filterString;
  }

module.exports = EventstoreStateList;
