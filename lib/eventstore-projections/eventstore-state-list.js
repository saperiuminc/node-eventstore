const shortid = require('shortid');
const _ = require('lodash');
const debug = require('debug')('eventstore:state-list')
const mysql = require('mysql');

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
 * @property {Object} mysql the mysql library
 * @property {String} host the mysql host
 * @property {Number} port the mysql port
 * @property {String} user the mysql user
 * @property {String} password the mysql password
 * @property {String} database the mysql database name
 * @property {String} listName the name of this list
 * @property {Object.<string, EventstoreStateListSecondaryKey[]>} secondaryKeys the secondary keys that make up the non clustered index. A key value pair were key is the secondaryKey name and value an array of fields
 * @property {EventstoreStateListField[]} fields the secondary keys that make up the non clustered index
 */

/**
 * @param {EventstoreStateListOptions} options additional options for the Eventstore state list
 * @constructor
 */
function EventstoreStateList(options) {
    options = options || {};
    var defaults = {
        host: '127.0.0.1',
        port: 3306,
        user: 'root',
        password: 'root',
        database: 'eventstore',
        listName: 'list_name',
        fields: [],
        secondaryKeys: {}
    };

    this.options = _.defaults(options, defaults);
}

/**
 * @type {EventstoreStateListOptions}
 */
EventstoreStateList.prototype.options;

EventstoreStateList.prototype._connection;

/**
 * @type {EventstoreStateListData}
 */
EventstoreStateList.prototype._data;

/**
 * @param {EventstoreStateListDoneCallback} cb callback to be called when the query is done retrieving data
 * @returns {void} - returns void. use the callback for the result (cb)
 */
EventstoreStateList.prototype.init = async function() {
    // undefined callbacks are ok for then() and catch() because it checks for undefined functions
    try {
        debug('init called');

        this._data = {};

        this._connection = mysql.createConnection({
            host: this.options.host,
            port: this.options.port,
            user: this.options.user,
            password: this.options.password,
            database: this.options.database
        });

        // build the create table script
        let fieldsString = '';
        const fields = this.options.fields;
        if (fields && fields.length > 0) {
            fieldsString += ',';
            fields.forEach(function(field, index) {
                switch (field.type) {
                    case 'string': {
                        fieldsString += `\`${field.name}\` varchar(250) GENERATED ALWAYS AS (\`state_json\` ->> '$.${field.name}')`;
                        break;
                    }

                    case 'int': {
                        fieldsString += `\`${field.name}\` INT GENERATED ALWAYS AS (\`state_json\` ->> '$.${field.name}')`;
                        break;
                    }
                    case 'boolean': {
                        fieldsString += `\`${field.name}\` BOOLEAN GENERATED ALWAYS AS (\`state_json\` ->> '$.${field.name}')`;
                        break;
                    }
                }

                // everything is nullable
                fieldsString += ' NULL';

                // add trailing commas if not yet the last field
                if (index < fields.length - 1) {
                    fieldsString += ', ';
                }
            });
        }

        let secondaryKeyString = '';

        if (this.options.secondaryKeys) {
            for (var key in this.options.secondaryKeys) {
                if (Object.prototype.hasOwnProperty.call(this.options.secondaryKeys, key)) {
                    const keyObject = this.options.secondaryKeys[key];
                    secondaryKeyString += `, KEY \`${key}\` (`
                    keyObject.forEach((field, index) => {
                        secondaryKeyString += `\`${field.name}\``;

                        if (index < keyObject.length - 1) {
                            secondaryKeyString += ', ';
                        } else {
                            secondaryKeyString += ')';
                        }
                    });
                }
            }
        }

        const query = `CREATE TABLE IF NOT EXISTS ${this.options.listName} 
            (
                id INT AUTO_INCREMENT PRIMARY KEY,
                row_type VARCHAR(10) NOT NULL,
                row_index INT,
                row_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                state_json JSON,
                meta_json JSON
                ${fieldsString} 
                ${secondaryKeyString}
            );`;

        await this._executeSqlQuery(query);
    } catch (error) {
        console.error('error in _init with error:', error);
        throw error;
    }
};

/**
 * @param {Number} index the index of the row to delete
 * @param {EventstoreStateListDoneCallback} cb the callback to call after the operation
 * @returns {void} - returns void. use the cb to get results
 */
EventstoreStateList.prototype.delete = function(index, cb) {
    this._delete(index).then(cb).catch(cb);
};

/**
 * @param {Number} index the index of the row to delete
 * @param {Object} state the new state of the item to update
 * @param {EventstoreStateListDoneCallback} cb the callback to call after the operation
 * @returns {void} - returns void. use the cb to get results
 */
EventstoreStateList.prototype.set = function(index, state, cb) {
    // push on end
    this._set(index, state).then(function(err, lastId) {
        cb(null, lastId);
    }).catch(cb);
};

/**
 * @param {Object} state the state of the item to add
 * @param {EventstoreStateListDoneCallback} cb the callback to call after the operation
 * @returns {void} - returns void. use the cb to get results
 */
EventstoreStateList.prototype.push = function(state, cb) {
    // push on end
    this._push(state).then(function(data) {
        cb(null, data);
    }).catch(cb);
};

/**
 * @param {EventstoreStateListDoneCallback} cb the callback to call after the operation
 * @returns {void} - returns void. use the cb to get results
 */
EventstoreStateList.prototype.count = function(cb) {
    this._count().then(cb).catch(cb);
};


/**
 * @param {Number} startRowIndex the start index to get the list
 * @param {Number} limit the number of items to get
 * @param {Object} filters not yet implemented
 * @param {Object} sorting not yet implemented
 * @param {EventstoreStateListDoneCallback} cb the callback to call after the operation
 * @returns {void} - returns void. use the cb to get results
 */
EventstoreStateList.prototype.find = function(lastId, filters, cb) {
    this._filter(lastId, null, 1, filters).then(function(rows) {
        if (rows && rows.length > 0) {
            cb(null, rows[0].state, rows[0].rowIndex);
        } else {
            cb();
        }
    }).catch(cb);
};


/**
 * @param {Number} startRowIndex the start index to get the list
 * @param {Number} limit the number of items to get
 * @param {Object} filters not yet implemented
 * @param {Object} sorting not yet implemented
 * @param {EventstoreStateListDoneCallback} cb the callback to call after the operation
 * @returns {void} - returns void. use the cb to get results
 */
EventstoreStateList.prototype.filter = function(lastId, startRowIndex, limit, filters, sorting, cb) {
    this._filter(lastId, startRowIndex, limit, filters, sorting).then(function(rows) {
        if (rows) {
            cb(null, rows.map((item) => item.state));
        } else {
            cb();
        }
    }).catch(cb);
};

/**
 * @param {EventStoreStateListRowType} type the type of row to insert. values are DELETE, UPDATE CREATE
 * @param {Number} index the index to insert the new row
 * @param {Object} state the state to insert
 * @param {EventstoreStateListDoneCallback} cb the callback to call after the operation
 * @returns {void} - returns void. use the cb to get results
 */
EventstoreStateList.prototype._insertRow = async function(type, index, state) {
    try {
        var rowData = {
            row_type: type,
            row_index: index,
            state_json: (state ? JSON.stringify(state) : null)
        };
        const results = await this._executeSqlQuery(`INSERT INTO ${this.options.listName} SET ?`, rowData);
    
        return results.insertId;
    } catch (error) {
        console.error('error in _insertRow with params and error', type || 'null', index || 'null', state || 'null', error);
        throw error;
    }
};

/**
 * @param {String} queryText callback to be called when the query is done retrieving data
 * @returns {Promise<Object>} - returns a Promise of type Object where object is the Query result
 */
EventstoreStateList.prototype._executeSqlQuery = async function(queryText, queryParams) {
    return new Promise((resolve, reject) => {
        try {
            this._connection.query(queryText, queryParams, function(err, results) {
                if (err) {
                    console.error(err);
                    reject(err);
                } else {
                    resolve(results);
                }
            });
        } catch (error) {
            console.error('error in _doMySqlQuery with params and error:', queryText, error);
            reject(error);
        }
    })
};

/**
 * @param {Number} index the index of the row to delete
 * @returns {Promise<void>} - returns a Promise of type void
 */
EventstoreStateList.prototype._delete = async function(index) {
    try {
        await this._insertRow('DELETE', index, null);
    } catch (error) {
        console.error('error in stateList._delete with params and error', index || 'null', error);
        throw error;
    }
};

/**
 * @param {Number} index the index of the row to delete
 * @param {Object} state the new state of the item to update
 * @returns {Promise<void>} - returns Promise of type void
 */
EventstoreStateList.prototype._set = async function(index, state) {
    try {
        return await this._insertRow('UPDATE', index, state);
    } catch (error) {
        console.error('error in stateList._set with params and error', index || 'null', state || 'null', error);
        throw error;
    }
};


/**
 * @param {Object} state the state of the item to add
 * @returns {Promise<void>} - returns Promise of type void
 */
EventstoreStateList.prototype._push = async function(state) {
    // push on end
    try {
        const nextRowIndex = await this._nextRowIndex();
        return await this._insertRow('CREATE', nextRowIndex, state);
    } catch (error) {
        console.error('error in stateList._push with params and error', state || 'null', error);
        throw error;
    }
};

/**
 * @returns {Promise<Number>} - returns the count in a promise
 */
EventstoreStateList.prototype._count = async function() {
    try {
        const lastId = (isNaN(this._data.lastId) ? 0 : this._data.lastId);
        const countQuery = `
                        SELECT
                            MAX(row_index) as max_row_index
                        FROM 
                            ${this.options.listName}`;

        const resultCount = await this._executeSqlQuery(countQuery);
        const count = (resultCount.length > 0 && !isNaN(parseInt(resultCount[0].max_row_index)) ? (resultCount[0].max_row_index + 1) : 0);
        return count;
    } catch (error) {
        console.error('error in stateList._count with error', error);
        throw error;
    }
};

/**
 * @returns {Promise<Number>} - returns the count in a promise
 */
EventstoreStateList.prototype._nextRowIndex = async function() {
    try {
        const lastId = (isNaN(this._data.lastId) ? 0 : this._data.lastId);
        const countQuery = `
                        SELECT
                            MAX(row_index) as max_row_index
                        FROM 
                            ${this.options.listName}`;

        const resultCount = await this._executeSqlQuery(countQuery);
        const count = (resultCount.length > 0 && !isNaN(parseInt(resultCount[0].max_row_index)) ? (resultCount[0].max_row_index + 1) : 0);
        return count;
    } catch (error) {
        console.error('error in stateList._count with error', error);
        throw error;
    }
};

/**
 * @param {Number} startRowIndex the start index to get the list
 * @param {Number} limit the number of items to get
 * @param {Object} filters not yet implemented
 * @param {Object} sorting not yet implemented
 * @returns {Promise<Array>} - returns an array of rows
 */
EventstoreStateList.prototype._filter = async function(lastId, startRowIndex, limit, filters, sorting) {
    try {
        // TODO: implement filters and sorting
        const additionalFilters = this._listFiltersToFilterString(filters);
        let rowIndexFilter = '';
        if (startRowIndex) {
            rowIndexFilter = `AND row_index >= ${startRowIndex}`;
        }
        let limitFilter = '';
        if (limit) {
            `LIMIT ${limit}`;
        }

        let idFilter = '';
        if (lastId) {
            idFilter = `AND id <= ${lastId}`
        }

        var getQuery = `

            SELECT
                list.state_json,
                list.row_index
            FROM
                (
                    SELECT
                        row_index,
                        MAX(id) as id
                    FROM 
                        ${this.options.listName}
                    WHERE 
                        1 = 1
                        ${idFilter}
                        ${rowIndexFilter}
                        ${additionalFilters}
                    GROUP BY row_index
                ) rows
            JOIN
                ${this.options.listName} list ON list.id = rows.id
            WHERE
                    list.row_type <> "DELETE"
            ORDER BY
                list.row_index
            ${limitFilter}
        `;

        //console.log(getQuery);


        const results = await this._executeSqlQuery(getQuery);

        // translate count/results
        const rows = results.map((x) => {
            return {
                rowIndex: x.row_index,
                state: x.state_json ? JSON.parse(x.state_json) : undefined
            }
        });
        return rows;
    } catch (error) {
        console.error('error in stateList.get with parameters and error', start || 'null', limit || 'null', filters || 'null', sorting || 'null', error);
        throw error;
    }
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