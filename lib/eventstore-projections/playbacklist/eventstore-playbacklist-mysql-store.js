const _ = require('lodash');
const debug = require('debug')('eventstore:playback-list-mysql-store');
const util = require('util');
const mysqlsharedpool = require('@saperiuminc/mysql2-shared-pool');
const BaseEventStorePlaybacklistStore = require('./base.eventstore-playbacklist-store');

/**
 * @param {BaseEventStorePlaybacklistStore.EventstorePlaybackListOptions} options additional options for the Eventstore playback list
 * @constructor
 */
function EventstorePlaybackListMySqlStore(options) {
  options = options || {
      connection: {},
      pool: {}
  };
  options.connection = options.connection || {};
  options.pool = options.pool || {};
  const defaultConnection = {
      host: '127.0.0.1',
      port: 3306,
      user: 'root',
      password: 'root',
      database: 'eventstore',
  };
  const defaultPool = {
      min: 1,
      max: 1,
      idleTimeoutMillis: 30000
  };
  options.connection = _.defaults(options.connection, defaultConnection);
  options.pool = _.defaults(options.pool, defaultPool);
  this.options = options;

  
  return this;
}


util.inherits(EventstorePlaybackListMySqlStore, BaseEventStorePlaybacklistStore);
/**
 * @type {BaseEventStorePlaybacklistStore.EventstorePlaybackListOptions}
 */
EventstorePlaybackListMySqlStore.prototype.options;

EventstorePlaybackListMySqlStore.prototype._pool;

/**
 * @param {BaseEventStorePlaybacklistStore.EventstorePlaybackListQueryDoneCallback} cb callback to be called when the query is done retrieving data
 * @returns {void} - returns void. use the callback for the result (cb)
 */
EventstorePlaybackListMySqlStore.prototype.init = async function() {
    // undefined callbacks are ok for then() and catch() because it checks for undefined functions
    try {
        debug('init called');

        if (!this.options.connection.host) {
            throw new Error('host is required to be passed as part of the options');
        }

        if (!this.options.connection.port) {
            throw new Error('port is required to be passed as part of the options');
        }

        if (!this.options.connection.user) {
            throw new Error('user is required to be passed as part of the options');
        }

        if (!this.options.connection.password) {
            throw new Error('password is required to be passed as part of the options');
        }

        if (!this.options.connection.database) {
            throw new Error('database is required to be passed as part of the options');
        }

        this._pool = mysqlsharedpool.createPool({
            host: this.options.connection.host,
            port: this.options.connection.port,
            database: this.options.connection.database,
            user: this.options.connection.user,
            password: this.options.connection.password,
            connectionLimit: this.options.pool.max,
            multipleStatements: true
        });

    } catch (error) {
        console.error('error in _init with error:', error);
        throw error;
    }
};


/**
 * @returns {void} - returns void Promise
 */
EventstorePlaybackListMySqlStore.prototype.close = async function() {
    // undefined callbacks are ok for then() and catch() because it checks for undefined functions
    try {
        debug('close called');
        await this._pool.end();
    } catch (error) {
        console.error('error in close with error:', error);
        throw error;
    }
};

/**
 * @param {Object} listOptions varies depending on the need of the store. implementors outside of the library can send any options to the playbacklist
 * and everything will just be forwarded to the createList as part of the listOptions
 * @returns {void} - returns void. use the callback for the result (cb)
 */
EventstorePlaybackListMySqlStore.prototype.createList = async function(listOptions) {
    // undefined callbacks are ok for then() and catch() because it checks for undefined functions
    try {
        debug('createList called');

        // build the create table script
        let fieldsString = '';
        const fields = listOptions.fields;
        if (fields && fields.length > 0) {
            fieldsString += ',';
            fields.forEach(function(field, index) {
                switch (field.type) {
                    case 'string': {
                        let size = 50;
                        if(field.size) {
                            size = field.size;
                        }

                        fieldsString += `\`${field.name}\` varchar(${size}) GENERATED ALWAYS AS (\`row_json\` ->> '$.${field.name}')`;
                        break;
                    }

                    case 'int': {
                        fieldsString += `\`${field.name}\` INT GENERATED ALWAYS AS (\`row_json\` ->> '$.${field.name}')`;
                        break;
                    }
                    case 'boolean': {
                        fieldsString += `\`${field.name}\` BOOLEAN GENERATED ALWAYS AS (\`row_json\` ->> '$.${field.name}')`;
                        break;
                    }
                    case 'bigint': {
                        fieldsString += `\`${field.name}\` BIGINT GENERATED ALWAYS AS (\`row_json\` ->> '$.${field.name}')`;
                        break;
                    }
                    case 'decimal': {
                        fieldsString += `\`${field.name}\` DECIMAL(10,2) GENERATED ALWAYS AS (\`row_json\` ->> '$.${field.name}')`;
                        break;
                    }
                    case 'date': {
                        fieldsString += `\`${field.name}\` DATE GENERATED ALWAYS AS (DATE(\`row_json\` ->> '$.${field.name}'))`;
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

        if (listOptions.secondaryKeys) {
            for (var key in listOptions.secondaryKeys) {
                if (Object.prototype.hasOwnProperty.call(listOptions.secondaryKeys, key)) {
                    const keyObject = listOptions.secondaryKeys[key];
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

        const query = `CREATE TABLE IF NOT EXISTS \`${listOptions.name}\`
            (
                row_id VARCHAR(250) PRIMARY KEY,
                row_revision INT,
                row_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                row_json JSON,
                meta_json JSON
                ${fieldsString} 
                ${secondaryKeyString}
            );`;

        await this._querySql(query);
    } catch (error) {
        console.error('error in _init with error:', error);
        throw error;
    }
};

/**
 * @param {String} listName the name of the list
 * @returns {Promise}
 */
EventstorePlaybackListMySqlStore.prototype.deleteList = async function(listName) {
    return this._querySql(`DROP TABLE ${listName};`);
};

/**
 * @param {String} queryText sql query string to be executed
 * @returns {Promise<Object>} - returns a Promise of type Object where object is the Query result
 */
EventstorePlaybackListMySqlStore.prototype._querySql = async function(queryText, queryParams) {
    const resultRaw = await this._pool.query(queryText, queryParams);
    return resultRaw[0];
};

/**
 * @param {String} queryText sql query string to be executed
 * @returns {Promise<Object>} - returns a Promise of type Object where object is the Query result
 */
 EventstorePlaybackListMySqlStore.prototype._executeSql = async function(queryText, queryParams) {
    const resultRaw = await this._pool.execute(queryText, queryParams);
    return resultRaw[0];
};

/**
 * @param {BaseEventStorePlaybacklistStore.EventstorePlaybackListFilter[]} filters filter parameters for the query
 * @returns {String} - returns the filters in string
 */
EventstorePlaybackListMySqlStore.prototype._listFiltersToFilterString = function(filters) {
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
 * @param {BaseEventStorePlaybacklistStore.EventstorePlaybackListSort[]} sorting sort parameters for the query
 * @returns {String} - the sorting in string
 */
EventstorePlaybackListMySqlStore.prototype._listSortingToSortingString = function(sorting) {
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
 * @param {BaseEventStorePlaybacklistStore.EventstorePlaybackListFilter[]} filters filter parameters for the query
 * @param {String} groupBooleanOperator group operator "or" or "and"
 * @returns {String} - returns the filters in string
 */
EventstorePlaybackListMySqlStore.prototype._appendFiltersToFilterString = function(filterString, filters, groupBooleanOperator) {
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
                  const inFilter = `'${filterValue}'`;
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

/**
 * @param {String} listName the name of the list
 * @param {Number} start zero-index start position of the rows to get
 * @param {Number} limit how many rows we want to get
 * @param {BaseEventStorePlaybacklistStore.EventstorePlaybackListFilter[]} filters filter parameters for the query
 * @param {BaseEventStorePlaybacklistStore.EventstorePlaybackListSort[]} sort sort parameters for the query
 * @returns {Promise<EventstorePlaybackListQueryResult} - returns void. use the callback for the result (cb)
 */
EventstorePlaybackListMySqlStore.prototype.query = async function(listName, start, limit, filters, sort) {
    try {
        debug('query called with params:', start, limit, filters, sort);
        
        let filterString = this._listFiltersToFilterString(filters);
        let orderByString = this._listSortingToSortingString(sort);

        const countQuery = `SELECT COUNT(1) as total_count FROM ${listName} WHERE 1 = 1 ${filterString}`;

        const getQuery = `SELECT * FROM ${listName} WHERE 1 = 1 ${filterString} ${orderByString} LIMIT ?,?`;
        const whereParams = [
            start,
            limit
        ];

        const resultCount = await this._querySql(countQuery);
        const resultRows = await this._querySql(getQuery, whereParams);

        const data = {
            count: (resultCount.length > 0 ? resultCount[0].total_count : 0),
            rows: resultRows.map(function(x) {
                return {
                    rowId: x.row_id,
                    revision: x.row_revision,
                    data: (x.row_json ? x.row_json : undefined),
                    meta: (x.meta_json ? x.meta_json : undefined)
                };
            })
        }

        return data;
    } catch (error) {
        console.error('error in query with params and error:', start, limit, filters, sort, error);
        throw error;
    }
};


/**
 * @param {String} listName the name of the list
 * @param {String} rowId the unique id for this row
 * @param {Number} revision the row revision
 * @param {Object} data the data to add
 * @param {Object} meta optional metadata
 * @returns {Promise<void>} - returns Promise of type void
 */
EventstorePlaybackListMySqlStore.prototype.add = async function(listName, rowId, revision, data, meta) {
    try {
        debug('add called with params:', rowId, revision, data, meta);
        var rowData = [
            rowId,
            revision,
            data ? JSON.stringify(data) : null,
            meta ? JSON.stringify(meta) : null
        ];
        await this._executeSql(`INSERT IGNORE INTO \`${listName}\` (row_id, row_revision, row_json, meta_json) VALUES (?, ?, ?, ?)`, rowData);
    } catch (error) {
        console.error('error in add with params and error:', rowId, revision, data, meta, error);
        throw error;
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
EventstorePlaybackListMySqlStore.prototype.update = async function(listName, rowId, revision, data, meta) {
    try {
        debug('update called with params:', rowId, revision, data, meta);

        var updateParams = [
            revision,
            (data ? JSON.stringify(data) : null),
            (meta ? JSON.stringify(meta) : null),
            rowId
        ];

        await this._executeSql(`UPDATE ${listName} SET row_revision = ?, row_json = ?, meta_json = ?  WHERE row_id = ?`, updateParams);

    } catch (error) {
        console.error('error in update with params and error:', rowId, revision, data, meta, error);
        throw error;
    }
};


 EventstorePlaybackListMySqlStore.prototype.batchUpdate = async function(listName, filters, newData, meta) {
    try {
        debug('batchUpdate called with params:', filters, newData, meta);
        
        let filterString = this._listFiltersToFilterString(filters);

        if (filterString) {
            const updateQuery = `UPDATE ${listName} SET row_json = JSON_MERGE_PATCH(row_json, ?), meta_json = JSON_MERGE_PATCH(meta_json, ?) WHERE 1 = 1 ${filterString}`;
            const updateParams = [
                JSON.stringify(newData),
                JSON.stringify(meta)
            ];
    
            await this._querySql(updateQuery, updateParams);
        }
        
    } catch (error) {
        console.error('error in query with params and error:', filters, newData, meta, error);
        throw error;
    }
};


/**
 * @param {String} listName the name of the list
 * @param {String} rowId the unique id for this row
 * @returns {Promise<void>} - returns Promise of type void
 */
EventstorePlaybackListMySqlStore.prototype.delete = async function(listName, rowId) {
    try {
        debug('delete called with params:', rowId);
        var whereParams = [
            rowId
        ];
        // NOTE: do actual/hard-deletes
        await this._executeSql(`DELETE FROM ${listName} WHERE row_id = ?`, whereParams);
    } catch (error) {
        console.error('error in delete with params and error:', rowId, error);
        throw error;
    }
};


 /**
  * 
  * @param {string} listName 
  */
 EventstorePlaybackListMySqlStore.prototype.truncate = async function(listName) {
    try {
        debug('truncate called');
        // NOTE: do actual/hard-deletes
        await this._querySql(`TRUNCATE TABLE ${listName};`);
    } catch (error) {
        console.error('error in truncate with params and error:', error);
        throw error;
    }
};


 /**
  * 
  * @param {string} listName 
  */
  EventstorePlaybackListMySqlStore.prototype.destroy = async function(listName) {
    try {
        debug('destroy called');
        // NOTE: do actual/hard-deletes
        await this._querySql(`DROP TABLE IF EXISTS ${listName};`);
    } catch (error) {
        console.error('error in destroy with params and error:', error);
        throw error;
    }
};

/**
 * @param {String} listName the name of the list
 * @param {String} rowId the unique id for this row
 * @param {BaseEventStorePlaybacklistStore.EventstorePlaybackListDoneCallback} cb callback to call when operation is done
 * @returns {Promise<void>} - returns Promise of type void
 */
EventstorePlaybackListMySqlStore.prototype.get = async function(listName, rowId) {
    try {
        debug('get called with params:', rowId);

        const getQuery = `
                            SELECT
                                row_revision,
                                row_json,
                                meta_json
                            FROM ${listName}
                            WHERE row_id = ? LIMIT 1`;
        var whereParams = [
            rowId
        ];
        const results = await this._executeSql(getQuery, whereParams);

        if (results.length > 0) {
            const rowData = results[0];
            return {
                data: rowData.row_json ? rowData.row_json : null,
                meta: rowData.meta_json ? rowData.meta_json : null,
                revision: rowData.row_revision,
                rowId: rowId
            };
        }

        return null;
    } catch (error) {
        console.error('error in get with params and error:', rowId, error);
        throw error;
    }
};

module.exports = EventstorePlaybackListMySqlStore;
