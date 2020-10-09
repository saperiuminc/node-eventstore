const _ = require('lodash');
const debug = require('debug')('eventstore:playback-list-mysql-store');
const queries = require('debug')('eventstore:show-queries');

/**
 * EventstorePlaybackListQueryResultRow
 * @typedef {Object} EventstorePlaybackListQueryResultRow
 * @property {String} rowId The uniqueid of this row
 * @property {Number} revision The row revision
 * @property {Object} data The data at this revision
 * @property {Object} meta Some user metadata
 */

/**
 * EventstorePlaybackListQueryResult
 * @typedef {Object} EventstorePlaybackListQueryResult
 * @property {Number} count The total number of rows in the result
 * @property {EventstorePlaybackListQueryResultRow[]} rows The rows in the query
 */

/**
 * EventstorePlaybackListQueryDoneCallback
 * @callback EventstorePlaybackListQueryDoneCallback
 * @param {Error} error The event to playback
 * @param {EventstorePlaybackListQueryResult} result Callback to tell that playback is done consuming the event
 * @returns {void} Returns void
 */

/**
 * EventstorePlaybackListDoneCallback
 * @callback EventstorePlaybackListDoneCallback
 * @param {Error} error The error if any
 * @param {Object} result Result of this callback
 * @returns {void} Returns void
 */


 /**
 * EventstorePlaybackListFilter
 * @typedef {Object} EventstorePlaybackListFilter
 * @property {String} field the field to filter
 * @property {String?} group the group of the filter
 * @property {("or"|"and"|null)} groupBooleanOperator the operator for the group
 * @property {("is"|"any"|"range"|"contains"|"arrayContains"|"startsWith"|"endsWith"|"exists"|"notExists")} operator The operator to use. 
 * @property {Object?} value The value of the field. valid only for "is", "any" and "contains" operators
 * @property {Object?} from The lower limit value of the field. valid only for "range" operator
 * @property {Object?} to The upper limit value of the field. valid only for "range" operator
 */

/**
 * EventstorePlaybackListSort
 * @typedef {Object} EventstorePlaybackListSort
 * @property {String} field the field to sort
 * @property {("ASC"|"DESC")} sort the direction to sort
 */

/**
 * EventstorePlaybackListSecondaryKey
 * @typedef {Object} EventstorePlaybackListSecondaryKey
 * @property {String} name The name of the field
 * @property {("ASC"|"DESC")} sort The sort directioon of the key. Default is ASC
 */

/**
 * EventstorePlaybackListField
 * @typedef {Object} EventstorePlaybackListField
 * @property {String} type The type of the field
 * @property {String} name The field name
 */

/**
 * EventstorePlaybackListOptions
 * @typedef {Object} EventstorePlaybackListOptions
 * @property {Object} mysql the mysql library
 * @property {String} host the mysql host
 * @property {Number} port the mysql port
 * @property {String} user the mysql user
 * @property {String} password the mysql password
 * @property {String} database the mysql database name
 * @property {Number} connectionLimit the max coonnections in the pool. default is 1
 */

/**
 * @param {EventstorePlaybackListOptions} options additional options for the Eventstore playback list
 * @constructor
 */
function EventstorePlaybackListMySqlStore(options) {
    options = options || {};
    var defaults = {
        host: '127.0.0.1',
        port: 3306,
        user: 'root',
        password: 'root',
        database: 'eventstore',
        connectionLimit: 1
    };

    this.options = _.defaults(options, defaults);
}

/**
 * @type {EventstorePlaybackListOptions}
 */
EventstorePlaybackListMySqlStore.prototype.options;

EventstorePlaybackListMySqlStore.prototype._pool;

/**
 * @param {EventstorePlaybackListQueryDoneCallback} cb callback to be called when the query is done retrieving data
 * @returns {void} - returns void. use the callback for the result (cb)
 */
EventstorePlaybackListMySqlStore.prototype.init = async function(options) {
    // undefined callbacks are ok for then() and catch() because it checks for undefined functions
    try {
        debug('init called');

        if (!this.options.host) {
            throw new Error('host is required to be passed as part of the options');
        }

        if (!this.options.port) {
            throw new Error('port is required to be passed as part of the options');
        }

        if (!this.options.user) {
            throw new Error('user is required to be passed as part of the options');
        }

        if (!this.options.password) {
            throw new Error('password is required to be passed as part of the options');
        }

        if (!this.options.database) {
            throw new Error('database is required to be passed as part of the options');
        }

        const mysql = require('mysql');

        this._pool = mysql.createPool({
            host: this.options.host,
            port: this.options.port,
            user: this.options.user,
            password: this.options.password,
            database: this.options.database,
            connectionLimit: this.options.connectionLimit
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

        await new Promise((resolve, reject) => {
            this._pool.end(function(err) {
                if (err) {
                    reject(err);
                } else {
                    resolve();
                }
            });
        })
        
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

        await this._executeSqlQuery(query);
    } catch (error) {
        console.error('error in _init with error:', error);
        throw error;
    }
};

/**
 * @param {String} queryText sql query string to be executed
 * @returns {Promise<Object>} - returns a Promise of type Object where object is the Query result
 */
EventstorePlaybackListMySqlStore.prototype._executeSqlQuery = async function(queryText, queryParams) {
    let connection;
    try {
        connection = await this._getConnection();
        const results = await this._doQueryWithConnection(connection, queryText, queryParams);
        return results;
    } catch (error) {
        console.error('error in _doMySqlQuery with params and error:', queryText, queryParams || 'undefined', error);
        throw error;
    } finally {
        try {
            if (connection) {
                await this._releaseConnection(connection);
            }
        } catch (error) {
            console.error('MySQL_EventStore: Error in attempting to release connection. Nothing to do. Please review');
            console.error(error);
        }
    }
};

/**
 * @param {String} queryText sql query string to be executed
 * @returns {Promise<Object>} - returns a Promise of type Object where object is the Query result
 */
EventstorePlaybackListMySqlStore.prototype._doQueryWithConnection = async function(connection, queryText, queryParams) {
    return new Promise((resolve, reject) => {
        try {
            queries(`Full query ${queryText}`);
            connection.query(queryText, queryParams, function(err, results) {
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
 * @param {String} queryText sql query string to be executed
 * @returns {Promise<import('mysql').PoolConnection>} - returns a Promise of type Object where object is the Query result
 */
EventstorePlaybackListMySqlStore.prototype._getConnection = async function() {
    return new Promise((resolve, reject) => {
        try {
            this._pool.getConnection(function(err, conn) {
                if (err) {
                    console.error(`MySQLStore: _getConnection: callback error:`);
                    console.error(err);
                    reject(err);
                } else {
                    resolve(conn);
                }
            });
        } catch (error) {
            console.error(`MySQLStore: _getConnection: try-catch error:`);
            console.error(error);
            reject(error);
        }
    });
};

/**
 * @param {Promise<import('mysql').PoolConnection>} conn the connection to release
 * @returns {Promise<void>} - returns a Promise of type void
 */
EventstorePlaybackListMySqlStore.prototype._releaseConnection = async function(conn) {
    return new Promise((resolve, reject) => {
        try {
            conn.release();
            resolve();
        } catch (error) {
            console.error(`MySQLStore: _releaseConnection: error:`);
            console.error(error);
            reject(error);
        }
    });
};



/**
 * @param {EventstorePlaybackListFilter[]} filters filter parameters for the query
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
 * @param {EventstorePlaybackListSort[]} sorting sort parameters for the query
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
 * @param {EventstorePlaybackListFilter[]} filters filter parameters for the query
 * @param {String} groupBooleanOperator group operator "or" or "and"
 * @returns {String} - returns the filters in string
 */
EventstorePlaybackListMySqlStore.prototype._appendFiltersToFilterString = function(filterString, filters, groupBooleanOperator) {
  if (filters && filters.length > 0) {
    filters.forEach(function(filter, index) {
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
                    filterString += ` ${prefix} ${filter.field} = '${filter.value}' ${suffix} `;
                }
                break;

            case 'any':
                {
                    if (filter.value && Array.isArray(filter.value) && filter.value.length > 0) {
                        let inFilter = '';
                        filter.value.forEach((v, index) => {
                            inFilter += `'${v}'`;

                            if (index < filter.value.length - 1) {
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
        
            case 'contains':
            {
                if (filter.value) {
                    filterString += ` ${prefix} ${filter.field} LIKE '%${filter.value}%' ${suffix} `;
                }
              break;
            }
            case 'arrayContains':
            {
                if (filter.value) {
                  filterString += ` ${prefix} `;
                  if (Array.isArray(filter.value)) {
                    filter.value.forEach((value, index) => {
                      filterString += ` JSON_CONTAINS(${filter.field}, '"${value}"')`;
                      if (index !== filter.value.length - 1) {
                        filterString += ' OR ';
                      }
                    });
                  } else {
                    filterString += ` JSON_CONTAINS(${filter.field}, '"${filter.value}"')`;
                  }
                  filterString += ` ${suffix} `;
                }
                break;
            }
            case 'startsWith':
            {
              if (filter.value) {
                filterString += ` ${prefix} ${filter.field} LIKE '${filter.value}%' ${suffix} `;
              }
              break;
            }
            case 'endsWith':
            {
              if (filter.value) {
                filterString += ` ${prefix} ${filter.field} LIKE '%${filter.value}' ${suffix} `;
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
 * @param {EventstorePlaybackListFilter[]} filters filter parameters for the query
 * @param {EventstorePlaybackListSort[]} sort sort parameters for the query
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

        const resultCount = await this._executeSqlQuery(countQuery);
        const resultRows = await this._executeSqlQuery(getQuery, whereParams);

        const data = {
            count: (resultCount.length > 0 ? resultCount[0].total_count : 0),
            rows: resultRows.map(function(x) {
                return {
                    rowId: x.row_id,
                    revision: x.row_revision,
                    data: (x.row_json ? JSON.parse(x.row_json) : undefined),
                    meta: (x.meta_json ? JSON.parse(x.meta_json) : undefined)
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
        var rowData = {
            row_id: rowId,
            row_revision: revision,
            row_json: (data ? JSON.stringify(data) : null),
            meta_json: (meta ? JSON.stringify(meta) : null)
        };
        await this._executeSqlQuery(`INSERT IGNORE INTO \`${listName}\` SET ?`, rowData);
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

        await this._executeSqlQuery(`UPDATE ${listName} SET row_revision = ?, row_json = ?, meta_json = ?  WHERE row_id = ?`, updateParams);

    } catch (error) {
        console.error('error in update with params and error:', rowId, revision, data, meta, error);
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
        await this._executeSqlQuery(`DELETE FROM ${listName} WHERE row_id = ?`, whereParams);
    } catch (error) {
        console.error('error in delete with params and error:', rowId, error);
        throw error;
    }
};

/**
 * @param {String} listName the name of the list
 * @param {String} rowId the unique id for this row
 * @param {EventstorePlaybackListDoneCallback} cb callback to call when operation is done
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
        const results = await this._executeSqlQuery(getQuery, whereParams);

        if (results.length > 0) {
            const rowData = results[0];
            return {
                data: rowData.row_json ? JSON.parse(rowData.row_json) : null,
                meta: rowData.meta_json ? JSON.parse(rowData.meta_json) : null,
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
