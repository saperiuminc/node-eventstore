const _ = require('lodash');
const debug = require('debug')('eventstore:playback-list-view');
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
 * @callback EventstorePlaybackListViewQueryDoneCallback
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
 * @property {("is"|"any"|"range"|"dateRange"|"contains"|"arrayContains"|"startsWith"|"endsWith"|"exists"|"notExists")} operator The operator to use. 
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
 * @typedef {Object} EventstorePlaybackListViewOptions
 * @property {Object} mysql the mysql library
 * @property {String} host the mysql host
 * @property {Number} port the mysql port
 * @property {String} user the mysql user
 * @property {String} password the mysql password
 * @property {String} database the mysql database name
 * @property {String} listName the name of this list
 * @property {String} query the query text to execute for the view
 */

/**
 * @param {EventstorePlaybackListViewOptions} options additional options for the Eventstore playback list view
 * @constructor
 */
function EventstorePlaybackListView(options) {
  options = options || {};
  var defaults = {
    host: '127.0.0.1',
    port: 3306,
    user: 'root',
    password: 'root',
    database: 'eventstore',
    listName: 'list_name',
    query: '',
    alias: undefined
  };

  this.options = _.defaults(options, defaults);
}

/**
 * @type {EventstorePlaybackListViewOptions}
 */
EventstorePlaybackListView.prototype.options;

EventstorePlaybackListView.prototype._pool;

/**
 * @param {EventstorePlaybackListViewQueryDoneCallback} cb callback to be called when the query is done retrieving data
 * @returns {void} - returns void. use the callback for the result (cb)
 */
EventstorePlaybackListView.prototype.init = async function () {
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

    if (!this.options.query) {
      throw new Error('query is required to be passed as part of the options');
    }

    const mysql = require('mysql');

    this._pool = mysql.createPool({
      host: this.options.host,
      port: this.options.port,
      user: this.options.user,
      password: this.options.password,
      database: this.options.database,
      connectionLimit: this.options.connectionLimit,
      multipleStatements: true
    });

    if (this.options.alias == undefined) {
      // NOTE: create only if there are no alias, for backward compatibility
      const query = `CREATE OR REPLACE VIEW ${this.options.listName} AS ${this.options.query}`;
      await this._executeSqlQuery(query);
    } else {
      // DO NOTHINGS
    }
  } catch (error) {
    console.error('error in _init with error:', error);
    throw error;
  }
};

/**
 * @param {String} queryText sql query string to be executed
 * @returns {Promise<Object>} - returns a Promise of type Object where object is the Query result
 */
EventstorePlaybackListView.prototype._executeSqlQuery = async function (queryText, queryParams) {
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
EventstorePlaybackListView.prototype._executeSqlQueryStream = async function(queryText, queryParams) {
  let connection;
  try {
      connection = await this._getConnection();
      const sqlStream = await this._doQueryStreamWithConnection(connection, queryText, queryParams);
      return Promise.resolve(sqlStream);
  } catch (error) {
      console.error('error in _doQueryStreamWithConnection with params and error:', queryText, queryParams || 'undefined', error);
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
EventstorePlaybackListView.prototype._doQueryWithConnection = async function (connection, queryText, queryParams) {
  return new Promise((resolve, reject) => {
    try {
      queries(`Full query ${queryText}`);
      connection.query(queryText, queryParams, function (err, results) {
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
 * @returns {Promise<Object>} - returns a Promise of type Object where object is the Query stream
 */
EventstorePlaybackListView.prototype._doQueryStreamWithConnection = async function(connection, queryText, queryParams) {
  return new Promise((resolve, reject) => {
      try {
          queries(`Full query ${queryText}`);
          return resolve(connection.query(queryText, queryParams).stream());
      } catch (error) {
          console.error('error in _doQueryStreamWithConnection with params and error:', queryText, error);
          reject(error);
      }
  })
};

/**
 * @param {String} queryText sql query string to be executed
 * @returns {Promise<import('mysql').PoolConnection>} - returns a Promise of type Object where object is the Query result
 */
EventstorePlaybackListView.prototype._getConnection = async function () {
  return new Promise((resolve, reject) => {
    try {
      this._pool.getConnection(function (err, conn) {
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
EventstorePlaybackListView.prototype._releaseConnection = async function (conn) {
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
 * @param {Number} start zero-index start position of the rows to get
 * @param {Number} limit how many rows we want to get
 * @param {EventstorePlaybackListFilter[]} filters filter parameters for the query
 * @param {EventstorePlaybackListSort[]} sort sort parameters for the query
 * @param {EventstorePlaybackListQueryDoneCallback} cb callback to be called when the query is done retrieving data
 * @returns {void} - returns void. use the callback for the result (cb)
 */
EventstorePlaybackListView.prototype.query = function (start, limit, filters, sort, cb) {
  this._query(start, limit, filters, sort).then((results) => cb(null, results)).catch(cb);
};


/**
 * @param {Number} start zero-index start position of the rows to get
 * @param {Number} limit how many rows we want to get
 * @param {EventstorePlaybackListFilter[]} filters filter parameters for the query
 * @param {EventstorePlaybackListSort[]} sort sort parameters for the query
 * @param {EventstorePlaybackListQueryDoneCallback} cb callback to be called when the query is done retrieving data
 * @returns {void} - returns void. use the callback for the result (cb)
 */
EventstorePlaybackListView.prototype.queryStream = function (start, limit, filters, sort, cb) {
  this._queryStream(start, limit, filters, sort).then((results) => cb(null, results)).catch(cb);
};


/**
 * @param {Number} start zero-index start position of the rows to get
 * @param {Number} limit how many rows we want to get
 * @param {EventstorePlaybackListFilter[]} filters filter parameters for the query
 * @param {EventstorePlaybackListSort[]} sort sort parameters for the query
 * @returns {Promise<EventstorePlaybackListQueryResult} - returns void. use the callback for the result (cb)
 */
EventstorePlaybackListView.prototype._query = async function (start, limit, filters, sort) {
  try {
    debug('query called with params:', start, limit, filters, sort);
    if (this.options.alias == undefined) {
      // NOTE: query using view only if there are no alias, for backward compatibility
      let filterString = this._listFiltersToFilterString(filters);
      let orderByString = this._listSortingToSortingString(sort);

      const countQuery = `SELECT COUNT(1) as total_count FROM ${this.options.listName} WHERE 1 = 1 ${filterString}`;
      const getQuery = `SELECT * FROM ${this.options.listName} WHERE 1 = 1 ${filterString} ${orderByString} LIMIT ?,?`;
      const whereParams = [
        start,
        limit
      ];

      const resultCount = await this._executeSqlQuery(countQuery);
      const resultRows = await this._executeSqlQuery(getQuery, whereParams);

      const data = {
        count: (resultCount.length > 0 ? resultCount[0].total_count : 0),
        rows: resultRows.map(function (x) {
          return {
            rowId: x.row_id,
            revision: x.row_revision,
            data: (x.row_json ? JSON.parse(x.row_json) : undefined),
            meta: (x.meta_json ? JSON.parse(x.meta_json) : undefined)
          };
        })
      }

      return data;
    } else {
      let whereString = this._listFiltersToFilterString(filters, true);
      let orderString = this._listSortingToSortingString(sort);
      let limitString = `LIMIT ${start},${limit}`;
      let unionLimitString = `LIMIT ${start+limit}`;

      let getQuery = `${this.options.query}`
      if (whereString != undefined && whereString.length > 0) {
        getQuery = getQuery.replace(/@@where/g, ` WHERE ${whereString} `)
      } else {
        getQuery = getQuery.replace(/@@where/g, ` WHERE 1 = 1 `)
      }
      if (orderString != undefined && orderString.length > 0) {
        getQuery = getQuery.replace(/@@order/g, ` ${orderString} `)
      } else {
        getQuery = getQuery.replace(/@@order/g, ``)
      }
      if (limitString != undefined && limitString.length > 0) {
        getQuery = getQuery.replace(/@@limit/g, ` ${limitString} `)
      } else {
        getQuery = getQuery.replace(/@@limit/g, ``)
      }
      if (unionLimitString != undefined && unionLimitString.length > 0) {
        getQuery = getQuery.replace(/@@unionlimit/g, ` ${unionLimitString} `)
      } else {
        getQuery = getQuery.replace(/@@unionlimit/g, ``)
      }

      let resultCount;
      let resultRows;
      const resultCompound = await this._executeSqlQuery(getQuery);
      if(resultCompound && resultCompound.length >= 2 && resultCompound[1].length > 0) {
        resultCount = resultCompound[1];
        resultRows = resultCompound[0];
      } else {
        resultRows = resultCompound;
      }
      const data = {
        count: (resultCount && resultCount.length > 0 ? resultCount[0].total_count : 0),
        rows: resultRows.map(function (x) {
          return {
            rowId: x.row_id,
            revision: x.row_revision,
            data: (x.row_json ? JSON.parse(x.row_json) : undefined),
            meta: (x.meta_json ? JSON.parse(x.meta_json) : undefined)
          };
        })
      }
      return data;
    }
  } catch (error) {
    console.error('error in query with params and error:', start, limit, filters, sort, error);
    throw error;
  }
};


/**
 * @param {Number} start zero-index start position of the rows to get
 * @param {Number} limit how many rows we want to get
 * @param {EventstorePlaybackListFilter[]} filters filter parameters for the query
 * @param {EventstorePlaybackListSort[]} sort sort parameters for the query
 * @returns {Promise<Object} - returns void. use the callback for the result (cb)
 */
EventstorePlaybackListView.prototype._queryStream = async function (start, limit, filters, sort) {
  try {
    debug('query called with params:', start, limit, filters, sort);
    if (this.options.alias == undefined) {
      // NOTE: query using view only if there are no alias, for backward compatibility
      let filterString = this._listFiltersToFilterString(filters);
      let orderByString = this._listSortingToSortingString(sort);

      const countQuery = `SELECT COUNT(1) as total_count FROM ${this.options.listName} WHERE 1 = 1 ${filterString}`;
      const getQuery = `SELECT * FROM ${this.options.listName} WHERE 1 = 1 ${filterString} ${orderByString} LIMIT ?,?`;
      const whereParams = [
        start,
        limit
      ];

      const resultCountStream = await this._executeSqlQueryStream(countQuery);
      const resultRowsStream = await this._executeSqlQueryStream(getQuery, whereParams);

      const data = {
        count: resultCountStream,
        rows: resultRowsStream
      };

      return data;
    } else {
      let whereString = this._listFiltersToFilterString(filters, true);
      let orderString = this._listSortingToSortingString(sort);
      let limitString = `LIMIT ${start},${limit}`;
      let unionLimitString = `LIMIT ${start+limit}`;

      let getQuery = `${this.options.query}`
      if (whereString != undefined && whereString.length > 0) {
        getQuery = getQuery.replace(/@@where/g, ` WHERE ${whereString} `)
      } else {
        getQuery = getQuery.replace(/@@where/g, ` WHERE 1 = 1 `)
      }
      if (orderString != undefined && orderString.length > 0) {
        getQuery = getQuery.replace(/@@order/g, ` ${orderString} `)
      } else {
        getQuery = getQuery.replace(/@@order/g, ``)
      }
      if (limitString != undefined && limitString.length > 0) {
        getQuery = getQuery.replace(/@@limit/g, ` ${limitString} `)
      } else {
        getQuery = getQuery.replace(/@@limit/g, ``)
      }
      if (unionLimitString != undefined && unionLimitString.length > 0) {
        getQuery = getQuery.replace(/@@unionlimit/g, ` ${unionLimitString} `)
      } else {
        getQuery = getQuery.replace(/@@unionlimit/g, ``)
      }
      
      const resultRowsStream = await this._executeSqlQueryStream(getQuery);

      const data = {
        rows: resultRowsStream
      };

      return data;
    }
  } catch (error) {
    console.error('error in queryStream with params and error:', start, limit, filters, sort, error);
    throw error;
  }
};


/**
 * @param {EventstorePlaybackListFilter[]} filters filter parameters for the query
 * @returns {String} - returns the filters in string
 */
EventstorePlaybackListView.prototype._listFiltersToFilterString = function (filters, removeLeadingAnd) {
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
          if(removeLeadingAnd && filterString.length == 0) {
            // DO NOTHING
          } else {
            filterString += ' AND ';
          }
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
EventstorePlaybackListView.prototype._listSortingToSortingString = function (sorting) {
  let sortingString = '';
  const self = this;

  if (sorting && sorting.length > 0) {
    sortingString = ' ORDER BY ';
    sorting.forEach(function (sort, index) {
      const field = self._replaceFieldWithAlias(sort.field);
      sortingString += `${field} ${sort.sortDirection}`;
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
EventstorePlaybackListView.prototype._appendFiltersToFilterString = function (filterString, filters, groupBooleanOperator) {
  const self = this;
  if (filters && filters.length > 0) {
    filters.forEach(function (filter, index) {
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

      const field = self._replaceFieldWithAlias(filter.field);
      let prefix = groupBooleanOperator === 'or' ? 'OR' : 'AND';
      let suffix = '';
      if (index === 0) {
        prefix = '(';
      }

      if (index === filters.length - 1) {
        suffix = ')';
      }

      switch (filter.operator) {
        case 'is': {
          const inFilter = !isNaN(filterValue) ? `${filterValue}` : `'${filterValue}'`;
          filterString += ` ${prefix} ${field} = ${inFilter} ${suffix} `;
        }
        break;

      case 'any': {
        if (filterValue && Array.isArray(filterValue) && filterValue.length > 0) {
          let inFilter = '';
          filterValue.forEach((v, index) => {
            inFilter += `'${v}'`;

            if (index < filterValue.length - 1) {
              inFilter += ',';
            }
          })
          filterString += ` ${prefix} ${field} IN (${inFilter}) ${suffix}`;
        }
      }
      break;

      case 'range': {
        if (filter.from || filter.to) {
          filterString += ` ${prefix}`;
        }

        if (filter.from) {
          filterString += ` ${field} >= ${filter.from} `;
        }

        if (filter.from && filter.to) {
          filterString += ' AND';
        }

        if (filter.to) {
          filterString += ` ${field} <= ${filter.to} `;
        }

        filterString += `${suffix} `;
      }
      break;

      case 'dateRange': {
        if (filter.from || filter.to) {
          filterString += ` ${prefix}`;
        }

        if (filter.from) {
          filterString += ` ${field} >= "${filter.from}" `;
        }

        if (filter.from && filter.to) {
          filterString += ' AND';
        }

        if (filter.to) {
          filterString += ` ${field} <= "${filter.to}" `;
        }

        filterString += `${suffix} `;
      }
      break;

      case 'contains': {
        if (filterValue) {
          const caseSensitiveFiller = filter.caseInsensitive ? '' : 'BINARY';
          filterString += ` ${prefix} ${field} LIKE ${caseSensitiveFiller} '%${filterValue}%' ${suffix} `;
        }
        break;
      }
      case 'arrayContains': {
        if (filterValue) {
          filterString += ` ${prefix} `;
          if (Array.isArray(filterValue)) {
            filterValue.forEach((value, index) => {
              filterString += ` JSON_CONTAINS(${field}, '"${value}"')`;
              if (index !== filterValue.length - 1) {
                filterString += ' OR ';
              }
            });
          } else {
            filterString += ` JSON_CONTAINS(${field}, '"${filterValue}"')`;
          }
          filterString += ` ${suffix} `;
        }
        break;
      }
      case 'startsWith': {
        if (filterValue) {
          const caseSensitiveFiller = filter.caseInsensitive ? '' : 'BINARY';
          filterString += ` ${prefix} ${field} LIKE ${caseSensitiveFiller} '${filterValue}%' ${suffix} `;
        }
        break;
      }
      case 'endsWith': {
        if (filterValue) {
          const caseSensitiveFiller = filter.caseInsensitive ? '' : 'BINARY';
          filterString += ` ${prefix} ${field} LIKE ${caseSensitiveFiller} '%${filterValue}' ${suffix} `;
        }
        break;
      }
      case 'exists': {
        filterString += ` ${prefix} ${field} IS NOT NULL ${suffix} `;
        break;
      }
      case 'notExists': {
        filterString += ` ${prefix} ${field} IS NULL ${suffix} `;
        break;
      }
      }
    });
  }
  return filterString;
}

/**
 * @param {String} field - field to replace alias with
 * @returns {String} - actual filed alias value, if no alias, return the field
 */
EventstorePlaybackListView.prototype._replaceFieldWithAlias = function (field) {
  if (this.options.alias != undefined) {
    var valueFromAlias = this.options.alias[field];
    if (valueFromAlias) {
      return valueFromAlias;
    }
  }
  return field;
};

module.exports = EventstorePlaybackListView;
