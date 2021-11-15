const _ = require('lodash');
const StreamTransform = require('stream').Transform;
const debug = require('debug')('eventstore:playback-list-view');
const queries = require('debug')('eventstore:show-queries');
const mysqlsharedpool = require('@saperiuminc/mysql2-shared-pool');
const EventStorePlaybackListViewPaginationError = require('./errors/EventStorePlaybackListViewPaginationError');

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
 * @property {("ASC"|"DESC")} sortDirection the direction to sort
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
 * @property {String} listQuery the list query text to execute for the view
 * @property {String} totalCountQuery the total count query text to execute for the view
 * @property {String[]} paginationPrimaryKeys the field names of the pagination primary fields
 * @property {("ASC"|"DESC")} defaultPaginationPrimaryKeySortDirection the default sort direction of the paginationPrimaryFields
 */

/**
 * @param {EventstorePlaybackListViewOptions} options additional options for the Eventstore playback list view
 * @constructor
 */
function EventstorePlaybackListView(options) {
  options = options || {
    connection: {},
    pool: {},
    listName: 'list_name',
    listQuery: '',
    totalCountQuery: '',
    alias: undefined,
    paginationPrimaryKeys: undefined,
    defaultPaginationPrimaryKeySortDirection: undefined
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

  // if (this.options.paginationPrimaryKeys) {
  //   // console.log('HAS paginationPrimaryKeys:', this.options.paginationPrimaryKeys);
  // }
  // if (this.options.defaultPaginationPrimaryKeySortDirection) {
  //   // console.log('HAS defaultPaginationPrimaryKeySortDirection:', this.options.defaultPaginationPrimaryKeySortDirection);
  // }
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

    if (!this.options.listQuery) {
      throw new Error('listQuery is required to be passed as part of the options');
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

    if (this.options.alias == undefined) {
      // NOTE: create only if there are no alias, for backward compatibility
      const listQuery = `CREATE OR REPLACE VIEW ${this.options.listName} AS ${this.options.listQuery}`;
      await this._executeSqlQuery(listQuery);
    } else {
      // DO NOTHING
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
  const resultRaw = await this._pool.execute(queryText, queryParams);
  return resultRaw[0];
};

/**
 * @param {String} queryText sql query string to be executed
 * @returns {Promise<Object>} - returns a Promise of type Object where object is the Query result
 */
EventstorePlaybackListView.prototype._executeSqlQueryStream = function (queryText, queryParams) {
  return this._pool.pool.query(queryText, queryParams).stream();
};


/**
 * @param {Number} start zero-index start position of the rows to get
 * @param {Number} limit how many rows we want to get
 * @param {EventstorePlaybackListFilter[]} filters filter parameters for the query
 * @param {EventstorePlaybackListSort[]} sort sort parameters for the query
 * @param {String} previousKey previousKey parameters for the query
 * @param {String} nextKey nextKey parameters for the query
 * @param {EventstorePlaybackListQueryDoneCallback} cb callback to be called when the query is done retrieving data
 * @returns {void} - returns void. use the callback for the result (cb)
 */
// EventstorePlaybackListView.prototype.query = function (start, limit, filters, sort, cb) {
//   this._query(start, limit, filters, sort).then((results) => cb(null, results)).catch(cb);
// };
EventstorePlaybackListView.prototype.query = function (start, limit, filters, sort, previousKey, nextKey, cb) {
  if (typeof previousKey === 'function') {
    const callback = previousKey;
    this._query(start, limit, filters, sort).then((results) => callback(null, results)).catch(callback);
  } else {
    this._query(start, limit, filters, sort, previousKey, nextKey).then((results) => cb(null, results)).catch(cb);
  }
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
 * @param {String} previousKey previousKey parameters for the query
 * @param {String} nextKey nextKey parameters for the query
 * @returns {Promise<EventstorePlaybackListQueryResult} - returns void. use the callback for the result (cb)
 */
EventstorePlaybackListView.prototype._query = async function (start, limit, filters, sort, previousKey, nextKey) {
  try {
    debug('query called with params:', start, limit, filters, sort);
    if (this.options.alias == undefined) {
      // NOTE: query using view only if there are no alias, for backward compatibility
      let filterString = this._buildFilterString(filters);
      let orderByString = this._buildOrderString(sort);

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
            data: (x.row_json ? x.row_json : undefined),
            meta: (x.meta_json ? x.meta_json : undefined)
          };
        })
      }

      return data;
    } else {
      const pageType = this._getPageType(previousKey, nextKey);
      // console.log('QUERY PARAMS:', this.options.listName, start, limit, filters, sort, pageType, previousKey, nextKey);

      let paginationFields;
      if (sort) {
        paginationFields = this._getPaginationFields(sort, pageType);
      }
      // console.log('PAGINATION FIELDS:', paginationFields);

      let whereString = this._buildFilterString(filters, true);

      let keysetPaginationString;
      if (!previousKey && !nextKey) {
        keysetPaginationString = '';
      } else {
        if (previousKey && nextKey) {
          throw new EventStorePlaybackListViewPaginationError('Both previousKey and nextKey are provided');
        }
      
        if (!this.options.paginationPrimaryKeys || this.options.paginationPrimaryKeys.length <= 0) {
          throw new EventStorePlaybackListViewPaginationError(`Missing PaginationPrimaryKeys!`);
        }

        if (pageType !== 'last') {
          keysetPaginationString = this._buildKeySetPaginationFilterString(sort, pageType, paginationFields, previousKey, nextKey);
        } else {
          keysetPaginationString = '';
        }
      }

      let paginationOrderString = this._buildPaginationOrderString(sort, pageType, paginationFields);
      let orderString = this._buildOrderString(sort, pageType, paginationFields);
      let limitString = `LIMIT ${start},${limit}`;
      let unionLimitString = `LIMIT ${start+limit}`;

      const promises = [];

      let getListQuery = `${this.options.listQuery}`;
      if (whereString != undefined && whereString.length > 0) {
        getListQuery = getListQuery.replace(/@@where/g, ` WHERE ${whereString} ${keysetPaginationString}`);
      } else {
        getListQuery = getListQuery.replace(/@@where/g, ` WHERE 1 = 1 ${keysetPaginationString}`);
      }
      if (paginationOrderString != undefined && paginationOrderString.length > 0) {
        getListQuery = getListQuery.replace(/@@paginationOrder/g, ` ${paginationOrderString} `);
      } else {
        getListQuery = getListQuery.replace(/@@paginationOrder/g, ``);
      }
      if (orderString != undefined && orderString.length > 0) {
        getListQuery = getListQuery.replace(/@@order/g, ` ${orderString} `);
      } else {
        getListQuery = getListQuery.replace(/@@order/g, ``);
      }
      if (limitString != undefined && limitString.length > 0) {
        getListQuery = getListQuery.replace(/@@limit/g, ` ${limitString} `);
      } else {
        getListQuery = getListQuery.replace(/@@limit/g, ``);
      }
      if (unionLimitString != undefined && unionLimitString.length > 0) {
        getListQuery = getListQuery.replace(/@@unionlimit/g, ` ${unionLimitString} `);
      } else {
        getListQuery = getListQuery.replace(/@@unionlimit/g, ``);
      }
      promises.push(this._executeSqlQuery(getListQuery));

      // console.log(getListQuery);

      if (this.options.totalCountQuery) {
        let getTotalCountQuery = `${this.options.totalCountQuery}`;
        if (whereString != undefined && whereString.length > 0) {
          getTotalCountQuery = getTotalCountQuery.replace(/@@where/g, ` WHERE ${whereString} `);
        } else {
          getTotalCountQuery = getTotalCountQuery.replace(/@@where/g, ` WHERE 1 = 1 `);
        }
        if (orderString != undefined && orderString.length > 0) {
          getTotalCountQuery = getTotalCountQuery.replace(/@@order/g, ` ${orderString} `);
        } else {
          getTotalCountQuery = getTotalCountQuery.replace(/@@order/g, ``);
        }
        if (limitString != undefined && limitString.length > 0) {
          getTotalCountQuery = getTotalCountQuery.replace(/@@limit/g, ` ${limitString} `);
        } else {
          getTotalCountQuery = getTotalCountQuery.replace(/@@limit/g, ``);
        }
        if (unionLimitString != undefined && unionLimitString.length > 0) {
          getTotalCountQuery = getTotalCountQuery.replace(/@@unionlimit/g, ` ${unionLimitString} `);
        } else {
          getTotalCountQuery = getTotalCountQuery.replace(/@@unionlimit/g, ``);
        }

        promises.push(this._executeSqlQuery(getTotalCountQuery));
      }

      let resultCount;
      let resultRows;
      const resultCompound = await Promise.all(promises);

      if (resultCompound && resultCompound.length >= 2) {
        resultRows = resultCompound[0];
        resultCount = resultCompound[1];
      } else {
        resultRows = resultCompound[0];
      }

      // If keyset last key is specified, trim the return based on total count
      if (pageType === 'last' && resultCount && resultCount[0].total_count) {
        const lastPageItemCount = resultCount[0].total_count % limit;
        resultRows = resultRows.splice(resultRows.length - lastPageItemCount);
      }

      const data = {
        count: this.options.totalCountQuery ? (resultCount && resultCount.length > 0 ? resultCount[0].total_count : 0) : undefined,
        rows: resultRows.map(function (x) {
          return {
            rowId: x.row_id,
            revision: x.row_revision,
            data: (x.row_json ? x.row_json : undefined),
            meta: (x.meta_json ? x.meta_json : undefined)
          };
        }),
        previousKey: undefined,
        nextKey: undefined
      }

      // Compute keyset pagination previous and next keys
      if (this.options.paginationPrimaryKeys && this.options.paginationPrimaryKeys.length > 0 && 
        paginationFields && paginationFields.length > 0 && resultRows.length > 0) {
        const firstItem = resultRows[0];
        const lastItem = resultRows[resultRows.length - 1];

        let newPreviousKey = [];
        let newNextKey = [];

        for(let i = 0; i < paginationFields.length; i++) {
          if (paginationFields[i].field) {
            const paginationField = paginationFields[i].field;
            if (firstItem[paginationField] !== undefined) {
              newPreviousKey.push(firstItem[paginationField]);
            } else {
              throw new EventStorePlaybackListViewPaginationError('Missing required pagination field in first item of playback list view', this.options.listName, paginationField);
            }
            if (lastItem[paginationField] !== undefined) {
              newNextKey.push(lastItem[paginationField]);
            } else {
              throw new EventStorePlaybackListViewPaginationError('Missing required pagination field in last item of playback list view', this.options.listName, paginationField);
            }
          } else {
            throw new EventStorePlaybackListViewPaginationError('Missing pagination field', this.options.listName);
          }
        }

        if (newPreviousKey && newPreviousKey.length > 0) {
          data.previousKey = Buffer.from(JSON.stringify(newPreviousKey)).toString('base64');
        }
        if (newNextKey && newNextKey.length > 0) {
          data.nextKey = Buffer.from(JSON.stringify(newNextKey)).toString('base64');
        }

        // console.log('NEW PREVIOUS KEY:', JSON.stringify(newPreviousKey), data.previousKey);
        // console.log('NEW NEXT KEY:', JSON.stringify(newNextKey), data.nextKey);
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
      let filterString = this._buildFilterString(filters);
      let orderByString = this._buildOrderString(sort);

      const countQuery = `SELECT COUNT(1) as total_count FROM ${this.options.listName} WHERE 1 = 1 ${filterString}`;
      const getQuery = `SELECT * FROM ${this.options.listName} WHERE 1 = 1 ${filterString} ${orderByString} LIMIT ?,?`;
      const whereParams = [
        start,
        limit
      ];

      const resultCount = this._executeSqlQuery(countQuery);
      const resultRowsStream = this._executeSqlQueryStream(getQuery, whereParams);

      const queryTransform = new StreamTransform({
        objectMode: true
      });

      queryTransform._transform = function (rowDataPacket, encoding, done) {
        done(null, {
          rowId: rowDataPacket.row_id,
          revision: rowDataPacket.row_revision,
          data: (rowDataPacket.row_json ? rowDataPacket.row_json : undefined),
          meta: (rowDataPacket.meta_json ? rowDataPacket.meta_json : undefined)
        });
      }

      const transformedRowsStream = resultRowsStream.pipe(queryTransform);

      const data = {
        count: resultCount,
        rows: transformedRowsStream
      };

      return data;
    } else {
      let whereString = this._buildFilterString(filters, true);
      let orderString = this._buildOrderString(sort);
      let limitString = `LIMIT ${start},${limit}`;
      let unionLimitString = `LIMIT ${start+limit}`;

      let getQuery = `${this.options.listQuery}`
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

      const resultRowsStream = this._executeSqlQueryStream(getQuery);

      const queryTransform = new StreamTransform({
        objectMode: true
      });

      queryTransform._transform = function (rowDataPacket, encoding, done) {
        done(null, {
          rowId: rowDataPacket.row_id,
          revision: rowDataPacket.row_revision,
          data: (rowDataPacket.row_json ? rowDataPacket.row_json : undefined),
          meta: (rowDataPacket.meta_json ? rowDataPacket.meta_json : undefined)
        });
      }

      const transformedRowsStream = resultRowsStream.pipe(queryTransform);

      const data = {
        rows: transformedRowsStream
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
EventstorePlaybackListView.prototype._buildFilterString = function (filters, removeLeadingAnd) {
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
        if (removeLeadingAnd && filterString.length == 0) {
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
 * @param {String} pageType pageType parameters for the query
 * @returns {String} - the sorting in string
 */
EventstorePlaybackListView.prototype._buildPaginationOrderString = function (sorting, pageType, paginationFields) {
  let sortingString = '';
  const self = this;

  if (self.options.paginationPrimaryKeys && self.options.paginationPrimaryKeys.length > 0 && paginationFields && paginationFields.length > 0) {
    sortingString = ' ORDER BY ';
    for (let i = 0; i < paginationFields.length; i++) {
      if (i > 0) {
        sortingString += ', ';
      }
      const paginationField = paginationFields[i];
      if (pageType === 'prev' || pageType === 'last') {
        sortingString += `${paginationField.aliasField} ${paginationField.previousSortDirection}`;
      } else {
        sortingString += `${paginationField.aliasField} ${paginationField.nextSortDirection}`;
      }
    }
  } else if (sorting && sorting.length > 0) {
    sortingString = ' ORDER BY ';
    sorting.forEach(function (sort, index) {
      const field = self._replaceFieldWithAlias(sort.field);
      sortingString += `${field} ${sort.sortDirection}`;
      if (index < sorting.length - 1) {
        sortingString += ', ';
      }
    });
  }

  return sortingString;
};

/**
 * @param {EventstorePlaybackListSort[]} sorting sort parameters for the query
 * @param {String} pageType pageType parameters for the query
 * @returns {String} - the sorting in string
 */
EventstorePlaybackListView.prototype._buildOrderString = function (sorting, pageType, paginationFields) {
  let sortingString = '';
  const self = this;

  if (self.options.paginationPrimaryKeys && self.options.paginationPrimaryKeys.length > 0 && paginationFields && paginationFields.length > 0) {
    sortingString = ' ORDER BY ';
    for (let i = 0; i < paginationFields.length; i++) {
      if (i > 0) {
        sortingString += ', ';
      }
      const paginationField = paginationFields[i];
      sortingString += `${paginationField.aliasField} ${paginationField.sortDirection}`;
    }
  } else if (sorting && sorting.length > 0) {
    sortingString = ' ORDER BY ';
    sorting.forEach(function (sort, index) {
      const field = self._replaceFieldWithAlias(sort.field);
      sortingString += `${field} ${sort.sortDirection}`;
      if (index < sorting.length - 1) {
        sortingString += ', ';
      }
    });
  }
  
  return sortingString;
};

/**
 * @param {EventstorePlaybackListSort[]} sorting sort parameters for the query
 * @param {String} paginationKey paginationKey parameters for the query
 * @returns {String} - the key set pagination string
 */
EventstorePlaybackListView.prototype._buildKeySetPaginationFilterString = function (sorting, pageType, paginationFields, previousKey, nextKey) {
  let keysetPaginationString = '';
  let serializedPageKey;
  if (pageType === 'prev') {
    serializedPageKey = previousKey;
  } else if (pageType === 'next') {
    serializedPageKey = nextKey;
  }

  let paginationKeys = [];
  try {
    paginationKeys = JSON.parse(Buffer.from(serializedPageKey, 'base64').toString('utf8'));
  } catch (error) {
    throw new EventStorePlaybackListViewPaginationError(`Invalid paginationKey! ${serializedPageKey}`, error);
  }

  if (paginationFields.length !== paginationKeys.length) {
    throw new EventStorePlaybackListViewPaginationError(`Invalid paginationKey length! ${serializedPageKey}`);
  }

  if (paginationKeys.length > 0) {
    keysetPaginationString += ' AND (';

    for (let i = 0; i < paginationKeys.length; i++) {
      let pageFilter = ' ';
      if (i > 0) {
        pageFilter += 'OR '
      }
      pageFilter += '( ';

      for (let j = 0; j <= i; j++) {
        if (j > 0) {
          pageFilter += 'AND ';
        }
        const paginationField = paginationFields[j];
        const paginationKey = paginationKeys[j];

        pageFilter += paginationField.aliasField + ' ';
        if (j === i) {
          if (pageType === 'prev') {
            pageFilter += paginationField.previousSortDirectionToken;
          } else {
            pageFilter += paginationField.nextSortDirectionToken;
          }
        } else {
          pageFilter += '='
        }
        if (typeof paginationKey === 'string') {
          pageFilter += ` '${paginationKey}' `;
        } else {
          pageFilter += ` ${paginationKey} `;
        }
      }
      pageFilter += ')';
      keysetPaginationString += pageFilter;
    }
    keysetPaginationString += ')';
  }

  return keysetPaginationString;
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
          const inFilter = `'${filterValue}'`;
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

/**
 * @param {String} previousKey - previous key; used in keyset pagination
 * @param {String} nextKey - next key; used in keyset pagination
 * @returns {("prev"|"next"|"last"|null)} page type
 */
EventstorePlaybackListView.prototype._getPageType = function (previousKey, nextKey) {
  let pageType = null;
  if (previousKey && !nextKey) {
    pageType = 'prev';
  } else if (!previousKey && nextKey) {
    if (nextKey === '__LAST') {
      pageType = 'last';
    } else {
      pageType = 'next';
    }
  }
  return pageType;
};

/**
 * @param {EventstorePlaybackListSort[]} sorting sort parameters for the query
 * @returns {String} - actual filed alias value, if no alias, return the field
 */
EventstorePlaybackListView.prototype._getPaginationFields = function (sorting) {
  const self = this;
  const paginationFieldMap = {};
  const paginationFields = [];

  for (let i = 0; i < sorting.length; i++) {
    const sort = sorting[i];
    const aliasField = self._replaceFieldWithAlias(sort.field);

    paginationFields.push({
      aliasField: aliasField,
      field: sort.field,
      sortDirection: sort.sortDirection,
      sortDirectionToken: sort.sortDirection === 'ASC' ? '>' : '<',
      previousSortDirection: sort.sortDirection === 'ASC' ? 'DESC' : 'ASC',
      previousSortDirectionToken: sort.sortDirection === 'ASC' ? '<' : '>',
      nextSortDirection: sort.sortDirection,
      nextSortDirectionToken: sort.sortDirection === 'ASC' ? '>' : '<'
    });

    paginationFieldMap[aliasField] = true;
  }
  // console.log('PAGINATION FIELD FROM SORT', paginationFields);
  if (self.options.paginationPrimaryKeys && self.options.paginationPrimaryKeys.length > 0) {
    for (let i = 0; i < self.options.paginationPrimaryKeys.length; i++) {
      const paginationPrimaryKeyField = self.options.paginationPrimaryKeys[i];
      const aliasField = self._replaceFieldWithAlias(paginationPrimaryKeyField);
      if (!paginationFieldMap[aliasField] ) {
        paginationFieldMap[aliasField] = true;
        
        if (self.options.defaultPaginationPrimaryKeySortDirection) {
          paginationFields.push({
            aliasField: aliasField,
            field: paginationPrimaryKeyField,
            sortDirection: self.options.defaultPaginationPrimaryKeySortDirection === 'DESC' ? 'DESC' : 'ASC',
            sortDirectionToken: self.options.defaultPaginationPrimaryKeySortDirection === 'DESC' ? '<' : '>',
            previousSortDirection: self.options.defaultPaginationPrimaryKeySortDirection === 'DESC' ? 'ASC' : 'DESC',
            previousSortDirectionToken: self.options.defaultPaginationPrimaryKeySortDirection === 'DESC' ? '>' : '<',
            nextSortDirection: self.options.defaultPaginationPrimaryKeySortDirection === 'DESC' ? 'DESC' : 'ASC',
            nextSortDirectionToken: self.options.defaultPaginationPrimaryKeySortDirection === 'DESC' ? '<' : '>'
          });
        } else {
          paginationFields.push({
            aliasField: aliasField,
            field: paginationPrimaryKeyField,
            sortDirection: 'ASC',
            sortDirectionToken: '>',
            previousSortDirection: 'DESC',
            previousSortDirectionToken: '<',
            nextSortDirection: 'ASC',
            nextSortDirectionToken: '>'
          });
        }
      }
    }
    // console.log('PAGINATION FIELD AFTER MERGE', paginationFields);
  }

  return paginationFields;
};

module.exports = EventstorePlaybackListView;
