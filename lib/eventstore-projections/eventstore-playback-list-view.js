const _ = require('lodash');
const debug = require('debug')('eventstore:playback-list-view');

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
 * EventstorePlaybackListFilters
 * @typedef {Object} EventstorePlaybackListFilters
 */

/**
 * EventstorePlaybackListSort
 * @typedef {Object} EventstorePlaybackListSort
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
        query: ''
    };

    this.options = _.defaults(options, defaults);
}

/**
 * @type {EventstorePlaybackListViewOptions}
 */
EventstorePlaybackListView.prototype.options;

EventstorePlaybackListView.prototype._connection;

/**
 * @param {EventstorePlaybackListViewQueryDoneCallback} cb callback to be called when the query is done retrieving data
 * @returns {void} - returns void. use the callback for the result (cb)
 */
EventstorePlaybackListView.prototype.init = async function() {
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

        this._connection = mysql.createConnection({
            host: this.options.host,
            port: this.options.port,
            user: this.options.user,
            password: this.options.password,
            database: this.options.database
        });

        const query = `CREATE OR REPLACE VIEW ${this.options.listName} AS ${this.options.query}`;

        await this._executeSqlQuery(query);
    } catch (error) {
        console.error('error in _init with error:', error);
        throw error;
    }
};

/**
 * @param {String} queryText callback to be called when the query is done retrieving data
 * @returns {Promise<Object>} - returns a Promise of type Object where object is the Query result
 */
EventstorePlaybackListView.prototype._executeSqlQuery = async function(queryText, queryParams) {
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
 * @param {Number} start zero-index start position of the rows to get
 * @param {Number} limit how many rows we want to get
 * @param {EventstorePlaybackListFilters} filters filter parameters for the query
 * @param {EventstorePlaybackListSort} sort sort parameters for the query
 * @param {EventstorePlaybackListQueryDoneCallback} cb callback to be called when the query is done retrieving data
 * @returns {void} - returns void. use the callback for the result (cb)
 */
EventstorePlaybackListView.prototype.query = function(start, limit, filters, sort, cb) {
    this._query(start, limit, filters, sort).then((results) => cb(null, results)).catch(cb);
};

/**
 * @param {Number} start zero-index start position of the rows to get
 * @param {Number} limit how many rows we want to get
 * @param {EventstorePlaybackListFilters} filters filter parameters for the query
 * @param {EventstorePlaybackListSort} sort sort parameters for the query
 * @returns {Promise<EventstorePlaybackListQueryResult} - returns void. use the callback for the result (cb)
 */
EventstorePlaybackListView.prototype._query = async function(start, limit, filters, sort) {
    try {
        debug('query called with params:', start, limit, filters, sort);
        
        let filterString = this._stateListFiltersToFilterString(filters);
        let orderByString = this._stateListSortingToSortingString(sort);

        const countQuery = `SELECT COUNT(1) as total_count FROM ${this.options.listName}`;

        const getQuery = `SELECT * FROM ${this.options.listName} WHERE 1 = 1 ${filterString} ${orderByString} LIMIT ?,?`;
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


EventstorePlaybackListView.prototype._stateListFiltersToFilterString = function(filters) {
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

EventstorePlaybackListView.prototype._stateListSortingToSortingString = function(sorting) {
    let sortingString = '';

    if (sorting && sorting.length > 0) {
        sortingString = 'ORDER BY ';
        sorting.forEach(function(sort, index) {
            sortingString += `${sort.field} ${sort.sort}`;
            if (index < sorting.length - 1) {
                sortingString += ',';
            }
        });
    }

    return sortingString;
};

EventstorePlaybackListView.prototype._appendFiltersToFilterString = function(filterString, filters, groupBooleanOperator) {
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
                    if (filter.value) {
                        filterString += ` ${prefix} ${filter.field} = '${filter.value}' ${suffix} `;
                    }
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
            }
            break;

          }
    });
  }
  return filterString;
}

module.exports = EventstorePlaybackListView;
