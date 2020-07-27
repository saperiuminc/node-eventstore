const _ = require('lodash');
const debug = require('debug')('eventstore:playback-list')

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
 * @property {("or"|"and")} groupBooleanOperator the operator for the group
 * @property {("is"|"any"|"range"|"contains")} operator The operator to use. 
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
 * @property {String} listName the name of this list
 * @property {Object.<string, EventstorePlaybackListSecondaryKey[]>} secondaryKeys the secondary keys that make up the non clustered index. A key value pair were key is the secondaryKey name and value an array of fields
 * @property {EventstorePlaybackListField[]} fields the secondary keys that make up the non clustered index
 */

/**
 * @param {EventstorePlaybackListOptions} options additional options for the Eventstore playback list
 * @constructor
 */
function EventstorePlaybackList(options) {
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
 * @type {EventstorePlaybackListOptions}
 */
EventstorePlaybackList.prototype.options;

EventstorePlaybackList.prototype._connection;

/**
 * @param {EventstorePlaybackListQueryDoneCallback} cb callback to be called when the query is done retrieving data
 * @returns {void} - returns void. use the callback for the result (cb)
 */
EventstorePlaybackList.prototype.init = async function() {
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
                        fieldsString += `\`${field.name}\` varchar(250) GENERATED ALWAYS AS (\`row_json\` ->> '$.${field.name}')`;
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
 * @param {Number} start zero-index start position of the rows to get
 * @param {Number} limit how many rows we want to get
 * @param {EventstorePlaybackListFilter[]} filters filter parameters for the query
 * @param {EventstorePlaybackListSort[]} sort sort parameters for the query
 * @param {EventstorePlaybackListQueryDoneCallback} cb callback to be called when the query is done retrieving data
 * @returns {void} - returns void. use the callback for the result (cb)
 */
EventstorePlaybackList.prototype.query = function(start, limit, filters, sort, cb) {
    this._query(start, limit, filters, sort).then((results) => cb(null, results)).catch(cb);
};

/**
 * @param {String} queryText sql query string to be executed
 * @returns {Promise<Object>} - returns a Promise of type Object where object is the Query result
 */
EventstorePlaybackList.prototype._executeSqlQuery = async function(queryText, queryParams) {
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
 * @param {EventstorePlaybackListFilter[]} filters filter parameters for the query
 * @returns {String} - returns the filters in string
 */
EventstorePlaybackList.prototype._listFiltersToFilterString = function(filters) {
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
EventstorePlaybackList.prototype._listSortingToSortingString = function(sorting) {
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

/**
 * @param {String} filterString the filter string
 * @param {EventstorePlaybackListFilter[]} filters filter parameters for the query
 * @param {String} groupBooleanOperator group operator "or" or "and"
 * @returns {String} - returns the filters in string
 */
EventstorePlaybackList.prototype._appendFiltersToFilterString = function(filterString, filters, groupBooleanOperator) {
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

/**
 * @param {Number} start zero-index start position of the rows to get
 * @param {Number} limit how many rows we want to get
 * @param {EventstorePlaybackListFilter[]} filters filter parameters for the query
 * @param {EventstorePlaybackListSort[]} sort sort parameters for the query
 * @returns {Promise<EventstorePlaybackListQueryResult} - returns void. use the callback for the result (cb)
 */
EventstorePlaybackList.prototype._query = async function(start, limit, filters, sort) {
    try {
        debug('query called with params:', start, limit, filters, sort);
        
        let filterString = this._listFiltersToFilterString(filters);
        let orderByString = this._listSortingToSortingString(sort);

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

/**
 * @param {String} rowId the unique id for this row
 * @param {Number} revision the row revision
 * @param {Object} data the data to add
 * @param {Object} meta optional metadata
 * @param {EventstorePlaybackListDoneCallback} cb callback to call when operation is done
 * @returns {void} - returns void. use the callback for the result (cb)
 */
EventstorePlaybackList.prototype.add = function(rowId, revision, data, meta, cb) {
    this._add(rowId, revision, data, meta).then(cb).catch(cb);
};

/**
 * @param {String} rowId the unique id for this row
 * @param {Number} revision the row revision
 * @param {Object} data the data to add
 * @param {Object} meta optional metadata
 * @returns {Promise<void>} - returns Promise of type void
 */
EventstorePlaybackList.prototype._add = async function(rowId, revision, data, meta) {
    try {
        debug('add called with params:', rowId, revision, data, meta);
        var rowData = {
            row_id: rowId,
            row_revision: revision,
            row_json: (data ? JSON.stringify(data) : null),
            meta_json: (meta ? JSON.stringify(meta) : null)
        };
        await this._executeSqlQuery(`INSERT IGNORE INTO ${this.options.listName} SET ?`, rowData);
    } catch (error) {
        console.error('error in add with params and error:', rowId, revision, data, meta, error);
        throw error;
    }
};

/**
 * @param {String} rowId the unique id for this row
 * @param {Number} revision the row revision
 * @param {Object} oldData Old data that we got from get
 * @param {Object} newData New data to persist
 * @param {Object} meta optional metadata
 * @param {EventstorePlaybackListDoneCallback} cb callback to call when operation is done
 * @returns {void} - returns void. use the callback for the result (cb)
 */
EventstorePlaybackList.prototype.update = function(rowId, revision, oldData, newData, meta, cb) {
    this._update(rowId, revision, oldData, newData, meta).then(cb).catch(cb);
};

/**
 * @param {String} rowId the unique id for this row
 * @param {Number} revision the row revision
 * @param {Object} oldData Old data that we got from get
 * @param {Object} newData New data to persist
 * @param {Object} meta optional metadata
 * @returns {Promise<void>} - returns Promise of type void
 */
EventstorePlaybackList.prototype._update = async function(rowId, revision, oldData, newData, meta) {
    try {
        debug('update called with params:', rowId, revision, data, meta);

        var data = Object.assign(oldData, newData);
        var updateParams = [
            revision,
            (data ? JSON.stringify(data) : null),
            (meta ? JSON.stringify(meta) : null),
            rowId
        ];

        await this._executeSqlQuery(`UPDATE ${this.options.listName} SET row_revision = ?, row_json = ?, meta_json = ?  WHERE row_id = ?`, updateParams);

    } catch (error) {
        console.error('error in update with params and error:', rowId, revision, oldData, newData, meta, error);
        throw error;
    }
};

/**
 * @param {String} rowId the unique id for this row
 * @param {EventstorePlaybackListDoneCallback} cb callback to call when operation is done
 * @returns {void} - returns void. use the callback for the result (cb)
 */
EventstorePlaybackList.prototype.delete = function(rowId, cb) {
    this._delete(rowId).then(cb).catch(cb);
};

/**
 * @param {String} rowId the unique id for this row
 * @returns {Promise<void>} - returns Promise of type void
 */
EventstorePlaybackList.prototype._delete = async function(rowId) {
    try {
        debug('delete called with params:', rowId);
        var whereParams = [
            rowId
        ];
        // NOTE: do actual/hard-deletes
        await this._executeSqlQuery(`DELETE FROM ${this.options.listName} WHERE row_id = ?`, whereParams);
    } catch (error) {
        console.error('error in delete with params and error:', rowId, error);
        throw error;
    }
};

/**
 * @param {String} rowId the unique id for this row
 * @param {EventstorePlaybackListDoneCallback} cb callback to call when operation is done
 * @returns {void} - returns void. use the callback for the result (cb)
 */
EventstorePlaybackList.prototype.get = function(rowId, cb) {
    this._get(rowId).then((item) => {
        cb(null, item);
    }).catch(cb);
};

/**
 * @param {String} rowId the unique id for this row
 * @param {EventstorePlaybackListDoneCallback} cb callback to call when operation is done
 * @returns {Promise<void>} - returns Promise of type void
 */
EventstorePlaybackList.prototype._get = async function(rowId) {
    try {
        debug('get called with params:', rowId);

        const getQuery = `
                            SELECT
                                row_revision,
                                row_json,
                                meta_json
                            FROM ${this.options.listName}
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

module.exports = EventstorePlaybackList;
