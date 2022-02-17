const _ = require('lodash');
const debug = require('debug')('eventstore:state-list')
const mysqlsharedpool = require('@saperiuminc/mysql2-shared-pool');
const util = require('util');
const BaseStateListStore = require('./base.eventstore-statelist-store');

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
function EventstoreStateListMySqlStore(options) {
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

  options.connection = Object.assign(_.clone(defaultConnection), options.connection);
  options.pool = Object.assign(_.clone(defaultPool), options.pool);

  this.options = options;
}

util.inherits(EventstoreStateListMySqlStore, BaseStateListStore);

/**
 * @type {EventstoreStateListOptions}
 */
EventstoreStateListMySqlStore.prototype.options;

EventstoreStateListMySqlStore.prototype._pool;

/**
 * @type {EventstoreStateListData}
 */
EventstoreStateListMySqlStore.prototype._data;

/**
 * @param {EventstoreStateListDoneCallback} cb callback to be called when the query is done retrieving data
 * @returns {void} - returns void. use the callback for the result (cb)
 */
EventstoreStateListMySqlStore.prototype.init = async function () {
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

    this._data = {};


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

EventstoreStateListMySqlStore.prototype.createList = async function (stateListConfig) {
  try {
    // build the create table script
    let fieldsString = '';
    const fields = stateListConfig.fields;
    if (fields && fields.length > 0) {
      fieldsString += ',';
      fields.forEach(function (field, index) {
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

    if (stateListConfig.secondaryKeys) {
      for (var key in stateListConfig.secondaryKeys) {
        if (Object.prototype.hasOwnProperty.call(stateListConfig.secondaryKeys, key)) {
          const keyObject = stateListConfig.secondaryKeys[key];
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

    let indexString = fieldsString.length > 0 ? ',' : '';
    indexString += 'INDEX idx_row_index (row_index)';

    const query = `CREATE TABLE IF NOT EXISTS ${stateListConfig.name} 
            (
                id INT AUTO_INCREMENT PRIMARY KEY,
                row_type VARCHAR(10) NOT NULL,
                row_index INT,
                row_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                state_json JSON,
                meta_json JSON
                ${fieldsString} 
                ${indexString}
                ${secondaryKeyString}
            );`;

    await this._querySql(query);
  } catch (error) {
    console.error('error in createList', stateListConfig, error);
    throw error;
  }

}

/**
 * @param {Number} index the index of the row to delete
 * @param {EventstoreStateListDoneCallback} cb the callback to call after the operation
 * @returns {void} - returns void. use the cb to get results
 */
EventstoreStateListMySqlStore.prototype.delete = async function (listName, index) {
  try {
    const sql = ` 
            INSERT INTO  
                ${listName} 
            (
                row_type, 
                row_index, 
                state_json, 
                meta_json
            )
            SELECT 
                'DELETE',
                ${index},
                state_json,
                meta_json
            FROM 
                ${listName} 
            WHERE
                row_index = ${index}
            ORDER BY 
                id desc
            LIMIT 1;`;

    const results = await this._querySql(sql);

    return results.insertId;
  } catch (error) {
    console.error('error in stateList.delete with params and error', listName, index, error);
    throw error;
  }
};

/**
 * @param {Number} index the index of the row to delete
 * @param {Object} state the new state of the item to update
 * @param {EventstoreStateListDoneCallback} cb the callback to call after the operation
 * @returns {void} - returns void. use the cb to get results
 */
EventstoreStateListMySqlStore.prototype.set = async function (listName, index, state, meta) {
  try {
    return await this._insertRow(listName, 'UPDATE', index, state, meta);
  } catch (error) {
    console.error('error in stateList._set with params and error', listName, index || 'null', state || 'null', error);
    throw error;
  }
};

/**
 * @param {Object} state the state of the item to add
 * @param {EventstoreStateListDoneCallback} cb the callback to call after the operation
 * @returns {void} - returns void. use the cb to get results
 */
EventstoreStateListMySqlStore.prototype.push = async function (listName, state, meta) {
  // push on end
  try {
    const sql = ` 
                SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
                INSERT INTO  
                    ${listName} 
                (
                    row_type, 
                    row_index, 
                    state_json, 
                    meta_json
                ) SELECT 
                    ?, 
                    CASE WHEN MAX(row_index) IS NULL THEN 0 ELSE max(row_index) + 1 END,
                    ?,
                    ?
                FROM ${listName};`;

    const results = await this._querySql(sql,
      [
        'CREATE',
        (state ? JSON.stringify(state) : null),
        (meta ? JSON.stringify(meta) : null)
      ]);

    return results.insertId;
  } catch (error) {
    console.error('error in stateList.push with params and error', listName, state, error);
    throw error;
  }
};

/**
 * @param {Number} startRowIndex the start index to get the list
 * @param {Number} limit the number of items to get
 * @param {Object} filters not yet implemented
 * @param {EventstoreStateListDoneCallback} cb the callback to call after the operation
 * @returns {void} - returns void. use the cb to get results
 */
EventstoreStateListMySqlStore.prototype.find = async function (listName, lastId, filters) {
  return this._find(listName, lastId, filters);
};

/**
 * @param {Number} startRowIndex the start index to get the list
 * @param {Number} limit the number of items to get
 * @param {Object} filters not yet implemented
 * @param {EventstoreStateListDoneCallback} cb the callback to call after the operation
 * @returns {void} - returns void. use the cb to get results
 */
EventstoreStateListMySqlStore.prototype.filter = async function (listName, lastId, filters) {
  return this._filter(listName, lastId, null, null, filters);
};


/**
 * @param {String} listName the name of the list
 * @returns {Promise}
 */
EventstoreStateListMySqlStore.prototype.truncate = async function (listName) {
  const resultRaw = await this._pool.execute(`TRUNCATE TABLE ${listName};`);
  return resultRaw[0];
};

/**
 * @param {String} listName the name of the list
 * @returns {Promise}
 */
EventstoreStateListMySqlStore.prototype.destroy = async function (listName) {
  const resultRaw = await this._pool.execute(`DROP TABLE IF EXISTS ${listName};`);
  return resultRaw[0];
};

/**
 * @param {String} listName the name of the list
 * @returns {Promise} - returns an array of rows
 */
EventstoreStateListMySqlStore.prototype.deleteList = async function (listName) {
  const resultRaw = await this._pool.execute(`DROP TABLE ${listName};`);
  return resultRaw[0];
};

/**
 * @param {EventStoreStateListRowType} type the type of row to insert. values are DELETE, UPDATE CREATE
 * @param {Number} index the index to insert the new row
 * @param {Object} state the state to insert
 * @param {EventstoreStateListDoneCallback} cb the callback to call after the operation
 * @returns {void} - returns void. use the cb to get results
 */
EventstoreStateListMySqlStore.prototype._insertRow = async function (listName, type, index, state, meta) {
  try {
    var rowData = [
      type,
      index,
      state ? JSON.stringify(state) : null,
      meta ? JSON.stringify(meta) : null
    ];
    const results = await this._executeSql(`INSERT INTO ${listName} (row_type, row_index, state_json, meta_json) VALUES (?, ?, ?, ?)`, rowData);

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
EventstoreStateListMySqlStore.prototype._querySql = async function (queryText, queryParams) {
  const resultRaw = await this._pool.query(queryText, queryParams);
  return resultRaw[0];
};

/**
 * @param {String} queryText callback to be called when the query is done retrieving data
 * @returns {Promise<Object>} - returns a Promise of type Object where object is the Query result
 */
 EventstoreStateListMySqlStore.prototype._executeSql = async function (queryText, queryParams) {
  const resultRaw = await this._pool.execute(queryText, queryParams);
  return resultRaw[0];
};

/**
 * @returns {Promise<Number>} - returns the count in a promise
 */
EventstoreStateListMySqlStore.prototype._nextRowIndex = async function (listName) {
  try {
    const countQuery = `
                        SELECT
                            MAX(row_index) as max_row_index
                        FROM 
                            ${listName}`;

    const resultCount = await this._querySql(countQuery);
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
 * @returns {Promise<Array>} - returns an array of rows
 */
EventstoreStateListMySqlStore.prototype._find = async function (listName, lastId, filters) {
  const rows = await this._filter(listName, lastId, null, null, filters);

  if (rows.length > 0) {
    return rows[0];
  }

  return null;
};

/**
 * @param {String} listName the name of the list
 * @param {Number} [lastId] the lastId pointer. can be null
 * @param {Number} rowIndex the index of the item in the list
 * @returns {Promise<StateListItem>} - returns an item
 */
EventstoreStateListMySqlStore.prototype._get = async function (listName, lastId, rowIndex) {
  try {
    let idFilter = '';
    if (lastId) {
      idFilter = `AND id <= ${lastId}`
    }

    let rowIndexFilter = '';
    if (rowIndex) {
      rowIndexFilter = `AND row_index = ${rowIndex}`
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
                        ${listName}
                    WHERE 
                        1 = 1
                        ${idFilter}
                        ${rowIndexFilter}
                    GROUP BY row_index
                ) rows
            JOIN
                ${listName} list ON list.id = rows.id
            WHERE
                    list.row_type <> "DELETE"
            LIMIT 1
        `;
    const results = await this._querySql(getQuery);

    if (results.length > 0) {
      // translate count/results
      const rows = results.map((x) => {
        return {
          index: x.row_index,
          value: x.state_json ? JSON.parse(x.state_json) : undefined,
          meta: x.meta_json ? JSON.parse(x.meta_json) : undefined
        }
      });

      return rows[0];
    }

    return null;
  } catch (error) {
    console.error('error in stateList._get with parameters and error', listName, lastId, rowIndex, error);
    throw error;
  }
};

/**
 * @param {Number} startRowIndex the start index to get the list
 * @param {Number} limit the number of items to get
 * @param {Object} filters not yet implemented
 * @returns {Promise<Array>} - returns an array of rows
 */
EventstoreStateListMySqlStore.prototype._filter = async function (listName, lastId, startRowIndex, limit, filters) {
  try {
    const additionalFilters = this._listFiltersToFilterString(filters);
    let rowIndexFilter = '';
    if (startRowIndex) {
      rowIndexFilter = `AND row_index >= ${startRowIndex}`;
    }
    let limitFilter = '';
    if (limit) {
      `LIMIT ${limit}`;
    }

    // Q&D: Temporarily removed idFilter due to state list bug
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
                        ${listName}
                    WHERE 
                        1 = 1
                        ${rowIndexFilter}
                        ${additionalFilters}
                    GROUP BY row_index
                ) rows
            JOIN
                ${listName} list ON list.id = rows.id
            WHERE
                    list.row_type <> "DELETE"
            ORDER BY
                list.row_index
            ${limitFilter}
        `;

    const results = await this._querySql(getQuery);

    // translate count/results
    const rows = results.map((x) => {
      return {
        index: x.row_index,
        value: x.state_json ? x.state_json : undefined,
        meta: x.meta_json ? x.meta_json : undefined
      }
    });
    return rows;
  } catch (error) {
    console.error('error in stateList.get with parameters and error', listName, limit || 'null', filters || 'null', error);
    throw error;
  }
};

/**
 * @param {EventstorePlaybackListFilter[]} filters filter parameters for the query
 * @returns {String} - returns the filters in string
 */
EventstoreStateListMySqlStore.prototype._listFiltersToFilterString = function (filters) {
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
 * @param {String} filterString the filter string
 * @param {EventstorePlaybackListFilter[]} filters filter parameters for the query
 * @param {String} groupBooleanOperator group operator "or" or "and"
 * @returns {String} - returns the filters in string
 */
EventstoreStateListMySqlStore.prototype._appendFiltersToFilterString = function (filterString, filters, groupBooleanOperator) {
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
          filterString += ` ${prefix} ${filter.field} = ${inFilter} ${suffix} `;
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
          filterString += ` ${prefix} ${filter.field} IN (${inFilter}) ${suffix}`;
        }
      }
      break;
      case 'range': {
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
      case 'dateRange': {
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
      case 'contains': {
        if (filterValue) {
          filterString += ` ${prefix} ${filter.field} LIKE '%${filterValue}%' ${suffix} `;
        }
        break;
      }
      case 'arrayContains': {
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
      case 'startsWith': {
        if (filterValue) {
          filterString += ` ${prefix} ${filter.field} LIKE '${filterValue}%' ${suffix} `;
        }
        break;
      }
      case 'endsWith': {
        if (filterValue) {
          filterString += ` ${prefix} ${filter.field} LIKE '%${filterValue}' ${suffix} `;
        }
        break;
      }
      case 'exists': {
        filterString += ` ${prefix} ${filter.field} IS NOT NULL ${suffix} `;
        break;
      }
      case 'notExists': {
        filterString += ` ${prefix} ${filter.field} IS NULL ${suffix} `;
        break;
      }
      }
    });
  }
  return filterString;
}


module.exports = EventstoreStateListMySqlStore;
