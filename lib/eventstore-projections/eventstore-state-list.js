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
    this._set(index, state).then(cb).catch(cb);
};

/**
 * @param {Object} state the state of the item to add
 * @param {EventstoreStateListDoneCallback} cb the callback to call after the operation
 * @returns {void} - returns void. use the cb to get results
 */
EventstoreStateList.prototype.push = function(state, cb) {
    // push on end
    this._push(state).then(cb).catch(cb);
};

/**
 * @param {EventstoreStateListDoneCallback} cb the callback to call after the operation
 * @returns {void} - returns void. use the cb to get results
 */
EventstoreStateList.prototype.count = function(cb) {
    this._count().then(cb).catch(cb);
};

/**
 * @param {Number} start the start index to get the list
 * @param {Number} limit the number of items to get
 * @param {Object} filters not yet implemented
 * @param {Object} sorting not yet implemented
 * @param {EventstoreStateListDoneCallback} cb the callback to call after the operation
 * @returns {void} - returns void. use the cb to get results
 */
EventstoreStateList.prototype.get = function(start, limit, filters, sorting, cb) {
    this._get(start, limit, filters, sorting).then(cb).catch(cb);
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
    
        this._data.lastId = results.insertId;
        this._data.rowIndex = index;
        return this._data;
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
        await this._insertRow('UPDATE', index, state);
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
        const count = await this._count();
        await this._insertRow('CREATE', count, state);
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
                        MAX(t.row_index) as max_row_index
                    FROM (
                        SELECT
                            row_type,
                            row_index,
                            ROW_NUMBER() OVER (PARTITION BY row_index ORDER BY id DESC) row_num
                        FROM ${this.options.listName}
                        WHERE id <= ?
                    ) AS t
                    WHERE t.row_num = 1 AND t.row_type <> "DELETE"`;
        const whereParams = [
            lastId
        ];

        const resultCount = await this._executeSqlQuery(countQuery, whereParams);
        const count = (resultCount.length > 0 && !isNaN(parseInt(resultCount[0].max_row_index)) ? (resultCount[0].max_row_index + 1) : 0);
        return count;
    } catch (error) {
        console.error('error in stateList._count with error', error);
        throw error;
    }
};

/**
 * @param {Number} start the start index to get the list
 * @param {Number} limit the number of items to get
 * @param {Object} filters not yet implemented
 * @param {Object} sorting not yet implemented
 * @returns {Promise<Array>} - returns an array of rows
 */
EventstoreStateList.prototype._get = async function(start, limit, filters, sorting) {
    try {
        // TODO: implement filters and sorting
        var lastId = (isNaN(this._data.lastId) ? 0 : this._data.lastId);
        var sOrd = 'ASC';

        var getQuery = `
        SELECT
            t.state_json
        FROM (
            SELECT
                t.row_index,
                t.state_json
            FROM (
                SELECT
                    row_type,
                    row_index,
                    state_json,
                    ROW_NUMBER() OVER (PARTITION BY row_index ORDER BY id DESC) row_num
                FROM ${this.options.listName}
                WHERE id <= ?
            ) AS t
            WHERE t.row_num = 1 AND t.row_type <> "DELETE"
        ) AS t
        WHERE t.row_index >= ?
        ORDER BY t.row_index ${sOrd}
    ${limit > 0 ? ('LIMIT ' + limit) : ''}`;
        var whereParams = [
            lastId,
            start
        ];

        const results = this._executeSqlQuery(getQuery, whereParams);

        // translate count/results
        const rows = results.map((x) => (x.state_json ? JSON.parse(x.state_json) : undefined));
        return rows;
    } catch (error) {
        console.error('error in stateList.get with parameters and error', start || 'null', limit || 'null', filters || 'null', sorting || 'null', error);
        throw error;
    }
};

module.exports = EventstoreStateList;