const _ = require('lodash');
const debug = require('debug')('eventstore:projection-store');
const mysql = require('mysql');

/**
 * EventstoreProjectionStoreOptions
 * @typedef {Object} EventstoreProjectionStoreOptions
 * @property {String} host the mysql host
 * @property {Number} port the mysql port
 * @property {String} user the mysql user
 * @property {String} password the mysql password
 * @property {String} database the mysql database name
 * @property {Number} connectionLimit the max coonnections in the pool. default is 1
 * @property {String} name the name of the store. for mysql, this is the table name
 */

/**
 * @param {EventstoreProjectionStoreOptions} options additional options for the Eventstore projection extension
 * @constructor
 */
function EventstoreProjectionStore(options) {
    options = options || {};
    var defaults = {
        name: 'projections',
        connectionLimit: 1
    };

    this.options = _.defaults(options, defaults);

    if (!this.options.host) {
        throw new Error('host is required');
    }

    if (!this.options.port) {
        throw new Error('port is required');
    }

    if (!this.options.user) {
        throw new Error('user is required');
    }

    if (!this.options.password) {
        throw new Error('password is required');
    }

    if (!this.options.database) {
        throw new Error('database is required');
    }
}

EventstoreProjectionStore.prototype._pool;
EventstoreProjectionStore.prototype._isInit;

/**
 * @returns {Promise<void>} - returns a Promise of type void
 */
EventstoreProjectionStore.prototype.init = async function() {
    // for reentrant.
    // dont init if init is called once
    if (!this._isInit) {
        const tableName = this.options.name;

        this._pool = mysql.createPool({
            host: this.options.host,
            port: this.options.port,
            user: this.options.user,
            password: this.options.password,
            database: this.options.database,
            connectionLimit: this.options.connectionPoolLimit
        });

        // initialize the store here
        // you can use whatever store you want here (playbacklist, redis, mysql, etc.)

        const query = `CREATE TABLE IF NOT EXISTS \`${tableName}\`
            (
                projection_id VARCHAR(100) PRIMARY KEY,
                config JSON NOT NULL,
                offset BIGINT NULL,
                processed_date BIGINT NULL,
                is_processing TINYINT NULL
            );`;

        await this._executeSqlQuery(query);
        this._isInit = true;
    }
};

/**
 * Stores the projection if it does not exist. Ignores if exists
 * @param {import('./eventstore-projection').Projection} projection the projection to add
 * @returns {Promise<void>} - returns a Promise of type void
 */
EventstoreProjectionStore.prototype.createProjectionIfNotExists = async function(projection) {
    const config = JSON.stringify(projection, function(key, value) {
        if (typeof value === "function") {
            return "/Function(" + value.toString() + ")/";
        }
        return value;
    });

    const query = `INSERT IGNORE INTO ${this.options.name}
                    (
                        projection_id,
                        config,
                        offset,
                        processed_date,
                        is_processing
                    ) VALUES
                    (
                        ?,
                        ?,
                        NULL,
                        NULL,
                        NULL
                    )`;

    debug('createProjectionIfNotExists query', query);
    await this._executeSqlQuery(query, [projection.projectionId, config]);
};

/**
 * Clears all data in the store
 * @returns {Promise<void>} - returns a Promise of type void
 */
EventstoreProjectionStore.prototype.clearAll = async function() {
    const query = `TRUNCATE TABLE ${this.options.name};`;
    await this._executeSqlQuery(query);
};

/**
 * @param {String} projectionId the projection to id to use
 * @returns {Promise<import('./eventstore-projection').Projection>} returns the projection
 */
EventstoreProjectionStore.prototype.getProjection = async function(projectionId) {
    const query = `SELECT * FROM ${this.options.name} WHERE projection_id = ? LIMIT 1;`;

    const resultRows = await this._executeSqlQuery(query, [projectionId]);

    if (resultRows && resultRows.length > 0) {
        const row = resultRows[0];
        debug('getProjection.result', row);
        const projection = JSON.parse(row.config, function(key, value) {
            if (typeof value === "string" &&
                value.startsWith("/Function(") &&
                value.endsWith(")/")) {
                value = value.substring(10, value.length - 2);
                return (0, eval)("(" + value + ")");
            }
            return value;
        });

        projection.processedDate = row.processed_date;
        projection.isProcessing = row.is_processing;
        projection.offset = row.offset;
        return projection;
    } else {
        return null;
    }
};

/**
 * @param {String} projectionId the projectionId to update
 * @param {Number} processedDate the date when the projection is processed
 * @param {Number} [newOffset] optional. also sets the new offset
 * @returns {Promise<void>} - returns a Promise of type void
 */
EventstoreProjectionStore.prototype.setProcessed = async function(projectionId, processedDate, newOffset, isProcessing) {
    let params;
    let query;
    if (newOffset) {
        query = `UPDATE ${this.options.name}
                    SET processed_date = ?,
                        offset = ?,
                        is_processing = ?
                    WHERE projection_id = ?;`;
        params = [processedDate, newOffset, isProcessing, projectionId];
    } else {
        query = `UPDATE ${this.options.name}
                    SET processed_date = ?, 
                        is_processing = ?
                    WHERE projection_id = ?;`;
        params = [processedDate, isProcessing, projectionId];
    }

    debug('setProcessed query', query, params);
    await this._executeSqlQuery(query, params);
};

/**
 * @param {String} queryText sql query string to be executed
 * @returns {Promise<Object>} - returns a Promise of type Object where object is the Query result
 */
EventstoreProjectionStore.prototype._executeSqlQuery = async function(queryText, queryParams) {
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
EventstoreProjectionStore.prototype._doQueryWithConnection = async function(connection, queryText, queryParams) {
    return new Promise((resolve, reject) => {
        try {
            debug(`_doQueryWithConnection ${queryText}`);
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
EventstoreProjectionStore.prototype._getConnection = async function() {
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
EventstoreProjectionStore.prototype._releaseConnection = async function(conn) {
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

module.exports = EventstoreProjectionStore;