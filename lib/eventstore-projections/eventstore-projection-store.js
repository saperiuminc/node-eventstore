const _ = require('lodash');
const debug = require('debug')('eventstore:projection-store');
const mysqlsharedpool = require('@saperiuminc/mysql-shared-pool');

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
    this.options = options || {};
    this.options.connection = options.connection || {};
    this.options.pool = options.pool || {};
    const defaultOptions = {
        name: 'projections',
    };
    const defaultPool = {
        min: 1,
        max: 1,
        idleTimeoutMillis: 30000
    };
    this.options = _.defaults(options, defaultOptions);
    this.options.pool = _.defaults(options.pool, defaultPool);

    if (!this.options.connection.host) {
        throw new Error('host is required');
    }

    if (!this.options.connection.port) {
        throw new Error('port is required');
    }

    if (!this.options.connection.user) {
        throw new Error('user is required');
    }

    if (!this.options.connection.password) {
        throw new Error('password is required');
    }

    if (!this.options.connection.database) {
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

        console.log('CREATED POOL WITH OPTIONS', this.options);
        this._pool = mysqlsharedpool.createPool(this.options);

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

    const query = `INSERT IGNORE INTO ${this.options.connection.name}
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
    const query = `TRUNCATE TABLE ${this.options.connection.name};`;
    await this._executeSqlQuery(query);
};

/**
 * @param {String} projectionId the projection to id to use
 * @returns {Promise<import('./eventstore-projection').Projection>} returns the projection
 */
EventstoreProjectionStore.prototype.getProjection = async function(projectionId) {
    const query = `SELECT * FROM ${this.options.connection.name} WHERE projection_id = ? LIMIT 1;`;

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
        query = `UPDATE ${this.options.connection.name}
                    SET processed_date = ?,
                        offset = ?,
                        is_processing = ?
                    WHERE projection_id = ?;`;
        params = [processedDate, newOffset, isProcessing, projectionId];
    } else {
        query = `UPDATE ${this.options.connection.name}
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
    const resultRaw = await this._pool.raw(queryText, queryParams);
    return resultRaw[0];
};

module.exports = EventstoreProjectionStore;