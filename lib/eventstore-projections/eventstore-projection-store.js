const _ = require('lodash');
const debug = require('debug')('eventstore:projection-store');
const mysqlsharedpool = require('@saperiuminc/mysql-shared-pool');
const serialize = require('serialize-javascript');

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
    options = options || {
        connection: {},
        pool: {}
    };
    options.connection = options.connection || {};
    options.pool = options.pool || {};
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

        this._pool = mysqlsharedpool.createPool(this.options);

        // initialize the store here
        // you can use whatever store you want here (playbacklist, redis, mysql, etc.)

        const query = `CREATE TABLE IF NOT EXISTS \`${tableName}\`
            (
                projection_id VARCHAR(100) PRIMARY KEY,
                projection_name VARCHAR(100) NOT NULL,
                config MEDIUMTEXT NOT NULL,
                offset BIGINT NULL,
                processed_date BIGINT NULL,
                state VARCHAR(7) NOT NULL,
                error TEXT NULL,
                error_offset BIGINT NULL,
                is_idle BOOLEAN NULL
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
 EventstoreProjectionStore.prototype.updateProjection = async function(projection) {
    const config = serialize(projection);

    const query = `UPDATE ${this.options.name}
                    SET config = ?,
                        projection_name = ?
                    WHERE projection_id = ?;`;

    const params = [config, projection.projectionName, projection.projectionId];
    await this._executeSqlQuery(query, params);
};

/**
 * Stores the projection if it does not exist. Ignores if exists
 * @param {import('./eventstore-projection').Projection} projection the projection to add
 * @returns {Promise<void>} - returns a Promise of type void
 */
EventstoreProjectionStore.prototype.createProjection = async function(projection) {
    const config = serialize(projection);

    const query = `INSERT IGNORE INTO ${this.options.name}
                    (
                        projection_id,
                        projection_name,
                        config,
                        offset,
                        processed_date,
                        state
                    ) VALUES
                    (
                        ?,
                        ?,
                        ?,
                        NULL,
                        NULL,
                        'paused'
                    )`;

    debug('createProjectionIfNotExists query', query);
    await this._executeSqlQuery(query, [projection.projectionId, projection.projectionName, config]);
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
        const projection = this._storedProjectionRowToObject(row);
        return projection;
    } else {
        return null;
    }
};


/**
 * @param {String} projectionName the projection to id to use
 * @returns {Promise<import('./eventstore-projection').Projection>} returns the projection
 */
 EventstoreProjectionStore.prototype.getProjectionByName = async function(projectionName) {
    const query = `SELECT * FROM ${this.options.name} WHERE projection_name = ? LIMIT 1;`;

    const resultRows = await this._executeSqlQuery(query, [projectionName]);

    if (resultRows && resultRows.length > 0) {
        const row = resultRows[0];
        debug('getProjection.result', row);
        const projection = this._storedProjectionRowToObject(row);
        return projection;
    } else {
        return null;
    }
};


/**
 * @returns {Promise<Array<import('./eventstore-projection').Projection>>} returns the projections
 */
 EventstoreProjectionStore.prototype.getProjections = async function() {
    const query = `SELECT * FROM ${this.options.name};`;

    const resultRows = await this._executeSqlQuery(query);


    const projections = [];

    if (resultRows && resultRows.length > 0) {
        for (let index = 0; index < resultRows.length; index++) {
            const row = resultRows[index];
            const projection = this._storedProjectionRowToObject(row);
            
            projections.push(projection);
        }
    }

    return projections;
};

/**
 * @param {String} projectionId the projectionId to update
 * @param {Number} processedDate the date when the projection is processed
 * @param {Number} [newOffset] optional. also sets the new offset
 * @returns {Promise<void>} - returns a Promise of type void
 */
EventstoreProjectionStore.prototype.setProcessed = async function(projectionId, processedDate, newOffset, isIdle) {
    let params;
    let query;
    if (newOffset) {
        query = `UPDATE ${this.options.name}
                    SET processed_date = ?,
                        offset = ?,
                        is_idle = ?
                    WHERE projection_id = ?;`;
        params = [processedDate, newOffset, isIdle == true ? 1 : false, projectionId];
    } else {
        query = `UPDATE ${this.options.name}
                    SET processed_date = ?,
                        is_idle = ?
                    WHERE projection_id = ?;`;
        params = [processedDate, isIdle == true ? 1 : false, projectionId];
    }

    debug('setProcessed query', query, params);
    await this._executeSqlQuery(query, params);
};

/**
 * @param {String} projectionId the projectionId to update
 * @param {"running|paused|faulted"} state the projectionId state
 * @returns {Promise<void>} - returns a Promise of type void
 */
 EventstoreProjectionStore.prototype.setState = async function(projectionId, state) {
    const query = `UPDATE ${this.options.name}
                    SET 
                        state = ?
                    WHERE projection_id = ?;`;
    const params = [state, projectionId];

    debug('setState query', query, params);
    await this._executeSqlQuery(query, params);
};

/**
 * @param {String} projectionId the projectionId to update
 * @param {"running|paused|faulted"} state the projection state
 * @param {Number} lastSuccessfulOffset the last successful offset
 * @param {Error} error the error that happened
 * @param {Object} event the event that has an error
 * @param {Number} offset the event that has an error
 * @returns {Promise<void>} - returns a Promise of type void
 */
 EventstoreProjectionStore.prototype.setStateWithError = async function(projectionId, state, lastSuccessfulOffset, error, event, offset) {
    const query = `UPDATE ${this.options.name}
                    SET 
                        state = ?,
                        offset = ?,
                        error = ?,
                        error_offset = ?
                    WHERE projection_id = ?;`;
    const params = [state, 
        lastSuccessfulOffset, 
        JSON.stringify({
            error: JSON.stringify(error, Object.getOwnPropertyNames(error)),
            event: event
        }), 
        offset,
        projectionId];

    debug('setStateWithError query', query, params);
    await this._executeSqlQuery(query, params);
};


 EventstoreProjectionStore.prototype.setOffset = async function(projectionId, offset) {
    const query = `UPDATE ${this.options.name}
                    SET 
                        offset = ?
                    WHERE projection_id = ?;`;
    const params = [offset, projectionId];

    debug('setOffset query', query, params);
    await this._executeSqlQuery(query, params);
};

EventstoreProjectionStore.prototype.delete = async function(projectionId) {
    const query = `DELETE FROM ${this.options.name}
                    WHERE projection_id = ?;`;
    const params = [projectionId];

    debug('delete query', query, params);
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

/**
 * @param {Object} row sql query string to be executed
 * @returns {Promise<Object>} - returns a Promise of type Object where object is the Query result
 */
 EventstoreProjectionStore.prototype._storedProjectionRowToObject = function(row) {
    const projection = eval('(' + row.config + ')');

    projection.processedDate = row.processed_date;
    projection.state = row.state;
    projection.offset = row.offset;
    projection.error = row.error;
    projection.errorOffset = row.error_offset;

    return projection;
};

module.exports = EventstoreProjectionStore;