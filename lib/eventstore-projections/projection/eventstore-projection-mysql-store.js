const _ = require('lodash');
const debug = require('debug')('eventstore:projection-store');
const mysqlsharedpool = require('@saperiuminc/mysql2-shared-pool');
const serialize = require('serialize-javascript');
const util = require('util');
const BaseEventstoreProjectionStore = require('./base.eventstore-projection-store');


/**
 * @param {BaseEventstoreProjectionStore.EventstoreProjectionStoreOptions} options additional options for the Eventstore projection extension
 * @constructor
 */
function EventstoreProjectionMysqlStore(options) {
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
util.inherits(EventstoreProjectionMysqlStore, BaseEventstoreProjectionStore);

EventstoreProjectionMysqlStore.prototype._pool;
EventstoreProjectionMysqlStore.prototype._isInit;

/**
 * @returns {Promise<void>} - returns a Promise of type void
 */
EventstoreProjectionMysqlStore.prototype.init = async function () {
  // for reentrant.
  // dont init if init is called once
  if (!this._isInit) {
    const tableName = this.options.name;


    this._pool = mysqlsharedpool.createPool({
      host: this.options.connection.host,
      port: this.options.connection.port,
      database: this.options.connection.database,
      user: this.options.connection.user,
      password: this.options.connection.password,
      connectionLimit: this.options.pool.max,
      multipleStatements: true
    });

    // initialize the store here
    // you can use whatever store you want here (playbacklist, redis, mysql, etc.)

    const query = `CREATE TABLE IF NOT EXISTS \`${tableName}\`
            (
                projection_id VARCHAR(100) PRIMARY KEY,
                projection_name VARCHAR(100) NOT NULL,
                config MEDIUMTEXT NOT NULL,
                context VARCHAR(50) NULL,
                offset BIGINT NULL,
                processed_date BIGINT NULL,
                state VARCHAR(7) NOT NULL,
                error TEXT NULL,
                error_event TEXT NULL,
                error_offset BIGINT NULL,
                is_idle BOOLEAN NULL
            );`;

    await this._executeSqlQuery(query);
    this._isInit = true;
  }
};


/**
 * Stores the projection if it does not exist. Ignores if exists
 * @param {import('../../eventstore-projections/eventstore-projection').Projection} projection the projection to add
 * @returns {Promise<void>} - returns a Promise of type void
 */
EventstoreProjectionMysqlStore.prototype.updateProjection = async function (projection) {
  const config = serialize(projection.configuration);

  const query = `UPDATE ${this.options.name}
                    SET config = ?,
                        projection_name = ?
                    WHERE projection_id = ?;`;

  const params = [config, projection.configuration.projectionName, projection.projectionId];
  await this._executeSqlQuery(query, params);
};

/**
 * Stores the projection if it does not exist. Ignores if exists
 * @param {import('../../eventstore-projections/eventstore-projection').Projection} projection the projection to add
 * @returns {Promise<void>} - returns a Promise of type void
 */
EventstoreProjectionMysqlStore.prototype.createProjection = async function (projection) {
  const config = serialize(projection.configuration);

  const query = `INSERT IGNORE INTO ${this.options.name}
                    (
                        projection_id,
                        projection_name,
                        config,
                        context,
                        offset,
                        processed_date,
                        state
                    ) VALUES
                    (
                        ?,
                        ?,
                        ?,
                        ?,
                        NULL,
                        NULL,
                        'paused'
                    )`;

  debug('createProjectionIfNotExists query', query);
  await this._executeSqlQuery(query, [projection.projectionId, projection.configuration.projectionName, config, projection.context]);
};

/**
 * Clears all data in the store
 * @returns {Promise<void>} - returns a Promise of type void
 */
EventstoreProjectionMysqlStore.prototype.clearAll = async function () {
  const query = `TRUNCATE TABLE ${this.options.name};`;
  await this._executeSqlQuery(query);
};

/**
 * @param {String} projectionId the projection to id to use
 * @returns {Promise<import('../../eventstore-projections/eventstore-projection').Projection>} returns the projection
 */
EventstoreProjectionMysqlStore.prototype.getProjection = async function (projectionId) {
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
 * @returns {Promise<import('../../eventstore-projections/eventstore-projection').Projection>} returns the projection
 */
EventstoreProjectionMysqlStore.prototype.getProjectionByName = async function (projectionName) {
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
 * @param {String} context
 * @returns {Promise<Array<import('../../eventstore-projections/eventstore-projection').Projection>>} returns the projections
 */
EventstoreProjectionMysqlStore.prototype.getProjections = async function (context) {
  const contextFilter = context ? `WHERE context = '${context}'` : '';
  const query = `SELECT * FROM ${this.options.name} ${contextFilter};`;

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
 * @param {Number} newOffset optional. also sets the new offset
 * @param {Boolean} isIdle optional. also sets the new offset
 * @returns {Promise<void>} - returns a Promise of type void
 */
EventstoreProjectionMysqlStore.prototype.setProcessed = async function (projectionId, processedDate, newOffset, isIdle) {
  let params;
  let query;
  if (newOffset) {
    query = `UPDATE ${this.options.name}
                    SET processed_date = ?,
                        offset = ?,
                        is_idle = ?
                    WHERE projection_id = ?;`;
    params = [processedDate, newOffset, isIdle == true ? 1 : 0, projectionId];
  } else {
    query = `UPDATE ${this.options.name}
                    SET processed_date = ?,
                        is_idle = ?
                    WHERE projection_id = ?;`;
    params = [processedDate, isIdle == true ? 1 : 0, projectionId];
  }

  debug('setProcessed query', query, params);
  await this._executeSqlQuery(query, params);
};

/**
 * @param {String} projectionId the projectionId to update
 * @param {"running|paused|faulted"} state the projectionId state
 * @returns {Promise<void>} - returns a Promise of type void
 */
EventstoreProjectionMysqlStore.prototype.setState = async function (projectionId, state) {
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
 * @param {Error} error the error that happened
 * @param {Object} errorEvent the event that has an error
 * @param {Number} errorOffset the event that has an error
 * @returns {Promise<void>} - returns a Promise of type void
 */
EventstoreProjectionMysqlStore.prototype.setError = async function (projectionId, error, errorEvent, errorOffset) {
  const query = `UPDATE ${this.options.name}
                    SET 
                        error = ?,
                        error_event = ?,
                        error_offset = ?
                    WHERE projection_id = ?;`;

  const errorDetails = error ? JSON.stringify(error, Object.getOwnPropertyNames(error)) : null;

  const params = [errorDetails,
    errorEvent ? JSON.stringify(errorEvent) : null,
    errorOffset || null,
    projectionId
  ];

  debug('setError query', query, params);
  await this._executeSqlQuery(query, params);
};


EventstoreProjectionMysqlStore.prototype.setOffset = async function (projectionId, offset) {
  const query = `UPDATE ${this.options.name}
                    SET 
                        offset = ?
                    WHERE projection_id = ?;`;
  const params = [offset, projectionId];

  debug('setOffset query', query, params);
  await this._executeSqlQuery(query, params);
};

EventstoreProjectionMysqlStore.prototype.delete = async function (projectionId) {
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
EventstoreProjectionMysqlStore.prototype._executeSqlQuery = async function (queryText, queryParams) {
  const resultRaw = await this._pool.execute(queryText, queryParams);
  return resultRaw[0];
};

/**
 * @param {Object} row sql query string to be executed
 * @returns {Promise<import('../../eventstore-projections/eventstore-projection').Projection>} - returns a Promise of type Object where object is the Query result
 */
EventstoreProjectionMysqlStore.prototype._storedProjectionRowToObject = function (row) {
  const projectionConfig = eval('(' + row.config + ')');

  /**
   * @type {import('../eventstore-projection').Projection}
   */
  const projection = {
    configuration: projectionConfig,
    processedDate: row.processed_date,
    state: row.state,
    offset: row.offset,
    error: row.error,
    errorEvent: row.error_event,
    errorOffset: row.error_offset,
    isIdle: row.is_idle,
    context: row.context,
    projectionId: row.projection_id
  }

  return projection;
};

module.exports = EventstoreProjectionMysqlStore;
