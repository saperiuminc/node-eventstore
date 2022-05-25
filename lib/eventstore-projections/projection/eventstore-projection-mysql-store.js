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
        projectionsTableName: 'projections',
        projectionTasksTableName: 'projection_tasks',
        projectionCheckpointTableName: 'projection_checkpoint',
        eventsTableName: 'events'
    };
    const defaultPool = {
        min: 1,
        max: 1,
        idleTimeoutMillis: 30000
    };
    this.options = Object.assign(_.clone(defaultOptions), options);
    this.options.pool = Object.assign(_.clone(defaultPool), options.pool);
    
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
        const projectionsTableName = this.options.projectionsTableName;
        const projectionTasksTableName = this.options.projectionTasksTableName;
        const projectionCheckpointTableName = this.options.projectionCheckpointTableName;
        
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
        
        const projectionsTableQuery = `CREATE TABLE IF NOT EXISTS \`${projectionsTableName}\`
        (
            projection_id VARCHAR(100) PRIMARY KEY,
            projection_name VARCHAR(100) NOT NULL,
            config MEDIUMTEXT NOT NULL,
            context VARCHAR(50) NULL,
            state VARCHAR(7) NOT NULL,
            INDEX idx_projection_name (projection_name)
        );`;

        // TODO: REVIEW TEXT offset and errorOffset. Setting an offset varchar limit will limit the num of allowed shards
        const projectionTasksTableQuery = `CREATE TABLE IF NOT EXISTS \`${projectionTasksTableName}\`
        (
            projection_task_id VARCHAR(100) PRIMARY KEY,
            projection_id VARCHAR(100) NOT NULL,
            shard VARCHAR(25) NOT NULL,
            \`partition\` VARCHAR(25) NOT NULL,
            \`offset\` TEXT NULL,
            processed_date BIGINT UNSIGNED NULL,
            is_idle BOOLEAN NULL,
            \`error\` TEXT NULL,
            error_event TEXT NULL,
            error_offset TEXT NULL,
            INDEX idx_projection_id_shard (projection_id, shard)
        );`;

        const projectionCheckpointTableQuery = `CREATE TABLE IF NOT EXISTS \`${projectionCheckpointTableName}\`
        (
            projection_task_id VARCHAR(100) NOT NULL,
            projection_id VARCHAR(100) NOT NULL,
            context VARCHAR(50) NOT NULL,
            aggregate VARCHAR(75) NOT NULL,
            aggregate_id VARCHAR(125) NOT NULL,
            projection_stream_version BIGINT NULL,
            PRIMARY KEY (projection_task_id, context, aggregate, aggregate_id),
            INDEX idx_projection_id (projection_id)
        );`;
    
            
        await this._executeSqlQuery(projectionsTableQuery);
        await this._executeSqlQuery(projectionTasksTableQuery);
        await this._executeSqlQuery(projectionCheckpointTableQuery);
        this._isInit = true;
    }
};
        
/**
* Stores the projection if it does not exist. Ignores if exists
* @param {import('../../eventstore-projections/eventstore-projection').Projection} projection the projection to add
* @returns {Promise<void>} - returns a Promise of type void
*/
EventstoreProjectionMysqlStore.prototype.createProjection = async function (projection) {
    const config = serialize(projection.configuration);
    
    const query = `INSERT IGNORE INTO ${this.options.projectionsTableName}
        (
            projection_id,
            projection_name,
            config,
            context,
            state
        ) VALUES (
            ?,
            ?,
            ?,
            ?,
            'paused'
        )`;

    debug('createProjectionIfNotExists query', query);
    await this._executeSqlQuery(query, [projection.projectionId, projection.configuration.projectionName, config, projection.context]);
};
            
/**
* Stores the projection task if it does not exist. Ignores if exists
* @param {import('../../eventstore-projections/eventstore-projection').ProjectionTask} projectionTask the projection task to add
* @returns {Promise<void>} - returns a Promise of type void
*/
EventstoreProjectionMysqlStore.prototype.createProjectionTask = async function (projectionTask) {
    const query = `INSERT IGNORE INTO ${this.options.projectionTasksTableName}
    (
        projection_task_id,
        projection_id,
        shard,
        \`partition\`,
        \`offset\`,
        processed_date
        ) VALUES
        (
        ?,
        ?,
        ?,
        ?,
        ?,
        NULL
    )`;
            
    await this._executeSqlQuery(query, [projectionTask.projectionTaskId, projectionTask.projectionId, projectionTask.shard, projectionTask.partition, projectionTask.offset]);
};
        
/**
* Updates a specified projection
* @param {import('../../eventstore-projections/eventstore-projection').Projection} projection the projection to update
* @returns {Promise<void>} - returns a Promise of type void
*/
EventstoreProjectionMysqlStore.prototype.updateProjection = async function (projection) {
    const config = serialize(projection.configuration);
    
    const query = `UPDATE ${this.options.projectionsTableName}
        SET config = ?,
        projection_name = ?
        WHERE projection_id = ?;`;
    
    const params = [config, projection.configuration.projectionName, projection.projectionId];
    await this._executeSqlQuery(query, params);
};

/**
* Clears all projections and projection tasks in the store
* @returns {Promise<void>} - returns a Promise of type void
*/
EventstoreProjectionMysqlStore.prototype.clearAll = async function () {
    const projectionTasksTableQuery = `TRUNCATE TABLE ${this.options.projectionTasksTableName};`;
    const projectionsTableQuery = `TRUNCATE TABLE ${this.options.projectionsTableName};`;
    const projectionsCheckpointTableQuery = `TRUNCATE TABLE ${this.options.projectionCheckpointTableName};`;
    
    await this._executeSqlQuery(projectionTasksTableQuery);
    await this._executeSqlQuery(projectionsTableQuery);
    await this._executeSqlQuery(projectionsCheckpointTableQuery);
};

/**
* Gets a specific projection given a projection id
* @param {String} projectionId the projection id to use
* @returns {Promise<import('../../eventstore-projections/eventstore-projection').Projection>} returns the projection
*/
EventstoreProjectionMysqlStore.prototype.getProjection = async function (projectionId) {
    const query = `SELECT * FROM ${this.options.projectionsTableName} WHERE projection_id = ? LIMIT 1;`;
    
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
* Gets a specific projection given a projection name
* @param {String} projectionName the projection name to use
* @returns {Promise<import('../../eventstore-projections/eventstore-projection').Projection>} returns the projection
*/
EventstoreProjectionMysqlStore.prototype.getProjectionByName = async function (projectionName) {
    const query = `SELECT * FROM ${this.options.projectionsTableName} WHERE projection_name = ? LIMIT 1;`;
    
    const resultRows = await this._executeSqlQuery(query, [projectionName]);
    
    if (resultRows && resultRows.length > 0) {
        const row = resultRows[0];
        debug('getProjectionByName.result', row);
        const projection = this._storedProjectionRowToObject(row);
        return projection;
    } else {
        return null;
    }
};

/**
* Gets a specific projection task given a projection task id
* @param {String} projectionTaskId the projection task id to use
* @returns {Promise<import('../../eventstore-projections/eventstore-projection').ProjectionTask>} returns the projection task
*/
EventstoreProjectionMysqlStore.prototype.getProjectionTask = async function (projectionTaskId) {
    const query = `SELECT
        pj.projection_task_id,
        pj.projection_id,
        pj.shard,
        pj.\`partition\`,
        pj.\`offset\`,
        pj.processed_date,
        pj.is_idle,
        pj.error,
        pj.error_event,
        pj.error_offset,
        p.config,
        p.context,
        p.state
        FROM ${this.options.projectionTasksTableName} pj
        JOIN ${this.options.projectionsTableName} p ON p.projection_id = pj.projection_id
        WHERE pj.projection_task_id = ? LIMIT 1;`;
    
    const resultRows = await this._executeSqlQuery(query, [projectionTaskId]);
    
    if (resultRows && resultRows.length > 0) {
        const row = resultRows[0];
        debug('getProjectionTask.result', row);
        const projectionTask = this._storedProjectionTaskRowToObject(row);
        return projectionTask;
    } else {
        return null;
    }
};

/**
* Gets a lists of projections
* @param {String} context
* @returns {Promise<Array<import('../../eventstore-projections/eventstore-projection').Projection>>} returns the projections
*/
EventstoreProjectionMysqlStore.prototype.getProjections = async function (context) {
    let query = `SELECT * FROM ${this.options.projectionsTableName}`;
    let params = [];
    
    if (context) {
        query = query.concat(` WHERE context = ?;`);
        params.push(context);
    } else {
        query = query.concat(`;`);
    }
    
    const resultRows = await this._executeSqlQuery(query, params);
    
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
* Gets a lists of projection tasks
* @param {String} projectionId the projection id to use. optional
* @param {String} shard the shard to use. optional
* @returns {Promise<Array<import('../../eventstore-projections/eventstore-projection').ProjectionTask>>} returns the projection tasks
*/
EventstoreProjectionMysqlStore.prototype.getProjectionTasks = async function (projectionId, shard) {
    let query = `SELECT
        pj.projection_task_id,
        pj.projection_id,
        pj.shard,
        pj.\`partition\`,
        pj.\`offset\`,
        pj.processed_date,
        pj.is_idle,
        pj.error,
        pj.error_event,
        pj.error_offset,
        p.config,
        p.context,
        p.state
        FROM ${this.options.projectionTasksTableName} pj
        JOIN ${this.options.projectionsTableName} p ON p.projection_id = pj.projection_id`;
    let params = [];
    
    if (projectionId !== undefined && shard !== undefined) {
        query = query.concat(` WHERE pj.projection_id = ? AND pj.shard = ?;`);
        params.push(projectionId);
        params.push(shard);
    } else if (projectionId !== undefined) {
        query = query.concat(` WHERE pj.projection_id = ?;`);
        params.push(projectionId);
    } else {
        query = query.concat(`;`);
    }
    
    // console.log('getProjectionTasks:', query, params);
    const resultRows = await this._executeSqlQuery(query, params);
    
    const projectionTasks = [];
    
    if (resultRows && resultRows.length > 0) {
        for (let index = 0; index < resultRows.length; index++) {
            const row = resultRows[index];
            const projectionTask = this._storedProjectionTaskRowToObject(row);
            
            // console.log('PROJECTION TASK:', projectionTask.projectionTaskId);
            projectionTasks.push(projectionTask);
        }
    }
    
    // console.log('PROJECTION TASKS:', projectionTasks.length);
    return projectionTasks;
};

/**
* Gets a lists of projection tasks
* @param {String} context the context to use
* @param {String} shard the shard to use. optional
* @returns {Promise<Array<import('../../eventstore-projections/eventstore-projection').ProjectionTask>>} returns the projection tasks
*/
EventstoreProjectionMysqlStore.prototype.getProjectionTasksByContext = async function (context, shard) {
    let query = `SELECT
        pj.projection_task_id,
        pj.projection_id,
        pj.shard,
        pj.\`partition\`,
        pj.\`offset\`,
        pj.processed_date,
        pj.is_idle,
        pj.error,
        pj.error_event,
        pj.error_offset,
        p.config,
        p.context,
        p.state
        FROM ${this.options.projectionTasksTableName} pj
        JOIN ${this.options.projectionsTableName} p ON p.projection_id = pj.projection_id`;
    let params = [];
    
    if (shard !== undefined) {
        query = query.concat(` WHERE p.context = ? AND pj.shard = ?;`);
        params.push(context);
        params.push(shard);
    } else {
        query = query.concat(` WHERE p.context = ?;`);
        params.push(context);
    }

    // console.log('getProjectionTasksByContext:', query, params);
    const resultRows = await this._executeSqlQuery(query, params);
    
    const projectionTasks = [];
    
    if (resultRows && resultRows.length > 0) {
        for (let index = 0; index < resultRows.length; index++) {
            const row = resultRows[index];
            const projectionTask = this._storedProjectionTaskRowToObject(row);
            
            // console.log('PROJECTION TASK:', projectionTask.projectionTaskId);
            projectionTasks.push(projectionTask);
        }
    }
    
    // console.log('PROJECTION TASKS:', projectionTasks.length);
    return projectionTasks;
};

/**
* Sets a projection task as processed
* @param {String} projectionTaskId the projection task id to update
* @param {Number} processedDate the date when the projection task is processed
* @param {String} newOffset optional. also sets the new offset
* @param {Boolean} isIdle optional. also sets the idle flag
* @returns {Promise<void>} - returns a Promise of type void
*/
EventstoreProjectionMysqlStore.prototype.setProcessed = async function (projectionTaskId, processedDate, newOffset) {
    const updateProjectionTasksQuery = `UPDATE ${this.options.projectionTasksTableName}
        SET processed_date = ?,
        offset = ?
        WHERE projection_task_id = ?;`;
    const updateProjectionTasksParams = [processedDate, newOffset, projectionTaskId];
    
    await this._executeSqlQuery(updateProjectionTasksQuery, updateProjectionTasksParams);
};

/**
* Sets the state of a projection task
* @param {String} projectionId the projectionId to update
* @param {"running|paused|faulted"} state the projectionId state
* @returns {Promise<void>} - returns a Promise of type void
*/
EventstoreProjectionMysqlStore.prototype.setState = async function (projectionId, state) {
    const query = `UPDATE ${this.options.projectionsTableName}
        SET 
        state = ?
        WHERE projection_id = ?;`;
    const params = [state, projectionId];
    
    debug('setState query', query, params);
    await this._executeSqlQuery(query, params);
};

/**
* Sets the error of a projection task
* @param {String} projectionTaskId the projection task id to update
* @param {Error} error the error that happened
* @param {Object} errorEvent the event that has an error
* @param {Number} errorOffset the event that has an error
* @returns {Promise<void>} - returns a Promise of type void
*/
EventstoreProjectionMysqlStore.prototype.setError = async function (projectionTaskId, error, errorEvent, errorOffset) {
    const query = `UPDATE ${this.options.projectionTasksTableName}
        SET 
        error = ?,
        error_event = ?,
        error_offset = ?
        WHERE projection_task_id = ?;`;
    
    const errorDetails = error ? JSON.stringify(error, Object.getOwnPropertyNames(error)) : null;
    
    const params = [errorDetails,
        errorEvent ? JSON.stringify(errorEvent) : null,
        errorOffset || null,
        projectionTaskId
    ];
    
    debug('setError query', query, params);
    await this._executeSqlQuery(query, params);
};

/**
* Sets the offset of a projection task
* @param {String} projectionTaskId the projection task id to update
* @param {String} offset the new offset to set
* @returns {Promise<void>} - returns a Promise of type void
*/
EventstoreProjectionMysqlStore.prototype.setOffset = async function (projectionTaskId, offset) {
    const query = `UPDATE ${this.options.projectionTasksTableName}
        SET 
        offset = ?
        WHERE projection_task_id = ?;`;
    const params = [offset, projectionTaskId];
    
    debug('setOffset query', query, params);
    await this._executeSqlQuery(query, params);
};

/**
* Deletes a projection and all associated projection tasks
* @param {String} projectionId the projection id of the projection to delete.
* @returns {Promise<void>} - returns a Promise of type void
*/
EventstoreProjectionMysqlStore.prototype.deleteProjection = async function (projectionId) {
    const params = [projectionId];
    
    const projectionTasksQuery = `DELETE FROM ${this.options.projectionTasksTableName}
        WHERE projection_id = ?;`;
    const projectionQuery = `DELETE FROM ${this.options.projectionsTableName}
        WHERE projection_id = ?;`;
    const projectionCheckpointQuery = `DELETE FROM ${this.options.projectionCheckpointTableName}
        WHERE projection_id = ?;`;
    
    debug('deleteProjection query: projectionTasks', projectionTasksQuery, params);
    await this._executeSqlQuery(projectionTasksQuery, params);
    
    debug('deleteProjection query: projection', projectionQuery, params);
    await this._executeSqlQuery(projectionQuery, params);

    debug('deleteProjection query: projectionCheckpoint', projectionCheckpointQuery, params);
    await this._executeSqlQuery(projectionCheckpointQuery, params);
};

/**
* Private function that executes a mysql query
* @param {String} queryText sql query string to be executed
* @returns {Promise<Object>} - returns a Promise of type Object where object is the Query result
*/
EventstoreProjectionMysqlStore.prototype._executeSqlQuery = async function (queryText, queryParams) {
    // console.log('SQL QUERY:', queryText, queryParams);
    const resultRaw = await this._pool.execute(queryText, queryParams);
    return resultRaw[0];
};

/**
* Private function that formats a Projection object given a sql row result
* @param {Object} row sql query string to be executed
* @returns {Promise<import('../../eventstore-projections/eventstore-projection').Projection>} - returns a Promise of type Object where object is the Query result
*/
EventstoreProjectionMysqlStore.prototype._storedProjectionRowToObject = function (row) {
    const projectionConfig = eval('(' + row.config + ')');
    
    /**
    * @type {import('../eventstore-projection').Projection}
    */
    const projection = {
        projectionId: row.projection_id,
        configuration: projectionConfig,
        context: row.context,
        state: row.state
    }
    return projection;
};

/**
* Private function that formats a ProjectionTask object given a sql row result
* @param {Object} row sql query string to be executed
* @returns {Promise<import('../../eventstore-projections/eventstore-projection').ProjectionTask>} - returns a Promise of type Object where object is the Query result
*/
EventstoreProjectionMysqlStore.prototype._storedProjectionTaskRowToObject = function (row) {
    const projection = this._storedProjectionRowToObject(row);
    
    /**
    * @type {import('../eventstore-projection').ProjectionTask}
    */
    const projectionTask = {
        projectionTaskId: row.projection_task_id,
        projectionId: row.projection_id,
        shard: row.shard,
        partition: row.partition,
        offset: row.offset,
        processedDate: row.processed_date,
        isIdle: row.is_idle,
        error: row.error,
        errorEvent: row.error_event,
        errorOffset: row.error_offset,
        projection: projection
    };
    return projectionTask;
};


/**
 * @param {String} projectionTaskId the projectionTaskId that processed the stream
 * @param {String} context the stream's context
 * @param {String} aggregate the stream's aggregate
 * @param {String} aggregateId the stream's aggregateId
 * @returns {Promise<number>} the latest processed version, per stream and per projection
 */
EventstoreProjectionMysqlStore.prototype.getProjectionStreamVersion = async function(projectionTaskId, context, aggregate, aggregateId) {
    const query = `SELECT projection_stream_version FROM ${this.options.projectionCheckpointTableName}
        WHERE projection_task_id = ? AND context = ? AND aggregate = ? AND aggregate_id = ?;`;
    const params = [projectionTaskId, context, aggregate, aggregateId];
    const result = await this._executeSqlQuery(query, params);
    // console.log(result);
    if (result.length == 0) {
        return -1;
    }
    return result[0].projection_stream_version;
}


/**
* @param {String} projectionTaskId the projectionTaskId to update
* @param {String} streamId the streamId to update
* @returns {Promise}
*/

// Maybe pass offset instead of incrementing? 
EventstoreProjectionMysqlStore.prototype.updateProjectionStreamVersion = async function(projectionTaskId, context, aggregate, aggregateId, version) {
    const query = `UPDATE ${this.options.projectionCheckpointTableName}
        SET projection_stream_version = ?
        WHERE projection_task_id = ?
        AND context = ?
        AND aggregate = ?
        AND aggregate_id = ?`;
    const params = [version, projectionTaskId, context, aggregate, aggregateId];
    await this._executeSqlQuery(query, params);
}

/**
* @param {String} projectionTaskId the projectionTaskId of the projectionTask that processed the stream
* @param {String} projectionId the projectionId of the projectionTask that processed the stream
* @param {String} context the stream's context
* @param {String} aggregate the stream's aggregate
* @param {String} aggregateId the stream's aggregateId
* @param {String} version the version to set as processed
* @returns {Promise}
*/
EventstoreProjectionMysqlStore.prototype.setProjectionStreamVersion = async function(projectionTaskId, projectionId, context, aggregate, aggregateId, version) {
    const query = `INSERT INTO ${this.options.projectionCheckpointTableName}
        (projection_task_id, projection_id, context, aggregate, aggregate_id, projection_stream_version)
        VALUES (?, ?, ?, ?, ?, ?)
        ON DUPLICATE KEY UPDATE projection_stream_version = ?;`;
    const params = [projectionTaskId, projectionId, context, aggregate, aggregateId, version, version];
    // console.log('INSERTING CHECKPOINT', projectionTaskId, projectionId, context, aggregate, aggregateId, version);
    await this._executeSqlQuery(query, params);
}

module.exports = EventstoreProjectionMysqlStore;
