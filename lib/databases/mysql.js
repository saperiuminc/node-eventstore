/* eslint-disable require-jsdoc */
const util = require('util');
const Store = require('../base');
const _ = require('lodash');
const mysql = require('mysql');
const debug = require('debug')('eventstore:datastore:mysql');
const mysqlsharedpool = require('@saperiuminc/mysql-shared-pool');

function MySqlEventStore(options) {
    const opts = options || {};

    const defaults = {
        eventsTableName: 'events',
        snapshotsTableName: 'snapshots',
        host: '127.0.0.1',
        port: 3306,
        user: 'root',
        password: 'root',
        database: 'eventstore',
        connectionPoolLimit: 1,
        usingEventDispatcher: true,
        eventTypeNameInPayload: 'name'
    };


    this._options = this.options = _.defaults(opts, defaults);

    this._mysql = options.mysql || mysql;
    Store.call(this, options);
}

util.inherits(MySqlEventStore, Store);

_.extend(MySqlEventStore.prototype, {
    _connect: async function() {
        try {
            debug('connect');
            debug(this._options);

            let minPool = !isNaN(this._options.connectionPoolLimit) ? parseInt(this._options.connectionPoolLimit) : 1;
            let maxPool = !isNaN(this._options.connectionPoolLimit) ? parseInt(this._options.connectionPoolLimit) : 1;

            this.pool = mysqlsharedpool.createPool({
                connection: {
                    host: this._options.host,
                    port: this._options.port,
                    user: this._options.user,
                    password: this._options.password,
                    database: this._options.database,
                    multipleStatements: true // mysql eventstore needs this
                },
                pool: {
                    min: minPool,
                    max: maxPool
                }
            });

            await this.pool.raw(`CREATE TABLE IF NOT EXISTS ${this._options.database}.${this.options.eventsTableName} (` +
                `id BIGINT NOT NULL AUTO_INCREMENT, ` +
                `event_id VARCHAR(45) NOT NULL, ` +
                `context VARCHAR(45) NOT NULL, ` +
                `payload JSON NOT NULL, ` +
                `commit_id VARCHAR(45) NOT NULL, ` +
                `position INT(11) NULL, ` +
                `stream_id VARCHAR(100) NULL, ` +
                `aggregate VARCHAR(100) NOT NULL, ` +
                `aggregate_id VARCHAR(100) NOT NULL, ` +
                `commit_stamp BIGINT(20) NOT NULL, ` +
                `commit_sequence INT(11) NOT NULL, ` +
                `stream_revision INT(11) NOT NULL, ` +
                `rest_in_commit_stream INT(11) NOT NULL, ` +
                `dispatched BOOLEAN NOT NULL, ` +
                `PRIMARY KEY (id), ` +
                `UNIQUE KEY (event_id),` +
                `INDEX idx_get_undispatched_events (dispatched),` +
                `INDEX idx_get_events_aggregate_id_stream_revision (aggregate_id, stream_revision),` +
                `INDEX idx_get_events_aggregate_context (aggregate, context, commit_stamp, stream_revision, commit_sequence),` + // ToDo: Review if able to be removed
                `INDEX idx_get_events_context_aggregate_primary_id (context, aggregate, id),` +
                `INDEX idx_get_events_commit_stamp (commit_stamp),` +
                `INDEX idx_get_last_events_aggregate_id (aggregate_id ASC, commit_stamp DESC, stream_revision DESC, commit_sequence DESC)` +
                `)`);

            await this.pool.raw(`CREATE TABLE IF NOT EXISTS ${this._options.database}.${this.options.snapshotsTableName} (` +
                `id VARCHAR(250) NOT NULL, ` +
                `data JSON NOT NULL, ` +
                `context VARCHAR(45) NOT NULL, ` +
                `revision INT(11) NOT NULL, ` +
                `stream_id VARCHAR(100) NULL, ` +
                `aggregate VARCHAR(100) NOT NULL, ` +
                `aggregate_id VARCHAR(100) NOT NULL, ` +
                `commit_stamp BIGINT(20) NOT NULL, ` +
                `PRIMARY KEY (id), ` +
                `INDEX idx_get_snapshot (context, aggregate, aggregate_id, commit_stamp)` +
                `)`);
        } catch (error) {
            console.error('error in creating pool', error);
        }
    },

    connect: function(callback) {
        this._connect().then((data) => callback(null, data)).catch(callback);
    },

    disconnect: function(callback) {
        const self = this;
        self.emit('disconnect');
        if (callback) callback(null);
    },

    _clear: async function() {
        const promises = [];

        return Promise.all([
            this.pool.raw(`TRUNCATE TABLE ${this._options.database}.${this.options.eventsTableName}`),
            this.pool.raw(`TRUNCATE TABLE ${this._options.database}.${this.options.snapshotsTableName}`)
        ]);
    },

    clear: function(done) {
        this._clear().then(data => done(null, data)).catch(done);
    },

    _addEvents: async function(events) {
        const self = this;

        try {
            const sql = ` 
            INSERT INTO  
                ${self._options.database}.${self.options.eventsTableName} 
            (
                event_id, 
                context, 
                payload, 
                commit_id, 
                position, 
                stream_id, 
                aggregate, 
                aggregate_id, 
                commit_stamp, 
                commit_sequence, 
                stream_revision, 
                rest_in_commit_stream,
                dispatched
            ) VALUES ?;`;
    
            const newEvents = [];
            for (let index = 0; index < events.length; index++) {
                const event = _.cloneDeep(events[index]);
                event.commitStamp = new Date(event.commitStamp).getTime();

                newEvents.push([event.id,
                    event.context,
                    JSON.stringify(event.payload),
                    event.commitId,
                    _.isNil(event.position) ? null : event.position,
                    event.streamId,
                    event.aggregate,
                    event.aggregateId,
                    event.commitStamp,
                    event.commitSequence,
                    event.streamRevision,
                    event.restInCommitStream,
                    0
                ]);
            }

            await self.pool.raw(sql, [newEvents])

            // return Promise.all(promises);
        } catch (error) {
            console.error('_addEvents error', error);
            throw error;
        }

    },
    /*
     *  Adds all events to the database.
     *  Events added should be atomic: Either all or none are added.
     *  This query is automatically rolled back should this fail.
     */
    addEvents: function(events, callback) {
        this._addEvents(events).then(data => callback(null, data)).catch(callback);
    },

    _getEvents: async function(query, skip, limit) {
        const self = this;

        const appendInnerQueryElseWhere = function(innerQueryString, length, predicate, elsePredicate) {
            if (length > 0) {
                innerQueryString = innerQueryString + predicate;
            } else {
                innerQueryString = innerQueryString + (elsePredicate ? elsePredicate : '');
            }
            return innerQueryString;
        };

        // Inner query - perform filtering, sorting, and limit here. But only retrieve the PK
        let innerQueryString = `SELECT id FROM ${self._options.database}.${self.options.eventsTableName}`;
        const params = [];
        let queryArray = [];

        if (!Array.isArray(query)) {
            queryArray.push(query);
        } else {
            queryArray = query;
        }

        if (queryArray.length > 0) {
            innerQueryString = appendInnerQueryElseWhere(innerQueryString, params.length, ` AND `, ` WHERE `);
            innerQueryString = innerQueryString + ' ( ';
            let counter = 0;
            _.forEach(queryArray, function(q) {
                innerQueryString = appendInnerQueryElseWhere(innerQueryString, counter, ` OR `);

                innerQueryString = innerQueryString + ` ( `
                if (q && q.aggregate) {
                    innerQueryString = innerQueryString + `aggregate = ?`;
                    params.push(q.aggregate);
                }
                if (q && q.context) {
                    innerQueryString = appendInnerQueryElseWhere(innerQueryString, params.length, ` AND `);
                    innerQueryString = innerQueryString + `context = ?`;
                    params.push(q.context);
                }
                if (q && q.aggregateId) {
                    innerQueryString = appendInnerQueryElseWhere(innerQueryString, params.length, ` AND `);
                    innerQueryString = innerQueryString + `aggregate_id = ?`;
                    params.push(`${q.aggregateId}`);
                }
                innerQueryString = innerQueryString + ` ) `;
                counter += 1;
            });
            innerQueryString = innerQueryString + ' ) ';
        }

        innerQueryString = appendInnerQueryElseWhere(innerQueryString, params.length, ` AND `, ` WHERE `);
        innerQueryString = innerQueryString + `id > ? ORDER BY id ASC LIMIT ?`
        params.push((!skip || isNaN(skip) ? 0 : skip));
        params.push((!limit || isNaN(limit) ? 0 : limit));

        // Enclose with outer query string for late row lookup of the event json
        const queryString = `SELECT e.* FROM ( ${innerQueryString} ) o JOIN ${self._options.database}.${self.options.eventsTableName} e ON o.id = e.id`;

        const rows = await self.pool.raw(queryString, params);

        let results = [];
        rows.forEach((storedEvent) => {
            const logicalEvent = self._storedEventToEvent(storedEvent);
            results.push(logicalEvent);
        });
        return results;
    },

    getEvents: function(query, skip, limit, callback) {
        this._getEvents(query, skip, limit).then(data => callback(null, data)).catch(callback);
    },

    _getEventsSince: async function(date, skip, limit) {
        // TODO: implement
        const self = this;

        let queryString = `SELECT * FROM ${self._options.database}.${self._options.eventsTableName} WHERE commit_stamp >= ? ORDER BY commit_stamp ASC, stream_revision ASC, commit_sequence ASC LIMIT ? OFFSET ?`;
        const params = [date || 0, (!limit || isNaN(limit) ? 0 : limit), (!skip || isNaN(skip) ? 0 : skip)];

        const rows = await self.pool.raw(
            queryString,
            params
        );
        let results = [];
        rows.forEach((storedEvent) => {
            const event = self._storedEventToEvent(storedEvent);
            results.push(event);
        });
        return results;

    },
    getEventsSince: function(date, skip, limit, callback) {
        this._getEventsSince(date, skip, limit).then(data => callback(null, data)).catch(callback);
    },

    _getEventsByRevision: async function(query, revMin, revMax) {
        try {
            const self = this;
            let queryString = `SELECT * FROM ${self._options.database}.${self.options.eventsTableName}`;
            const params = [];

            if (query && query.aggregate) {
                queryString = queryString + ` WHERE aggregate = ?`
                params.push(query.aggregate);
            }
            if (query && query.context) {
                if (params.length > 0) {
                    queryString = queryString + ` AND `
                } else {
                    queryString = queryString + ` WHERE `
                }
                queryString = queryString + `context = ?`
                params.push(query.context);
            }
            if (query && query.aggregateId) {
                if (params.length > 0) {
                    queryString = queryString + ` AND `
                } else {
                    queryString = queryString + ` WHERE `
                }
                queryString = queryString + `aggregate_id = ?`
                params.push(`${query.aggregateId}`);
            }

            if (revMin && !isNaN(revMin)) {
                if (params.length > 0) {
                    queryString = queryString + ` AND `
                } else {
                    queryString = queryString + ` WHERE `
                }
                queryString = queryString + 'stream_revision >= ?';
                params.push(revMin);
            }

            if (revMax && revMax !== -1) {
                if (params.length > 0) {
                    queryString = queryString + ` AND `
                } else {
                    queryString = queryString + ` WHERE `
                }
                queryString = queryString + 'stream_revision < ?';
                params.push(revMax);
            }

            queryString = queryString + ` ORDER BY commit_stamp ASC, stream_revision ASC, commit_sequence ASC`;

            const rows = await self.pool.raw(queryString, params);

            let results = [];
            rows.forEach((storedEvent) => {
                const event = self._storedEventToEvent(storedEvent);
                results.push(event);
            });
            return results;
        } catch (error) {
            console.error('got error in _getEventsByRevision', error);
            throw error;
        }

    },
    getEventsByRevision: function(query, revMin, revMax, callback) {
        this._getEventsByRevision(query, revMin, revMax).then(data => callback(null, data)).catch(callback);
    },

    _getLastEvent: async function(query) {
        try {
            const self = this;

            let queryString = `SELECT * FROM ${self._options.database}.${self.options.eventsTableName}`;
            const params = [];
            if (query && query.aggregate) {
                queryString = queryString + ` WHERE aggregate = ?`
                params.push(query.aggregate);
            }
            if (query && query.context) {
                if (params.length > 0) {
                    queryString = queryString + ` AND `
                } else {
                    queryString = queryString + ` WHERE `
                }
                queryString = queryString + `context = ?`
                params.push(query.context);
            }
            if (query && query.aggregateId) {
                if (params.length > 0) {
                    queryString = queryString + ` AND `
                } else {
                    queryString = queryString + ` WHERE `
                }
                queryString = queryString + `aggregate_id = ?`
                params.push(`${query.aggregateId}`);
            }

            queryString = queryString + ` ORDER BY commit_stamp DESC, stream_revision DESC, commit_sequence DESC LIMIT 1`;

            const rows = await this.pool.raw(queryString, params);

            let resultEvent = null;
            if (rows.length > 0) {
                const storedEvent = rows[0];
                resultEvent = this._storedEventToEvent(storedEvent);
            }
            return resultEvent;
        } catch (error) {
            console.error('error in _getLastEvent', error);
        }

    },

    getLastEvent: function(query, callback) {
        this._getLastEvent(query).then(data => callback(null, data)).catch(callback);
    },

    _getUndispatchedEvents: async function(query) {
        const self = this;

        let queryString = `SELECT * FROM ${self._options.database}.${self.options.eventsTableName} WHERE dispatched = 0 `;
        const params = [];

        if (query && query.aggregate) {
            queryString = queryString + ` AND aggregate = ?`
            params.push(query.aggregate);
        }
        if (query && query.context) {
            queryString = queryString + ` AND context = ?`
            params.push(query.context);
        }
        if (query && query.aggregateId) {
            queryString = queryString + ` AND aggregate_id = ?`
            params.push(`${query.aggregateId}`);
        }

        queryString = queryString + ` ORDER BY commit_stamp ASC, stream_revision ASC, commit_sequence ASC`;

        const rows = await self.pool.raw(
            queryString,
            params
        );
        let results = [];
        rows.forEach((storedUndispatchedEvent) => {
            const event = self._storedEventToEvent(storedUndispatchedEvent);
            results.push(event);
        });
        return results;

    },
    getUndispatchedEvents: function(query, callback) {
        this._getUndispatchedEvents(query).then((data) => callback(null, data)).catch(callback);
    },

    _setEventToDispatched: async function(id) {
        const self = this;
        await self.pool.raw(
            `UPDATE ${self._options.database}.${self.options.eventsTableName} SET dispatched = 1 WHERE event_id = ?`,
            [id]
        );
    },
    setEventToDispatched: function(id, callback) {
        this._setEventToDispatched(id).then((data) => callback(null, data)).catch(callback);
    },

    _addSnapshot: async function(snapshot) {
        const self = this;

        const clonedSnapshot = _.cloneDeep(snapshot);
        clonedSnapshot.commitStamp = new Date(clonedSnapshot.commitStamp).getTime();
        const newSnapshot = {
            id: clonedSnapshot.id,
            data: JSON.stringify(clonedSnapshot.data),
            context: clonedSnapshot.context,
            revision: clonedSnapshot.revision,
            stream_id: clonedSnapshot.streamId,
            aggregate: clonedSnapshot.aggregate,
            aggregate_id: clonedSnapshot.aggregateId,
            commit_stamp: clonedSnapshot.commitStamp
        };
        await self.pool.raw(
            `INSERT INTO ${self._options.database}.${self.options.snapshotsTableName} SET ?`, [newSnapshot]
        );
    },
    addSnapshot: function(snapshot, callback) {
        this._addSnapshot(snapshot).then((data) => callback(null, data)).catch(callback);
    },

    _getSnapshot: async function(query, revMax) {
        const self = this;

        const rows = await self.pool.raw(
            `SELECT * FROM ${self._options.database}.${self.options.snapshotsTableName} WHERE aggregate_id = ? AND aggregate = ? AND context = ? ORDER BY commit_stamp DESC LIMIT 1`,
            [`${query.aggregateId}`, query.aggregate || null, query.context || null]
        );
        let resultSnapshot = null;
        if (rows.length > 0) {
            const storedSnapshot = rows[0];
            resultSnapshot = self._storedSnapshotToSnapshot(storedSnapshot);
        }

        return resultSnapshot;
    },
    getSnapshot: function(query, revMax, callback) {
        this._getSnapshot(query, revMax).then(data => callback(null, data)).catch(callback);
    },

    /* Private Methods */
    _storedSnapshotToSnapshot: function(storedSnapshot) {
        const logicalSnapshot = {
            id: storedSnapshot.id,
            data: JSON.parse(storedSnapshot.data),
            context: storedSnapshot.context,
            revision: storedSnapshot.revision,
            streamId: storedSnapshot.stream_id,
            aggregate: storedSnapshot.aggregate,
            aggregateId: storedSnapshot.aggregate_id,
            commitStamp: new Date(storedSnapshot.commit_stamp)
        }

        return logicalSnapshot;
    },

    _storedEventToEvent: function(storedEvent) {
        const logicalEvent = {
            id: storedEvent.event_id,
            eventSequence: storedEvent.id,
            context: storedEvent.context,
            payload: JSON.parse(storedEvent.payload),
            commitId: storedEvent.commit_id,
            position: storedEvent.position,
            streamId: storedEvent.stream_id,
            aggregate: storedEvent.aggregate,
            aggregateId: storedEvent.aggregate_id,
            commitStamp: new Date(storedEvent.commit_stamp),
            commitSequence: storedEvent.commit_sequence,
            streamRevision: storedEvent.stream_revision,
            restInCommitStream: storedEvent.rest_in_commit_stream
        }

        return logicalEvent;
    },


    _executeWithConnection: async function(queryFunc, callback) {
        const self = this;

        // Step 1: Get connection
        let conn;
        try {
            conn = await self._getConnection();
        } catch (error) {
            console.error('MySQL_EventStore: Error in attempting to get connection');
            return callback(error);
        }

        // Step 2: Execute query
        let caughtError;
        let results;
        try {
            results = await queryFunc(conn);
        } catch (error) {
            caughtError = error;
        }

        // Step 3: Release the connection
        try {
            await self._releaseConnection(conn);
        } catch (error) {
            console.error('MySQL_EventStore: Error in attempting to release connection');
            console.error(error);
        }

        // Step 4: Resolve or Reject
        if (caughtError) {
            return callback(caughtError)
        } else {
            return callback(null, results);
        }
    },

    _getConnection: function() {
        return new Promise((resolve, reject) => {
            try {
                this.pool.getConnection(function(err, conn) {
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
    },

    _query: function(conn, query, payload) {
        return new Promise((resolve, reject) => {
            try {
                conn.query(query, payload, function(err, results, fields) {
                    if (err) {
                        console.error(`MySQLStore: _query: callback error:`);
                        console.error(err);
                        reject(err);
                    } else {
                        resolve({
                            results: results,
                            fields: fields
                        });
                    }
                });
            } catch (error) {
                console.error(`MySQLStore: _query: try-catch error:`);
                console.error(error);
                reject(error);
            }
        });
    },

    _releaseConnection: function(conn) {
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
    },

    _beginTransaction: async function(conn) {
        return new Promise((resolve, reject) => {
            try {
                conn.beginTransaction(function(err) {
                    if (err) {
                        console.error(`MySQLStore: _beginTransaction: callback error:`);
                        console.error(err);
                        reject(err);
                    } else {
                        resolve();
                    }
                });
            } catch (error) {
                console.error(`MySQLStore: _beginTransaction: try-catch error:`);
                console.error(error);
                reject(error);
            }
        });
    },

    _commitTransaction: function(conn) {
        return new Promise((resolve, reject) => {
            try {
                conn.commit(function(err) {
                    if (err) {
                        console.error(`MySQLStore: _commitTransaction: callback error:`);
                        console.error(err);
                        reject(err);
                    } else {
                        resolve();
                    }
                });
            } catch (error) {
                console.error(`MySQLStore: _commitTransaction: try-catch error:`);
                console.error(error);
                reject(error);
            }
        });
    },

    _rollbackTransaction: function(conn) {
        return new Promise((resolve, reject) => {
            try {
                conn.rollback(function(err) {
                    if (err) {
                        console.error(`MySQLStore: _rollbackTransaction: callback error:`);
                        console.error(err);
                        reject(err);
                    } else {
                        resolve(conn);
                    }
                });
            } catch (error) {
                console.error(`MySQLStore: _rollbackTransaction: try-catch error:`);
                console.error(error);
                reject(error);
            }
        });
    }
});

module.exports = MySqlEventStore;