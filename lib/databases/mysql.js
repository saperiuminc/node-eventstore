/* eslint-disable require-jsdoc */
const util = require('util');
const Store = require('../base');
const EventStoreDuplicateError = require('./errors/EventStoreDuplicateError');
const _ = require('lodash');
const mysql = require('mysql');
const debug = require('debug')('eventstore:datastore:mysql');

function MySqlEventStore(options) {
    const opts = options || {};

    const defaults = {
        eventsTableName: 'events',
        undispatchedEventsTableName: 'undispatched_events',
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
    connect: function(callback) {
        const self = this;

        debug('connect');
        debug(self._options);

        return new Promise((resolve, reject) => {
            try {
                self.pool = self._mysql.createPool({
                    host: self._options.host,
                    port: self._options.port,
                    user: self._options.user,
                    password: self._options.password,
                    database: self._options.database,
                    connectionLimit: self._options.connectionPoolLimit,
                });
                debug('MySqlEventStore: Connection Pool Established. Limit: ' + self._options.connectionPoolLimit);
                return resolve();
            } catch (error) {
                debug('MySqlEventStore: Connection Pool Failed to Establish:');
                debug(error);
                return reject(error);
            }
        }).then(() => {
            // Create Events Table if not exists
            debug('MySqlEventStore: Setting up Events Table');
            return new Promise((resolve, reject) => {
                self._initQuery(
                    `CREATE TABLE IF NOT EXISTS ${self._options.database}.${self.options.eventsTableName} (` +
                    `id BIGINT NOT NULL AUTO_INCREMENT, ` +
                    `event_id VARCHAR(45) NOT NULL, ` +
                    `context VARCHAR(45) NOT NULL, ` +
                    `payload JSON NOT NULL, ` +
                    `commit_id VARCHAR(45) NOT NULL, ` +
                    `position INT(11) NULL, ` +
                    `stream_id VARCHAR(45) NOT NULL, ` +
                    `aggregate VARCHAR(45) NOT NULL, ` +
                    `aggregate_id VARCHAR(45) NOT NULL, ` +
                    `commit_stamp BIGINT(20) NOT NULL, ` +
                    `commit_sequence INT(11) NOT NULL, ` +
                    `stream_revision INT(11) NOT NULL, ` +
                    `rest_in_commit_stream INT(11) NOT NULL, ` +
                    `PRIMARY KEY (id), ` + 
                    `UNIQUE KEY (event_id),` +
                    `INDEX idx_get_events_aggregate_id_stream_revision (aggregate_id, stream_revision),` +
                    `INDEX idx_get_events_aggregate_context (aggregate, context, commit_stamp, stream_revision, commit_sequence),` +
                    `INDEX idx_get_events_context (context),` +
                    `INDEX idx_get_events_commit_stamp (commit_stamp),` +
                    `INDEX idx_get_last_events_aggregate_id (aggregate_id ASC, commit_stamp DESC, stream_revision DESC, commit_sequence DESC)` +
                    `)`, (err) => {
                        if (err) {
                            return reject(err);
                        } else {
                            return resolve();
                        }
                    }
                );
            });
        }).then(() => {
            // Create Unsidpatched Events Table if not exists
            debug('MySqlEventStore: Done Setting up Events Table');
            debug('MySqlEventStore: Setting up Undispatched Events Table');
            return new Promise((resolve, reject) => {
                self._initQuery(
                    `CREATE TABLE IF NOT EXISTS ${self._options.database}.${self.options.undispatchedEventsTableName} (` +
                    `id BIGINT NOT NULL AUTO_INCREMENT, ` +
                    `event_id VARCHAR(45) NOT NULL, ` +
                    `context VARCHAR(45) NOT NULL, ` +
                    `payload JSON NOT NULL, ` +
                    `commit_id VARCHAR(45) NOT NULL, ` +
                    `position INT(11) NULL, ` +
                    `stream_id VARCHAR(45) NOT NULL, ` +
                    `aggregate VARCHAR(45) NOT NULL, ` +
                    `aggregate_id VARCHAR(45) NOT NULL, ` +
                    `commit_stamp BIGINT(20) NOT NULL, ` +
                    `commit_sequence INT(11) NOT NULL, ` +
                    `stream_revision INT(11) NOT NULL, ` +
                    `rest_in_commit_stream INT(11) NOT NULL, ` +
                    `PRIMARY KEY (id), ` + 
                    `UNIQUE KEY (event_id),` +
                    `INDEX idx_get_events_aggregate_id (aggregate_id),` +
                    `INDEX idx_get_events_aggregate (aggregate),` +
                    `INDEX idx_get_events_context (context),` +
                    `INDEX idx_get_events_commit_stamp (commit_stamp)` +
                    `)`, (err) => {
                        if (err) {
                            return reject(err);
                        } else {
                            return resolve();
                        }
                    }
                );
            });
        }).then(() => {
            // Create Snapshots Table if not exists
            return new Promise((resolve, reject) => {
                self._initQuery(
                    `CREATE TABLE IF NOT EXISTS ${self._options.database}.${self.options.snapshotsTableName} (` +
                    `id VARCHAR(250) NOT NULL, ` +
                    `data JSON NOT NULL, ` +
                    `context VARCHAR(45) NOT NULL, ` +
                    `revision INT(11) NOT NULL, ` +
                    `stream_id VARCHAR(100) NOT NULL, ` +
                    `aggregate VARCHAR(45) NOT NULL, ` +
                    `aggregate_id VARCHAR(100) NOT NULL, ` +
                    `commit_stamp BIGINT(20) NOT NULL, ` +
                    `PRIMARY KEY (id), ` + 
                    `INDEX idx_get_snapshot (context, aggregate, aggregate_id, commit_stamp)` +
                    `)`, (err) => {
                        if (err) {
                            return reject(err);
                        } else {
                            return resolve();
                        }
                    }
                );
            });
        }).then(() => {
            debug('MySqlEventStore: Done Setting up Snapshots Table');
            debug('MySqlEventStore: Successfully Created Tables');
            self.emit('connect');
            if (callback) callback(null, self);
        }).catch((error) => {
            debug('MySqlEventStore: Failed to Create Tables:');
            debug(error);
            if (callback) callback(error, null);
        });
    },

    disconnect: function(callback) {
        const self = this;
        self.emit('disconnect');
        if (callback) callback(null);
    },

    clear: function(done) {
        const promises = [];
        const self = this;

        promises.push(new Promise((resolve, reject) => {
            self._initQuery(
                `TRUNCATE TABLE ${self._options.database}.${self.options.eventsTableName}`, (err) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve();
                    }
                }
            );
        }));

        promises.push(new Promise((resolve, reject) => {
            self._initQuery(
                `TRUNCATE TABLE ${self._options.database}.${self.options.undispatchedEventsTableName}`, (err) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve();
                    }
                }
            );
        }));

        promises.push(new Promise((resolve, reject) => {
            self._initQuery(
                `TRUNCATE TABLE ${self._options.database}.${self.options.snapshotsTableName}`, (err) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve();
                    }
                }
            );
        }));

        Promise.all(promises)
            .then(() => {
                debug('MySqlEventStore: Successfully Cleared Tables:');
                done(null, self);
            }).catch((err) => {
                debug('MySqlEventStore: Failed to Clear Tables:');
                debug(err);
                done(err);
            })
    },

    /*
     *  Adds all events to the database.
     *  Events added should be atomic: Either all or none are added.
     *  This query is automatically rolled back should this fail.
     */
    addEvents: function(events, callback) {
        const self = this;

        const queryFunc = async function(conn) {
            return self._batchAddEvents(conn, events).catch((error) => {
                // Watch for Duplicate Entry events, then wrap it under a EventStoreDuplicateError and throw.
                if (error && error.code === 'ER_DUP_ENTRY') {
                    return Promise.reject(new EventStoreDuplicateError(error));
                } else {
                    return Promise.reject(error);
                }
            });
        };

        self._executeWithConnection(queryFunc, (err, events) => {
            if (err) {
                if (callback) callback(err, null);
            } else {
                if (callback) {
                    callback(null, events);
                }
            }
        });
    },

    getEvents: function(query, skip, limit, callback) {
        const self = this;

        const queryFunc = async function(conn) {

            // Inner query - perform filtering, sorting, and limit here. But only retrieve the PK
            let innerQueryString = `SELECT id FROM ${self._options.database}.${self.options.eventsTableName}`;
            const params = [];

            if (query && query.aggregate) {
                innerQueryString = innerQueryString + ` WHERE aggregate = ?`
                params.push(query.aggregate);
            }
            if (query && query.context) {
                if (params.length > 0) {
                    innerQueryString = innerQueryString + ` AND `
                } else {
                    innerQueryString = innerQueryString + ` WHERE `
                }
                innerQueryString = innerQueryString + `context = ?`
                params.push(query.context);
            }
            if (query && query.aggregateId) {
                if (params.length > 0) {
                    innerQueryString = innerQueryString + ` AND `
                } else {
                    innerQueryString = innerQueryString + ` WHERE `
                }
                innerQueryString = innerQueryString + `aggregate_id = ?`
                params.push(`${query.aggregateId}`);
            }

            if (params.length > 0) {
                innerQueryString = innerQueryString + ` AND `
            } else {
                innerQueryString = innerQueryString + ` WHERE `
            }
            innerQueryString = innerQueryString + `id > ? ORDER BY id ASC LIMIT ?`
            params.push((!skip || isNaN(skip) ? 0 : skip));
            params.push((!limit || isNaN(limit) ? 0 : limit));

            // Enclose with outer query string for late row lookup of the event json
            const queryString = `SELECT e.* FROM ( ${innerQueryString} ) o JOIN ${self._options.database}.${self.options.eventsTableName} e ON o.id = e.id`;

            const resultsAndFields = await self._query(
                conn,
                queryString,
                params
            );
            let results = [];
            resultsAndFields.results.forEach((storedEvent) => {
                const logicalEvent = self._storedEventToEvent(storedEvent);
                results.push(logicalEvent);
            });
            return results;
        };
        self._executeWithConnection(queryFunc, (err, events) => {
            if (err) {
                if (callback) callback(err, null);
            } else {
                if (callback) {
                    callback(null, events);
                }
            }
        });
    },

    getEventsSince: function(date, skip, limit, callback) {
        const self = this;

        const queryFunc = async function(conn) {
            let queryString = `SELECT * FROM ${self._options.database}.${self._options.eventsTableName} WHERE commit_stamp >= ? ORDER BY commit_stamp ASC, stream_revision ASC, commit_sequence ASC LIMIT ? OFFSET ?`;
            const params = [date || 0, (!limit || isNaN(limit) ? 0 : limit), (!skip || isNaN(skip) ? 0 : skip)];

            const resultsAndFields = await self._query(
                conn,
                queryString,
                params
            );
            let results = [];
            resultsAndFields.results.forEach((storedEvent) => {
                const event = self._storedEventToEvent(storedEvent);
                results.push(event);
            });
            return results;
        };
        self._executeWithConnection(queryFunc, (err, events) => {
            if (err) {
                if (callback) callback(err, null);
            } else {
                if (callback) {
                    callback(null, events);
                }
            }
        });
    },

    getEventsByRevision: function(query, revMin, revMax, callback) {
        const self = this;
        const queryFunc = async function(conn) {
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

            const resultsAndFields = await self._query(
                conn,
                queryString,
                params
            );
            let results = [];
            resultsAndFields.results.forEach((storedEvent) => {
                const event = self._storedEventToEvent(storedEvent);
                results.push(event);
            });
            return results;
        };
        self._executeWithConnection(queryFunc, (err, events) => {
            if (err) {
                if (callback) callback(err, null);
            } else {
                if (callback) {
                    callback(null, events);
                }
            }
        });
    },

    getLastEvent: function(query, callback) {
        const self = this;

        const queryFunc = async function(conn) {
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

            const resultsAndFields = await self._query(
                conn,
                queryString,
                params
            );

            let resultEvent = null;
            if (resultsAndFields.results.length > 0) {
                const storedEvent = resultsAndFields.results[0];
                resultEvent = self._storedEventToEvent(storedEvent);
            }
            return resultEvent;
        };
        self._executeWithConnection(queryFunc, (err, event) => {
            if (err) {
                if (callback) callback(err, null);
            } else {
                if (callback) {
                    callback(null, event);
                }
            }
        });
    },

    getUndispatchedEvents: function(query, callback) {
        const self = this;

        const queryFunc = async function(conn) {
            let queryString = `SELECT * FROM ${self._options.database}.${self.options.undispatchedEventsTableName}`;
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

            queryString = queryString + ` ORDER BY commit_stamp ASC, stream_revision ASC, commit_sequence ASC`;

            const resultsAndFields = await self._query(
                conn,
                queryString,
                params
            );
            let results = [];
            resultsAndFields.results.forEach((storedUndispatchedEvent) => {
                const event = self._storedEventToEvent(storedUndispatchedEvent);
                results.push(event);
            });
            return results;
        };
        self._executeWithConnection(queryFunc, (err, events) => {
            if (err) {
                if (callback) callback(err, null);
            } else {
                if (callback) {
                    callback(null, events);
                }
            }
        });
    },

    setEventToDispatched: function(id, callback) {
        const self = this;

        const queryFunc = async function(conn) {
            await self._query(
                conn,
                `DELETE FROM ${self._options.database}.${self.options.undispatchedEventsTableName} WHERE event_id = ?`,
                [id]
            );
        };

        self._executeWithConnection(queryFunc, (err, result) => {
            if (err) {
                if (callback) callback(err, null);
            } else {
                if (callback) {
                    callback(null, result);
                }
            }
        });
    },

    addSnapshot: function(snapshot, callback) {
        const self = this;

        const queryFunc = async function(conn) {
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
            await self._query(
                conn,
                `INSERT INTO ${self._options.database}.${self.options.snapshotsTableName} SET ?`, [newSnapshot]
            );
        };

        self._executeWithConnection(queryFunc, (err, result) => {
            if (err) {
                if (callback) callback(err, null);
            } else {
                if (callback) {
                    callback(null, result);
                }
            }
        });
    },

    getSnapshot: function(query, revMax, callback) {
        const self = this;

        const queryFunc = async function(conn) {
            const resultsAndFields = await self._query(
                conn,
                `SELECT * FROM ${self._options.database}.${self.options.snapshotsTableName} WHERE aggregate_id = ? AND aggregate = ? AND context = ? ORDER BY commit_stamp DESC LIMIT 1`,
                [`${query.aggregateId}`, query.aggregate || null, query.context || null]
            );
            let resultSnapshot = null;
            if (resultsAndFields.results.length > 0) {
                const storedSnapshot = resultsAndFields.results[0];
                resultSnapshot = self._storedSnapshotToSnapshot(storedSnapshot);
            }

            return resultSnapshot;
        };

        self._executeWithConnection(queryFunc, (err, snapshot) => {
            if (err) {
                console.error('SNAPSHOT ERROR');
                console.error(err);
                if (callback) callback(err, null);
            } else {
                if (callback) {
                    callback(null, snapshot);
                }
            }
        });
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

    _initQuery: async function(query, callback) {
        const self = this;

        const queryFunc = async function(conn) {
            const resultsAndFields = await self._query(
                conn,
                query, []
            );
            return resultsAndFields;
        };
        self._executeWithConnection(queryFunc, callback);
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
    },

    _batchAddEvents: async function(conn, events) {
        const self = this;

        await self._beginTransaction(conn);

        try {
            for (let index = 0; index < events.length; index++) {
                const event = _.cloneDeep(events[index]);
                const promises = [];

                event.commitStamp = new Date(event.commitStamp).getTime();
                const newEvent = {
                    event_id: event.id,
                    context: event.context,
                    payload: JSON.stringify(event.payload),
                    commit_id: event.commitId,
                    position: event.position,
                    stream_id: event.streamId,
                    aggregate: event.aggregate,
                    aggregate_id: event.aggregateId,
                    commit_stamp: event.commitStamp,
                    commit_sequence: event.commitSequence,
                    stream_revision: event.streamRevision,
                    rest_in_commit_stream: event.restInCommitStream
                };

                const eventSet = self._query(conn, `INSERT INTO ${self._options.database}.${self.options.eventsTableName} SET ?`, newEvent);
                promises.push(eventSet);

                if (self._options.usingEventDispatcher) {
                    const undispatchedSet = self._query(conn, `INSERT INTO ${self._options.database}.${self.options.undispatchedEventsTableName} SET ?`, newEvent);
                    promises.push(undispatchedSet);
                }

                await Promise.all(promises);
            }

            await self._commitTransaction(conn);
        } catch (error) {
            try {
                await self._rollbackTransaction(conn);
            } catch (rollbackError) {
                console.error('MySQL_EventStore: Error in attempting to rollback transaction');
                console.error(rollbackError);
            }
            throw error;
        }
    }
});

module.exports = MySqlEventStore;