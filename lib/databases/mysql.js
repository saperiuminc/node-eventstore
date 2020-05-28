/* eslint-disable require-jsdoc */
const util = require('util');
const Store = require('../base');
const EventStoreDuplicateError = require('./errors/EventStoreDuplicateError');
const _ = require('lodash');
const mysql = require('mysql');

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
        usingEventDispatcher: true
    };

    this._options = this.options = _.defaults(opts, defaults);
    this._mysql = mysql;
    Store.call(this, options);
}

util.inherits(MySqlEventStore, Store);

_.extend(MySqlEventStore.prototype, {
    connect: function(callback) {
        const self = this;

        console.log('connect');
        console.log(self._options);

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
                console.log('MySqlEventStore: Connection Pool Established. Limit: ' + self._options.connectionPoolLimit);
                return resolve();
            } catch (error) {
                console.log('MySqlEventStore: Connection Pool Failed to Establish:');
                console.log(error);
                return reject(error);
            }
        }).then(() => {
            // Create Events Table if not exists
            console.log('MySqlEventStore: Setting up Events Table');
            self._initQuery(
                `CREATE TABLE IF NOT EXISTS ${self._options.database}.${self.options.eventsTableName} (` +
                `id VARCHAR(250) NOT NULL, ` +
                `event JSON NOT NULL, ` +
                `aggregate_id VARCHAR(250) GENERATED ALWAYS AS (json_unquote(json_extract(event,'$.aggregateId'))) VIRTUAL, ` +
                `aggregate VARCHAR(45) GENERATED ALWAYS AS (json_unquote(json_extract(event,'$.aggregate'))) VIRTUAL, ` +
                `context VARCHAR(45) GENERATED ALWAYS AS (json_unquote(json_extract(event,'$.context'))) VIRTUAL, ` +
                `stream_revision INT(11) GENERATED ALWAYS AS (json_unquote(json_extract(event,'$.streamRevision'))) VIRTUAL, ` +
                `commit_stamp BIGINT(20) GENERATED ALWAYS AS (json_unquote(json_extract(event,'$.commitStamp'))) VIRTUAL, ` +
                `PRIMARY KEY (id), ` + 
                `INDEX idx_get_events_aggregate_id_first (aggregate_id, aggregate, context, stream_revision, commit_stamp),` +
                `INDEX idx_get_events_aggregate_first (aggregate, context, aggregate_id, stream_revision, commit_stamp),` +
                `INDEX idx_get_events_context_first (context, aggregate, aggregate_id, stream_revision, commit_stamp),` +
                `INDEX idx_get_events_commit_stamp (commit_stamp)` +
                `)`, (err) => {
                    if (err) {
                        return Promise.reject(err);
                    } else {
                        return Promise.resolve();
                    }
                }
            );
        }).then(() => {
            // Create Unsidpatched Events Table if not exists
            console.log('MySqlEventStore: Done Setting up Events Table');
            console.log('MySqlEventStore: Setting up Undispatched Events Table');
            self._initQuery(
                `CREATE TABLE IF NOT EXISTS ${self._options.database}.${self.options.undispatchedEventsTableName} (` +
                `id VARCHAR(250) NOT NULL, ` +
                `event JSON NOT NULL, ` +
                `aggregate_id VARCHAR(250) GENERATED ALWAYS AS (json_unquote(json_extract(event,'$.aggregateId'))) VIRTUAL, ` +
                `aggregate VARCHAR(45) GENERATED ALWAYS AS (json_unquote(json_extract(event,'$.aggregate'))) VIRTUAL, ` +
                `context VARCHAR(45) GENERATED ALWAYS AS (json_unquote(json_extract(event,'$.context'))) VIRTUAL, ` +
                `commit_stamp BIGINT(20) GENERATED ALWAYS AS (json_unquote(json_extract(event,'$.commitStamp'))) VIRTUAL, ` +
                `PRIMARY KEY (id), ` + 
                `INDEX idx_get_events_aggregate_id_first (aggregate_id, context, aggregate, commit_stamp),` +
                `INDEX idx_get_events_aggregate_first (aggregate, context, aggregate_id, commit_stamp),` +
                `INDEX idx_get_events_context_first (context, aggregate, aggregate_id, commit_stamp),` +
                `INDEX idx_get_events_commit_stamp (commit_stamp)` +
                `)`, (err) => {
                    if (err) {
                        return Promise.reject(err);
                    } else {
                        return Promise.resolve();
                    }
                }
            );
        }).then(() => {
            // Create Snapshots Table if not exists
            self._initQuery(
                `CREATE TABLE IF NOT EXISTS ${self._options.database}.${self.options.snapshotsTableName} (` +
                `id VARCHAR(250) NOT NULL, ` +
                `snapshot JSON NOT NULL, ` +
                `aggregate_id VARCHAR(250) GENERATED ALWAYS AS (json_unquote(json_extract(snapshot,'$.aggregateId'))) VIRTUAL, ` +
                `aggregate VARCHAR(45) GENERATED ALWAYS AS (json_unquote(json_extract(snapshot,'$.aggregate'))) VIRTUAL, ` +
                `context VARCHAR(45) GENERATED ALWAYS AS (json_unquote(json_extract(snapshot,'$.context'))) VIRTUAL, ` +
                `commit_stamp BIGINT(20) GENERATED ALWAYS AS (json_unquote(json_extract(snapshot,'$.commitStamp'))) VIRTUAL, ` +
                `PRIMARY KEY (id), ` + 
                `INDEX idx_get_snapshot (context, aggregate, aggregate_id, commit_stamp)` +
                `)`, (err) => {
                    if (err) {
                        return Promise.reject(err);
                    } else {
                        return Promise.resolve();
                    }
                }
            );
        }).then(() => {
            console.log('MySqlEventStore: Done Setting up Snapshots Table');
            console.log('MySqlEventStore: Successfully Created Tables');
            self.emit('connect');
            if (callback) callback(null, self);
        }).catch((error) => {
            console.log('MySqlEventStore: Failed to Create Tables:');
            console.log(error);
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
                `TRUNCATE TABLE ${self._options.database}.${self.options.eventsTableName} `, (err) => {
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
                `TRUNCATE TABLE ${self._options.database}.${self.options.undispatchedEventsTableName} `, (err) => {
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
                `TRUNCATE TABLE ${self._options.database}.${self.options.snapshotsTableName} `, (err) => {
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
                console.log('MySqlEventStore: Successfully Cleared Tables:');
                done(null, self);
            }).catch((err) => {
                console.log('MySqlEventStore: Failed to Clear Tables:');
                console.log(err);
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
            let queryString = `SELECT id, event FROM ${self._options.database}.${self.options.eventsTableName}`;
            const params = [];

            if (query && query.aggregate) {
                if (params.length > 0) {
                    queryString = queryString + ` AND `
                } else {
                    queryString = queryString + ` WHERE `
                }
                queryString = queryString + `aggregate = ?`
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

            queryString = queryString + ` ORDER BY commit_stamp ASC LIMIT ? OFFSET ?`
            params.push((!limit || isNaN(limit) ? 0 : limit));
            params.push((!skip || isNaN(skip) ? 0 : skip));

            const resultsAndFields = await self._query(
                conn,
                queryString,
                params
            );
            let results = [];
            resultsAndFields.results.forEach((storedEvent) => {
                const event = JSON.parse(storedEvent.event);
                event.commitStamp = new Date(event.commitStamp);
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

    getEventsSince: function(date, skip, limit, callback) {
        const self = this;

        const queryFunc = async function(conn) {
            let queryString = `SELECT id, event FROM ${self._options.database}.${self._options.eventsTableName}  WHERE commit_stamp >= ? ORDER BY commit_stamp ASC LIMIT ? OFFSET ?`;
            const params = [date, (!limit || isNaN(limit) ? 0 : limit), (!skip || isNaN(skip) ? 0 : skip)];

            const resultsAndFields = await self._query(
                conn,
                queryString,
                params
            );
            let results = [];
            resultsAndFields.results.forEach((storedEvent) => {
                const event = JSON.parse(storedEvent.event);
                event.commitStamp = new Date(event.commitStamp);
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
            let queryString = `SELECT id, event FROM ${self._options.database}.${self.options.eventsTableName}`;
            const params = [];

            if (query && query.aggregate) {
                if (params.length > 0) {
                    queryString = queryString + ` AND `
                } else {
                    queryString = queryString + ` WHERE `
                }
                queryString = queryString + `aggregate = ?`
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

            if (revMin !== undefined) {
                if (params.length > 0) {
                    queryString = queryString + ` AND `
                } else {
                    queryString = queryString + ` WHERE `
                }
                queryString = queryString + 'stream_revision >= ?';
                params.push(revMin);
            }

            if (revMax !== undefined && revMax !== -1) {
                if (params.length > 0) {
                    queryString = queryString + ` AND `
                } else {
                    queryString = queryString + ` WHERE `
                }
                queryString = queryString + 'stream_revision < ?';
                params.push(revMax);
            }

            queryString = queryString + ` ORDER BY commit_stamp ASC`;

            const resultsAndFields = await self._query(
                conn,
                queryString,
                params
            );
            let results = [];
            resultsAndFields.results.forEach((storedEvent) => {
                const event = JSON.parse(storedEvent.event);
                event.commitStamp = new Date(event.commitStamp);
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

    getUndispatchedEvents: function(query, callback) {
        const self = this;

        const queryFunc = async function(conn) {
            let queryString = `SELECT id, event FROM ${self._options.database}.${self.options.undispatchedEventsTableName}`;
            const params = [];

            if (query && query.aggregate) {
                if (params.length > 0) {
                    queryString = queryString + ` AND `
                } else {
                    queryString = queryString + ` WHERE `
                }
                queryString = queryString + `aggregate = ?`
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

            queryString = queryString + ` ORDER BY commit_stamp ASC`;

            const resultsAndFields = await self._query(
                conn,
                queryString,
                params
            );
            let results = [];
            resultsAndFields.results.forEach((storedUndispatchedEvent) => {
                const event = JSON.parse(storedUndispatchedEvent.event);
                event.commitStamp = new Date(event.commitStamp);
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

        callback(null, []);
    },

    getLastEvent: function(query, callback) {
        const self = this;

        const queryFunc = async function(conn) {
            let queryString = `SELECT id, event FROM ${self._options.database}.${self.options.eventsTableName}`;
            const params = [];
            if (query && query.aggregate) {
                if (params.length > 0) {
                    queryString = queryString + ` AND `
                } else {
                    queryString = queryString + ` WHERE `
                }
                queryString = queryString + `aggregate = ?`
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

            queryString = queryString + ` ORDER BY commit_stamp DESC LIMIT 1`;

            const resultsAndFields = await self._query(
                conn,
                queryString,
                params
            );


            let resultEvent = null;
            if (resultsAndFields.results.length > 0) {
                const storedEvent = resultsAndFields.results[0];
                resultEvent = JSON.parse(storedEvent.event);
                resultEvent.commitStamp = new Date(resultEvent.commitStamp);
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

    setEventToDispatched: function(id, callback) {
        const self = this;

        const queryFunc = async function(conn) {
            await self._query(
                conn,
                `DELETE FROM ${self._options.database}.${self.options.undispatchedEventsTableName} WHERE id = ?`,
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
            snapshot.commitStamp = new Date(snapshot.commitStamp).getTime();
            const newSnapshot = {
                id: snapshot.id,
                snapshot: JSON.stringify(snapshot)
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
                `SELECT id, snapshot FROM ${self._options.database}.${self.options.snapshotsTableName} WHERE aggregate_id = ? AND aggregate = ? AND context = ? ORDER BY commit_stamp DESC LIMIT 1`, [`${query.aggregateId}`, query.aggregate, query.context]
            );
            let resultSnapshot = null;
            if (resultsAndFields.results.length > 0) {
                const storedSnapshot = resultsAndFields.results[0];
                resultSnapshot = JSON.parse(storedSnapshot.snapshot);
                resultSnapshot.commitStamp = new Date(resultSnapshot.commit_stamp);
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
            return Promise.reject(error);
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
            callback(caughtError)
        } else {
            callback(null, results);
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
                const event = events[index];
                const promises = [];

                event.commitStamp = new Date(event.commitStamp).getTime();
                const newEvent = {
                    id: event.id,
                    event: JSON.stringify(event)
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