/* eslint-disable require-jsdoc */
const util = require('util');
const Store = require('../base');
const _ = require('lodash');
const mysqlsharedpool = require('@saperiuminc/mysql2-shared-pool');
const debug = require('debug')('eventstore:datastore:mysql');
const EventStoreDuplicateError = require('./errors/EventStoreDuplicateError');

function MySqlEventStore(options) {
    const opts = options || {};

    const defaults = {
        eventsTableName: 'events',
        undispatchedEventsTableName: 'undispatched_events',
        snapshotsTableName: 'snapshots',
        insertEventsSPName: 'insert_events',
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
    Store.call(this, options);
}

util.inherits(MySqlEventStore, Store);

_.extend(MySqlEventStore.prototype, {
    _connect: async function() {
        try {
            debug('connect');
            debug(this._options);

            let maxPool = !isNaN(this._options.connectionPoolLimit) ? parseInt(this._options.connectionPoolLimit) : 1;

            this.pool = await mysqlsharedpool.createPool({
                host: this._options.host,
                user: this._options.user,
                password: this._options.password,
                database: this._options.database,
                multipleStatements: true,
                connectionLimit: maxPool
            });

            await this.pool.query(`CREATE TABLE IF NOT EXISTS \`${this._options.database}\`.\`${this.options.eventsTableName}\` (` +
                    `id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT, ` +
                    `event_id VARCHAR(40) NOT NULL, ` +
                    `aggregate_id VARCHAR(100) NOT NULL, ` +
                    `aggregate VARCHAR(50) NOT NULL, ` +
                    `context VARCHAR(25) NOT NULL, ` +
                    `payload TEXT NOT NULL, ` +
                    `commit_stamp BIGINT UNSIGNED NOT NULL, ` +
                    `stream_revision MEDIUMINT UNSIGNED NOT NULL, ` +
                    `PRIMARY KEY (context, aggregate, aggregate_id, stream_revision), ` +
                    `INDEX idx_get_events (id, context, aggregate)` +
                `)` +
                    `PARTITION BY KEY (context, aggregate)` +
                    `PARTITIONS 10`
            );

            await this.pool.query(`CREATE TABLE IF NOT EXISTS \`${this._options.database}\`.\`${this.options.undispatchedEventsTableName}\` (` +
                    `event_id VARCHAR(40) NOT NULL, ` +
                    `aggregate_id VARCHAR(100) NOT NULL, ` +
                    `aggregate VARCHAR(50) NOT NULL, ` +
                    `context VARCHAR(25) NOT NULL, ` +
                    `payload TEXT NOT NULL, ` +
                    `commit_stamp BIGINT UNSIGNED NOT NULL, ` +
                    `stream_revision MEDIUMINT UNSIGNED NOT NULL, ` +
                    `PRIMARY KEY (event_id)` +
                `)`
            );

            await this.pool.query(`CREATE TABLE IF NOT EXISTS \`${this._options.database}\`.\`${this.options.snapshotsTableName}\` (` +
                    `id VARCHAR(40) NOT NULL, ` +
                    `data TEXT NOT NULL, ` +
                    `context VARCHAR(25) NOT NULL, ` +
                    `revision MEDIUMINT UNSIGNED NOT NULL, ` +
                    `aggregate VARCHAR(50) NOT NULL, ` +
                    `aggregate_id VARCHAR(100) NOT NULL, ` +
                    `commit_stamp BIGINT UNSIGNED NOT NULL, ` +
                    `PRIMARY KEY (id), ` +
                    `INDEX idx_get_snapshot (context, aggregate, aggregate_id, commit_stamp)` +
                `)`
            );
            
            await this.pool.query(`DROP PROCEDURE IF EXISTS \`${this._options.database}\`.\`${this.options.insertEventsSPName}\`;
                CREATE PROCEDURE \`${this._options.database}\`.\`${this.options.insertEventsSPName}\` (
                    IN p_events JSON
                ) BEGIN
                    DECLARE event_id VARCHAR(40);
                    DECLARE context VARCHAR(25);
                    DECLARE payload TEXT;
                    DECLARE aggregate VARCHAR(50);
                    DECLARE aggregate_id VARCHAR(100);
                    DECLARE commit_stamp BIGINT UNSIGNED;
                    DECLARE stream_revision MEDIUMINT UNSIGNED;
                    DECLARE indx SMALLINT;
                    DECLARE event JSON;
                    DECLARE EXIT HANDLER FOR SQLEXCEPTION
                    BEGIN
                        ROLLBACK;
                        RESIGNAL;
                    END;
                    SET indx = 0;

                    START TRANSACTION;
                    WHILE indx < JSON_LENGTH(p_events) DO
                        SET event = JSON_EXTRACT(p_events, CONCAT("$[", indx, "]"));
                        SET event_id = event->>'$.eventId';
                        SET context = event->>'$.context';
                        SET payload = event->>'$.payload';
                        SET aggregate = event->>'$.aggregate';
                        SET aggregate_id = event->>'$.aggregateId';
                        SET commit_stamp = event->>'$.commitStamp';
                        SET stream_revision = event->>'$.streamRevision';
                        
                        INSERT INTO \`${this._options.database}\`.\`${this.options.eventsTableName}\` (event_id, context, payload, aggregate, aggregate_id, commit_stamp, stream_revision)
                        VALUES (event_id, context, payload, aggregate, aggregate_id, commit_stamp, stream_revision);

                        INSERT INTO \`${this._options.database}\`.\`${this.options.undispatchedEventsTableName}\` (event_id, context, payload, aggregate, aggregate_id, commit_stamp, stream_revision)
                        VALUES (event_id, context, payload, aggregate, aggregate_id, commit_stamp, stream_revision);

                        SET indx = indx + 1;
                    END WHILE;
                    COMMIT;
                END;
            `);
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
        return Promise.all([
            this.pool.query(`TRUNCATE TABLE \`${this._options.database}\`.\`${this.options.eventsTableName}\``),
            this.pool.query(`TRUNCATE TABLE \`${this._options.database}\`.\`${this.options.undispatchedEventsTableName}\``),
            this.pool.query(`TRUNCATE TABLE \`${this._options.database}\`.\`${this.options.snapshotsTableName}\``)
        ]);
    },

    clear: function(done) {
        this._clear().then(data => done(null, data)).catch(done);
    },

    _addEvents: async function(events) {
        const self = this;
        if (events && events.length > 0) {
            let newEvents = [];
            try {
                for (let index = 0; index < events.length; index++) {
                    const event = events[index];
    
                    newEvents = newEvents.concat({
                        eventId: event.id,
                        context: event.context,
                        payload: JSON.stringify(event.payload),
                        aggregate: event.aggregate,
                        aggregateId: event.aggregateId,
                        commitStamp: new Date(event.commitStamp).getTime(),
                        streamRevision: event.streamRevision
                    });
                }
    
                await self.pool.execute(`CALL \`${this._options.database}\`.\`${this.options.insertEventsSPName}\`(?);`, [newEvents]);
            } catch (error) {
                if (error.code === 'ER_DUP_ENTRY') {
                    throw new EventStoreDuplicateError('Duplicate Error', error);
                }
                console.error('_addEvents error', error, events, newEvents);
                throw error;
            }
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

        const resultRaw = await self.pool.execute(queryString, params);
        const rows = resultRaw[0];

        let results = [];
        rows.forEach((storedEvent) => {
            const logicalEvent = self._storedEventToEvent(storedEvent, query);
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

        let queryString = `SELECT * FROM ${self._options.database}.${self._options.eventsTableName} WHERE commit_stamp >= ? ORDER BY commit_stamp ASC, stream_revision ASC LIMIT ? OFFSET ?`;
        const params = [date || 0, (!limit || isNaN(limit) ? 0 : limit), (!skip || isNaN(skip) ? 0 : skip)];

        const resultRaw = await self.pool.execute(
            queryString,
            params
        );
        const rows = resultRaw[0];

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
            let queryString = `SELECT event_id, payload, stream_revision, commit_stamp FROM ${self._options.database}.${self.options.eventsTableName}`;
            const params = [];

            if (query && query.context) {
                queryString = queryString + ` WHERE context = ?`
                params.push(query.context);
            }
            if (query && query.aggregate) {
                if (params.length > 0) {
                    queryString = queryString + ` AND `
                } else {
                    queryString = queryString + ` WHERE `
                }
                queryString = queryString + `aggregate = ?`
                params.push(query.aggregate);
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

            queryString = queryString + ` ORDER BY stream_revision ASC`;

            const resultRaw = await self.pool.execute(queryString, params);
            const rows = resultRaw[0];

            let results = [];
            rows.forEach((storedEvent) => {
                const event = self._storedEventToEvent(storedEvent, query);
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

            let queryString = `SELECT event_id, payload, stream_revision, commit_stamp FROM ${self._options.database}.${self.options.eventsTableName}`;
            const params = [];
            if (query && query.context) {
                queryString = queryString + ` WHERE context = ?`
                params.push(query.context);
            }
            if (query && query.aggregate) {
                if (params.length > 0) {
                    queryString = queryString + ` AND `
                } else {
                    queryString = queryString + ` WHERE `
                }
                queryString = queryString + `aggregate = ?`
                params.push(query.aggregate);
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

            queryString = queryString + ` ORDER BY stream_revision DESC LIMIT 1`;

            const resultRaw = await this.pool.execute(queryString, params);
            const rows = resultRaw[0];

            let resultEvent = null;
            if (rows.length > 0) {
                const storedEvent = rows[0];
                resultEvent = this._storedEventToEvent(storedEvent, query);
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

        let queryString = `SELECT * FROM ${self._options.database}.${self.options.undispatchedEventsTableName}`;
        const params = [];

        if (query && query.context) {
            if (params.length > 0) {
                queryString = queryString + ` AND `
            } else {
                queryString = queryString + ` WHERE `
            }
            queryString = queryString + `context = ?`
            params.push(query.context);
        }
        if (query && query.aggregate) {
            if (params.length > 0) {
                queryString = queryString + ` AND `
            } else {
                queryString = queryString + ` WHERE `
            }
            queryString = queryString + `aggregate = ?`
            params.push(query.aggregate);
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

        queryString = queryString + ` ORDER BY stream_revision ASC`;

        const resultRaw = await self.pool.execute(
            queryString,
            params
        );
        const rows = resultRaw[0];
        let results = [];
        rows.forEach((storedUndispatchedEvent) => {
            const event = self._storedEventToEvent(storedUndispatchedEvent, query);
            results.push(event);
        });
        return results;

    },
    getUndispatchedEvents: function(query, callback) {
        this._getUndispatchedEvents(query).then((data) => callback(null, data)).catch(callback);
    },

    _setEventToDispatched: async function(id) {
        const self = this;
        await self.pool.execute(
            `DELETE FROM ${self._options.database}.${self.options.undispatchedEventsTableName} WHERE event_id = ?`,
            [id]
        );
    },
    setEventToDispatched: function(id, callback) {
        this._setEventToDispatched(id).then((data) => callback(null, data)).catch(callback);
    },

    _addSnapshot: async function(snapshot) {
        const self = this;

        await self.pool.execute(
            `INSERT INTO  
                ${self._options.database}.${self.options.snapshotsTableName}
            (
                id, 
                data, 
                context, 
                revision,
                aggregate, 
                aggregate_id,
                commit_stamp
            ) VALUES (?,?,?,?,?,?,?);`,
            [
                snapshot.id,
                JSON.stringify(snapshot.data),
                snapshot.context,
                snapshot.revision,
                snapshot.aggregate,
                snapshot.aggregateId,
                new Date(snapshot.commitStamp).getTime()
            ]
        );
    },
    addSnapshot: function(snapshot, callback) {
        this._addSnapshot(snapshot).then((data) => callback(null, data)).catch(callback);
    },

    _getSnapshot: async function(query, revMax) {
        const self = this;

        const resultRaw = await self.pool.execute(
            `SELECT id, data, revision, commit_stamp FROM ${self._options.database}.${self.options.snapshotsTableName} WHERE aggregate_id = ? AND aggregate = ? AND context = ? ORDER BY commit_stamp DESC LIMIT 1`,
            [`${query.aggregateId}`, query.aggregate || null, query.context || null]
        );

        const rows = resultRaw[0];
        let resultSnapshot = null;
        if (rows.length > 0) {
            const storedSnapshot = rows[0];
            resultSnapshot = self._storedSnapshotToSnapshot(storedSnapshot, query);
        }

        return resultSnapshot;
    },
    getSnapshot: function(query, revMax, callback) {
        this._getSnapshot(query, revMax).then(data => callback(null, data)).catch(callback);
    },

    /* Private Methods */
    _storedSnapshotToSnapshot: function(storedSnapshot, query) {
        const logicalSnapshot = {
            id: storedSnapshot.id,
            data: JSON.parse(storedSnapshot.data),
            context: query && query.context ? query.context: storedSnapshot.context,
            revision: storedSnapshot.revision,
            aggregate: query && query.aggregate ? query.aggregate : storedSnapshot.aggregate,
            aggregateId: query && query.aggregateId ? query.aggregateId : storedSnapshot.aggregate_id,
            commitStamp: new Date(storedSnapshot.commit_stamp)
        }

        return logicalSnapshot;
    },

    _storedEventToEvent: function(storedEvent, query) {
        const logicalEvent = {
            id: storedEvent.event_id,
            eventSequence: storedEvent.id,
            context: query && query.context ? query.context: storedEvent.context,
            payload: JSON.parse(storedEvent.payload),
            aggregate: query && query.aggregate ? query.aggregate : storedEvent.aggregate,
            aggregateId: query && query.aggregateId ? query.aggregateId : storedEvent.aggregate_id,
            commitStamp: new Date(storedEvent.commit_stamp),
            streamRevision: storedEvent.stream_revision
        }

        return logicalEvent;
    }
});

module.exports = MySqlEventStore;