/* eslint-disable require-jsdoc */
const util = require('util');
const PartitionedStore = require('../partitioned-store');
const _ = require('lodash');
const murmurhash = require('murmurhash');
const mysqlsharedpool = require('@saperiuminc/mysql2-shared-pool');
const debug = require('debug')('eventstore:datastore:mysql');
const debugPP = require('debug')('eventstore:datastore:mysqlpp');
const EventStoreDuplicateError = require('../errors/EventStoreDuplicateError');

function MySqlEventStore(options, offsetManager) {
    let defaults = {
        eventsTableName: 'events',
        snapshotsTableName: 'snapshots',
        insertEventsSPName: 'insert_eventstore_events',
        host: '127.0.0.1',
        port: 3306,
        user: 'root',
        password: 'root',
        database: 'eventstore',
        connectionPoolLimit: 1,
        usingEventDispatcher: true,
        eventTypeNameInPayload: 'name',
        partitions: 25
    };
    
    this._options = this.options = Object.assign(defaults, options);
    PartitionedStore.call(this, options, offsetManager);
}

util.inherits(MySqlEventStore, PartitionedStore);

_.extend(MySqlEventStore.prototype, {
    _connect: async function() {
        try {
            debug('connect');
            debug(this._options);
            
            let maxPool = !isNaN(this._options.connectionPoolLimit) ? parseInt(this._options.connectionPoolLimit) : 1;
            this.pool = await mysqlsharedpool.createPool({
                host: this._options.host,
                port: this._options.port,
                user: this._options.user,
                password: this._options.password,
                database: this._options.database,
                multipleStatements: true,
                connectionLimit: maxPool
            });
            
            let createTableQuery = `CREATE TABLE IF NOT EXISTS \`${this._options.eventsTableName}\` (` +
                `id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT, ` +
                `event_id VARCHAR(40) NOT NULL, ` +
                `aggregate_id VARCHAR(125) NOT NULL, ` +
                `aggregate VARCHAR(75) NOT NULL, ` +
                `context VARCHAR(25) NOT NULL, ` +
                `payload MEDIUMTEXT NOT NULL, ` +
                `commit_stamp BIGINT UNSIGNED NOT NULL, ` +
                `stream_revision MEDIUMINT UNSIGNED NOT NULL, ` +
                `partition_id SMALLINT UNSIGNED NOT NULL, ` +
                `PRIMARY KEY (context, aggregate, aggregate_id, stream_revision, partition_id), ` +
                `INDEX idx_get_events (id, context, aggregate, commit_stamp, stream_revision, event_id)` +
            `)` +
            `PARTITION BY LIST (partition_id) (`;
            
            for (let i = 0; i < this._options.partitions; i++) {
                createTableQuery += `PARTITION ${this._getPartitionKey(i)} VALUES IN(${i})${i !== (this._options.partitions - 1) ? ', ': ''}`;
            }
            
            createTableQuery += ')';
            
            
            await this.pool.query(createTableQuery);
            
            await this.pool.query(`CREATE TABLE IF NOT EXISTS \`${this._options.snapshotsTableName}\` (` +
                `id VARCHAR(40) NOT NULL, ` +
                `data MEDIUMTEXT NOT NULL, ` +
                `context VARCHAR(25) NOT NULL, ` +
                `revision MEDIUMINT UNSIGNED NOT NULL, ` +
                `aggregate VARCHAR(75) NOT NULL, ` +
                `aggregate_id VARCHAR(125) NOT NULL, ` +
                `commit_stamp BIGINT UNSIGNED NOT NULL, ` +
                `PRIMARY KEY (id), ` +
                `INDEX idx_get_snapshot (context, aggregate, aggregate_id, commit_stamp)` +
            `)`
            );
            
            try {
                // TODO: find a better way to not create the sproc if it already exists
                await this.pool.query(`
                    CREATE PROCEDURE ${this._options.insertEventsSPName} (
                        IN p_events JSON
                    ) BEGIN
                        DECLARE event_id VARCHAR(40);
                        DECLARE context VARCHAR(25);
                        DECLARE payload MEDIUMTEXT;
                        DECLARE aggregate VARCHAR(75);
                        DECLARE aggregate_id VARCHAR(125);
                        DECLARE commit_stamp BIGINT UNSIGNED;
                        DECLARE stream_revision MEDIUMINT UNSIGNED;
                        DECLARE partition_id SMALLINT UNSIGNED;
                        DECLARE indx SMALLINT;
                        DECLARE event JSON;
                        DECLARE EXIT HANDLER FOR SQLEXCEPTION
                        BEGIN
                        ROLLBACK;
                        RESIGNAL;
                        END;
                        SET indx = 0;
                        
                        SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
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
                        SET partition_id = event->>'$.partitionId';
                        
                        INSERT INTO ${this._options.eventsTableName} (event_id, context, payload, aggregate, aggregate_id, commit_stamp, stream_revision, partition_id)
                        VALUES (event_id, context, payload, aggregate, aggregate_id, commit_stamp, stream_revision, partition_id);
                        
                        SET indx = indx + 1;
                        END WHILE;
                        COMMIT;
                    END;
                `);
            } catch (error) {
                if (error.code == 'ER_SP_ALREADY_EXISTS') {
                    // TODO: find a better way to check if sp already exists. known error.
                } else {
                    throw error;
                }
            }
        } catch (error) {
            console.error('error in mysql.connect', error);
            throw error;
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
        try {
            await this.pool.query(`TRUNCATE TABLE \`${this._options.database}\`.\`${this._options.eventsTableName}\``);
        } catch (error) {
            console.info('Cannot truncate events table');
        }
        try {
            await this.pool.query(`TRUNCATE TABLE \`${this._options.database}\`.\`${this._options.snapshotsTableName}\``);
        } catch (error) {
            console.info('Cannot truncate snapshots table');
        }
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
                        streamRevision: event.streamRevision,
                        partitionId: this.getPartitionId(event.aggregateId)
                    });
                }
                
                await self.pool.execute(`CALL \`${this._options.database}\`.\`${this._options.insertEventsSPName}\`(?);`, [newEvents]);
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
    
    getPartitionId: function(aggregateId) {
        const partitionId = murmurhash(aggregateId, 2) % this._options.partitions;
        return partitionId;
    },
    
    _getPartitionKey: function(partitionId) {
        return `p${partitionId}`;
    },
    
    getPartitions: function() {
        const partitions = [];
        for (let i = 0; i < this._options.partitions; i++) {
            partitions.push(i);
        }
        return partitions;
    },
    
    _getEvents: async function(query, skip, limit) {
        const self = this;
        const queryBuilder = self._getEventsQueryBuilder(query, skip, limit);
        const queryString = queryBuilder.queryString;
        const params = queryBuilder.params;
        
        const resultRaw = await self.pool.execute(queryString, params);
        const rows = resultRaw[0];
        
        if(rows.length > 0) {
            debugPP('GET EVENTS QUERY', queryString, params, rows);
        }
        
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
    const queryBuilder = self._getEventsSinceQueryBuilder(date, skip, limit);
    const queryString = queryBuilder.queryString;
    const params = queryBuilder.params;
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
            const queryBuilder = self._getEventsByRevisionQueryBuilder(query, revMin, revMax);
            const queryString = queryBuilder.queryString;
            const params = queryBuilder.params;
            
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
            const queryBuilder = self._getLastEventQueryBuilder(query);
            const queryString = queryBuilder.queryString;
            const params = queryBuilder.params;
            
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
        return [];
    },
    getUndispatchedEvents: function(query, callback) {
        this._getUndispatchedEvents(query).then((data) => callback(null, data)).catch(callback);
    },
    
    setEventToDispatched: function(id, callback) {
        // No Undispatched Table Implementation
        callback(null, null);
    },
    
    _addSnapshot: async function(snapshot) {
        const self = this;
        
        await self.pool.execute(
            `INSERT INTO  
            ${self._options.database}.${self._options.snapshotsTableName}
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
        const queryBuilder = self._getSnapshotQueryBuilder(query, revMax);
        const queryString = queryBuilder.queryString;
        const params = queryBuilder.params;
        
        const resultRaw = await self.pool.execute(
        queryString,
        params
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
    _getLatestOffset: async function(shard, partition) {
        const self = this;
        
        const partitionClause = _.isNil(partition) ? '' : `PARTITION (${self._getPartitionKey(partition)})`;
        const resultRaw = await self.pool.execute(`SELECT commit_stamp, stream_revision, event_id FROM ${self._options.database}.${self._options.eventsTableName} ${partitionClause} ORDER BY id DESC LIMIT 1`, []);
        
        const rows = resultRaw[0];
        let latestOffset = {
            commitStamp: 0,
            streamRevision: -1,
            eventId: '',
        };
        if (rows.length > 0) {
            const row = rows[0];
            latestOffset = {
                commitStamp: row.commit_stamp,
                streamRevision: row.stream_revision,
                eventId: row.event_id
            };
        }
        
        return self.getOffsetManager().serializeOffset(latestOffset);
    },
    getLatestOffset: function(shard, partition, callback) {
        this._getLatestOffset(shard, partition).then(data => callback(null, data)).catch(callback);
    },
    
    /* Private Methods */
    _storedSnapshotToSnapshot: function(storedSnapshot, query) {
        const logicalSnapshot = {
            id: storedSnapshot.id,
            data: JSON.parse(storedSnapshot.data),
            context: query && query.context ? query.context : storedSnapshot.context,
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
            eventSequence: this.getOffsetManager().serializeOffset({
                commitStamp: storedEvent.commit_stamp,
                streamRevision: storedEvent.stream_revision,
                eventId: storedEvent.event_id
            }),
            context: query && query.context ? query.context : storedEvent.context,
            payload: JSON.parse(storedEvent.payload),
            aggregate: query && query.aggregate ? query.aggregate : storedEvent.aggregate,
            aggregateId: query && query.aggregateId ? query.aggregateId : storedEvent.aggregate_id,
            commitStamp: new Date(storedEvent.commit_stamp),
            streamRevision: storedEvent.stream_revision
        }
        
        return logicalEvent;
    },
    _getEventsQueryBuilder: function(query, skip, limit) {
        const self = this;
        const bookmark = self.getOffsetManager().deserializeOffset(skip);
        if (!bookmark || _.isNil(bookmark.commitStamp) || _.isNil(bookmark.streamRevision) || _.isNil(bookmark.eventId)) {
            throw new Error('Missing or invalid: bookmark');
        }
        const appendInnerQueryElseWhere = function(innerQueryString, length, predicate, elsePredicate) {
            if (length > 0) {
                innerQueryString = innerQueryString + predicate;
            } else {
                innerQueryString = innerQueryString + (elsePredicate ? elsePredicate : '');
            }
            return innerQueryString;
        };
        
        // select * from eventstore_domain.events PARTITION (p1) limit 100;
        // Inner query - perform filtering, sorting, and limit here. But only retrieve the PK
        let partitionClause = '';
        
        if (!Array.isArray(query)) {
            if(query.partition != undefined) {
                partitionClause = `PARTITION (${self._getPartitionKey(query.partition)})`;
            }
        } else {
            if(query[0] != undefined && query[0].partition != undefined) {
                partitionClause = `PARTITION (${self._getPartitionKey(query[0].partition)})`;
            }
        }
        
        let queryString = `SELECT * FROM ${self._options.eventsTableName} ${partitionClause} `;
        const params = [];
        let queryArray = [];
        
        if (!Array.isArray(query)) {
            queryArray.push(query);
        } else {
            queryArray = query;
        }
        
        if (queryArray.length > 0) {
            queryString = appendInnerQueryElseWhere(queryString, params.length, ` AND `, ` WHERE `);
            queryString = queryString + ' ( ';
            let counter = 0;
            _.forEach(queryArray, function(q) {
                queryString = appendInnerQueryElseWhere(queryString, counter, ` OR `);
                
                queryString = queryString + ` ( `
                
                if (q && q.context) {
                    queryString = queryString + `context = ?`;
                    params.push(q.context);
                }
                
                if (q && q.aggregate) {
                    queryString = appendInnerQueryElseWhere(queryString, params.length, ` AND `);
                    queryString = queryString + `aggregate = ?`;
                    params.push(q.aggregate);
                }
                
                if (q && q.aggregateId) {
                    queryString = appendInnerQueryElseWhere(queryString, params.length, ` AND `);
                    queryString = queryString + `aggregate_id = ?`;
                    params.push(`${q.aggregateId}`);
                }
                queryString = queryString + ` ) `;
                counter += 1;
            });
            queryString = queryString + ' ) ';
        }
        
        
        queryString = appendInnerQueryElseWhere(queryString, params.length, ` AND `, ` WHERE `);
        queryString += `(`;
        queryString += `(commit_stamp > ?) OR `;
        params.push(bookmark.commitStamp);
        queryString += `(commit_stamp = ? AND stream_revision > ?) OR `;
        params.push(bookmark.commitStamp);
        params.push(bookmark.streamRevision);
        queryString += `(commit_stamp = ? AND stream_revision = ? AND event_id > ?) `;
        params.push(bookmark.commitStamp);
        params.push(bookmark.streamRevision);
        params.push(bookmark.eventId);
        queryString += `)`;
        queryString = queryString + ` ORDER BY commit_stamp ASC, stream_revision ASC, event_id ASC LIMIT ?`
        params.push((!limit || isNaN(limit) ? 0 : limit));
        
        // Enclose with outer query string for late row lookup of the event json
        
        return {
            queryString: queryString,
            params: params
        }
    },
    _getEventsSinceQueryBuilder: function(date, skip, limit) {
        const self = this;
        let queryString = `SELECT * FROM ${self._options.database}.${self._options.eventsTableName} WHERE commit_stamp >= ? ORDER BY commit_stamp ASC, stream_revision ASC LIMIT ? OFFSET ?`;
        const params = [date || 0, (!limit || isNaN(limit) ? 0 : limit), (!skip || isNaN(skip) ? 0 : skip)];
        
        return {
            queryString: queryString,
            params: params
        }
    },
    _getEventsByRevisionQueryBuilder: function(query, revMin, revMax) {
        const self = this;
        let queryString = `SELECT event_id, payload, stream_revision, commit_stamp FROM ${self._options.database}.${self._options.eventsTableName}`;
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
        return {
            queryString: queryString,
            params: params
        }
    },
    _getLastEventQueryBuilder: function(query) {
        const self = this;
        let queryString = `SELECT event_id, payload, stream_revision, commit_stamp FROM ${self._options.database}.${self._options.eventsTableName}`;
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
        
        return {
            queryString: queryString,
            params: params
        }
    },
    _getSnapshotQueryBuilder: function(query, revMax) {
        const self = this;
        return {
            queryString: `SELECT id, data, revision, commit_stamp FROM ${self._options.database}.${self._options.snapshotsTableName} WHERE aggregate_id = ? AND aggregate = ? AND context = ? ORDER BY commit_stamp DESC LIMIT 1`,
            params: [`${query.aggregateId}`, query.aggregate || null, query.context || null]
        }
    }
});
        
module.exports = MySqlEventStore;
