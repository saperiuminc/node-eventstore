/* eslint-disable require-jsdoc */
const util = require('util');
const _ = require('lodash');
const debug = require('debug')('eventstore:datastore:mysql');
const QueryCombiner = require('../joiners/query-combiner');
const Bluebird = require('bluebird');
const helpers = require('../../helpers');
const PartitionedStore = require('../partitioned-store');
const MysqlPartitionedStore = require('./mysql-partitioned-store');
const StreamBufferedStore = require('./stream-buffered/stream-buffered-partitioned-store');

function ClusteredPartitionedStore(options, offsetManager) {
    this._options = options;
    this._partitionedStores = [];
    
    if (!this._options.clusters || !Array.isArray(this._options.clusters)) {
        throw new Error('missing or invalid options.clusters')
    }
    
    if (!this._options.partitions) {
        this._options.partitions = 25;
    }
    
    this._sortColumns = ['commit_stamp', 'stream_revision', 'event_id'];
    // TODO: Feb24: ClusteredStore should have its own OffsetManager to be able to properly process Single-concurrent offsets
    PartitionedStore.call(this, options, offsetManager);
}

util.inherits(ClusteredPartitionedStore, PartitionedStore);

_.extend(ClusteredPartitionedStore.prototype, {
    _getSorting: function(type) {
        const sortColumnsForShards = [];
        this._sortColumns.forEach((columnName) => {
            sortColumnsForShards.push({
                fieldName: columnName,
                sort: type
            });
        });
        
        return sortColumnsForShards;
    },
    _connect: async function() {
        try {
            debug('connect', this._options);
            for (const storeConfig of this._options.clusters) {
                const config = _.cloneDeep(storeConfig);
                if (!config.partitions) {
                    config.partitions = this._options.partitions;
                }
                // TODO: Feb24: Get Specific Store? Factory Method
                // Include offsetmanager in factory
                const mysqlStore = new MysqlPartitionedStore(config, this.offsetManager);

                if (this._options.enableProjectionEventStreamBuffer) {
                    const streamBufferedStore = new StreamBufferedStore({}, this.offsetManager, mysqlStore);
                    Bluebird.promisifyAll(streamBufferedStore);
                    await streamBufferedStore.connectAsync();
                    this._partitionedStores.push(streamBufferedStore);
                } else {
                    Bluebird.promisifyAll(mysqlStore);
                    await mysqlStore.connectAsync();
                    this._partitionedStores.push(mysqlStore);
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
        // TODO: Feb24: Call Disconnect on all partitionedStores
        const self = this;
        self.emit('disconnect');
        if (callback) callback(null);
    },
    
    clear: function(done) {
        const promises = [];
        this._partitionedStores.forEach((store) => {
            promises.push(store.clearAsync())
        });
        Promise.all(promises).then(data => done(null, data)).catch(done);
    },
    /*
    *  Adds all events to the database.
    *  Events added should be atomic: Either all or none are added.
    *  This query is automatically rolled back should this fail.
    */
    _addEvents: async function(events) {
        const self = this;
        const aggregateId = (events && events.length > 0 ? events[0] : {}).aggregateId;
        if (aggregateId) {
            const shard = helpers.getShard(aggregateId, self._partitionedStores.length);
            await self._partitionedStores[shard].addEventsAsync(events);
        }
    },
    addEvents: function(events, callback) {
        this._addEvents(events).then((data) => callback(null, data)).catch(callback);
    },
    getPartitionId: function(aggregateId) {
        const partitionId = this._partitionedStores[0].getPartitionId(aggregateId)
        return partitionId;
    },
    getPartitions: function() {
        return [];
    },
    
    _getEvents: async function(query, skips, limit) {
        const self = this;
        
        let shard = null;
        if (!Array.isArray(query) && !_.isNil(query.shard)) {
            shard = query.shard;
        } else if (Array.isArray(query) && query.length > 0 && !_.isNil(query[0].shard)) {
            shard = query[0].shard;
        } else if (!Array.isArray(query) && !_.isNil(query.aggregateId)) {
            shard = helpers.getShard(query.aggregateId, self._partitionedStores.length);
        }
        let offsets;
        if (skips) {
            offsets = self.getOffsetManager().deserializeOffset(skips);
        } else if (!skips && !_.isNil(shard) && shard >= 0) {
            offsets = self.getOffsetManager().getStartingOffset();
        } else if (!skips && (_.isNil(shard) || shard < 0)) {
            offsets = [];
            self._partitionedStores.forEach((store) => {
                offsets.push(self.getOffsetManager().getStartingOffset());
            })
        }
        
        if (!_.isNil(shard) && shard >= 0) {
            const offset = Array.isArray(offsets) ? offsets[shard] : offsets;
            const events = await self._partitionedStores[shard].getEventsAsync(query, offset, limit);
            let finalOffsets = _.clone(offsets);
            const finalResults = events.map((event) => {
                const objEventSequence = _.clone(event.eventSequence);
                if (Array.isArray(offsets)) {
                    finalOffsets[shard] = objEventSequence 
                } else {
                    finalOffsets = objEventSequence;
                }
                event.eventSequence = self.getOffsetManager().serializeOffset(finalOffsets);
                return event;
            });
            return finalResults;
        } else {
            const queryCombiner = new QueryCombiner({
                readableSortFieldNames: this._getSorting('asc')
            });
            let storeIndex = 0;
            for (const store of self._partitionedStores) {
                const offset =  Array.isArray(offsets) ? offsets[storeIndex] : offsets;
                queryCombiner.addRowResults(await store.getEventsAsync(query, offset, limit));
                storeIndex++
            }
            
            const results = await queryCombiner.getJoinedQueryRows(limit);
            let finalOffsets = _.clone(offsets);
            const finalResults = results.map((event) => {
                const shard = helpers.getShard(event.aggregateId, self._partitionedStores.length);
                const objEventSequence = _.clone(event.eventSequence);
                if (Array.isArray(offsets)) {
                    finalOffsets[shard] = objEventSequence 
                } else {
                    finalOffsets = objEventSequence;
                }
                event.eventSequence = Array.isArray(finalOffsets) ? self.getOffsetManager().serializeOffset(finalOffsets) : finalOffsets;
                return event;
            });
            return finalResults;
        }
    },
    
    getEvents: function(query, skip, limit, callback) {
        this._getEvents(query, skip, limit).then(data => {
            callback(null, data);
        }).catch(callback);
    },
    
    _getEventsSince: async function(date, skips, limit) {
        const self = this;
        // could be an array or an int
        let offsets;
        if (skips) {
            offsets = self.getOffsetManager().deserializeOffset(skips);
        }
        
        const queryCombiner = new QueryCombiner({
            readableSortFieldNames: self._getSorting('asc')
        });
        let storeIndex = 0;
        for (const store of self._partitionedStores) {
            const offset = Array.isArray(offsets) ? offsets[storeIndex] : offsets;
            queryCombiner.addRowResults(await store.getEventsSinceAsync(date, offset, limit));
            storeIndex++
        }
        
        const results = await queryCombiner.getJoinedQueryRows(limit);
        let finalOffsets = _.clone(offsets);
        const finalResults = results.map((event) => {
            const shard = helpers.getShard(event.aggregateId, self._partitionedStores.length);
            const objEventSequence = _.clone(event.eventSequence);
            if (Array.isArray(offsets)) {
                finalOffsets[shard] = objEventSequence; 
            } else {
                finalOffsets = objEventSequence;
            }
            event.eventSequence = self.getOffsetManager().serializeOffset(finalOffsets);
            return event;
        });
        return finalResults;
    },
    
    getEventsSince: function(date, skip, limit, callback) {
        this._getEvents(date, skip, limit).then(data => {
            callback(null, data);
        }).catch(callback);
    },
    
    _getEventsByRevision: async function(query, revMin, revMax) {
        const self = this;
        let shard = null;
        if (!Array.isArray(query) && !_.isNil(query.shard)) {
            shard = query.shard;
        } else if (Array.isArray(query) && query.length > 0 && !_.isNil(query[0].shard)) {
            shard = query[0].shard;
        } else if (!Array.isArray(query) && !_.isNil(query.aggregateId)) {
            shard = helpers.getShard(query.aggregateId, self._partitionedStores.length);
        }
        
        if (!_.isNil(shard) && shard >= 0) {
            const events = await self._partitionedStores[shard].getEventsByRevisionAsync(query, revMin, revMax);
            return events;
        }
        const queryCombiner = new QueryCombiner({
            readableSortFieldNames: self._getSorting('asc')
        });
        
        for (const store of self._partitionedStores) {
            queryCombiner.addRowResults(await store.getEventsByRevisionAsync(query, revMin, revMax));
        }
        
        const results = await queryCombiner.getJoinedQueryRows(null);
        
        return results;
    },
    
    getEventsByRevision: function(query, revMin, revMax, callback) {
        this._getEventsByRevision(query, revMin, revMax).then((data) => callback(null, data)).catch(callback);
    },
    
    _getLastEvent: async function(query) {
        const self = this;
        let shard = null;
        if (!Array.isArray(query) && !_.isNil(query.shard)) {
            shard = query.shard;
        } else if (Array.isArray(query) && query.length > 0 && !_.isNil(query[0].shard)) {
            shard = query[0].shard;
        } else if (!Array.isArray(query) && !_.isNil(query.aggregateId)) {
            shard = helpers.getShard(query.aggregateId, self._partitionedStores.length);
        }
        if (!_.isNil(shard) && shard >= 0) {
            const lastEvent = await self._partitionedStores[shard].getLastEventAsync(query);
            return lastEvent;
        }
        const queryCombiner = new QueryCombiner({
            readableSortFieldNames: self._getSorting('desc')
        });
        
        for (const store of self._partitionedStores) {
            queryCombiner.addRowResults(await store.getLastEventAsync(query));
        }
        
        const results = await queryCombiner.getJoinedQueryRows(null)
        return results;
    },
    
    getLastEvent: function(query, callback) {
        this._getLastEvent(query).then((data) => callback(null, data)).catch(callback);
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
        const shard = helpers.getShard(snapshot.aggregateId, self._partitionedStores.length);
        await self._partitionedStores[shard].addSnapshotAsync(snapshot);
    },
    
    addSnapshot: function(snapshot, callback) {
        this._addSnapshot(snapshot).then(data => {
            callback(null, data);
        }).catch(callback);
    },
    
    _getSnapshot: async function(query, revMax) {
        const self = this;
        let shard = null;
        if (!Array.isArray(query) && !_.isNil(query.shard)) {
            shard = query.shard;
        } else if (Array.isArray(query) && query.length > 0 && !_.isNil(query[0].shard)) {
            shard = query[0].shard;
        } else if (!Array.isArray(query) && !_.isNil(query.aggregateId)) {
            shard = helpers.getShard(query.aggregateId, self._partitionedStores.length);
        }
        if (!_.isNil(shard) && shard >= 0) {
            const latestSnapshot = await self._partitionedStores[shard].getSnapshotAsync(query, revMax);
            return latestSnapshot;
        }
        const queryCombiner = new QueryCombiner({
            readableSortFieldNames: self._getSorting('asc')
        });
        
        for (const store of self._partitionedStores) {
            queryCombiner.addRowResults(await store.getSnapshotAsync(query, revMax));
        }
        
        const results = await queryCombiner.getJoinedQueryRows(null)
        return results;
    },
    getSnapshot: function(query, revMax, callback) {
        this._getSnapshot(query, revMax).then(data => {
            callback(null, data);
        }).catch(callback);
    },

    _getLatestOffset: async function(shard, partition) {
        const self = this;
        // TODO: REVIEW: no more array offsets, just one offset?
        if (!_.isNil(shard) && !_.isNil(partition)) {
            return self.getOffsetManager().serializeOffset(await self._partitionedStores[shard].getLatestOffsetAsync(shard, partition));
        } else {
            const latestOffsets = [];
            for (const store of self._partitionedStores) {
                // this returns serialized offset
                const storeLatestOffset = await store.getLatestOffsetAsync(null, null);
                latestOffsets.push(storeLatestOffset);
            }
            
            return self.getOffsetManager().serializeOffset(latestOffsets);
        }
    },
    getLatestOffset: function(shard, partition, callback) {
        this._getLatestOffset(shard, partition).then(data => callback(null, data)).catch(callback);
    }
});

module.exports = ClusteredPartitionedStore;
