/* eslint-disable require-jsdoc */
const util = require('util');
const Store = require('../../base');
const _ = require('lodash');
const debug = require('debug')('eventstore:datastore:mysql');
const QueryCombiner = require('../joiners/query-combiner');
const ClusteredEventStore = require('../clusteredstore');
const MySqlEventStore = require('../mysql')
const Bluebird = require('bluebird');
const helpers = require('../../helpers');

function ClusteredMySqlEventStore(options) {
    this._options = options;
    this._stores = [];
    
    if (!this._options.clusters || !Array.isArray(this._options.clusters)) {
        throw new Error('missing or invalid options.clusters')
    }
    
    if (!this._options.partitions) {
        this._options.partitions = 25;
    }
    
    this._sortColumns = ['commit_stamp', 'stream_revision', 'event_id'];
    Store.call(this, options);
}

util.inherits(ClusteredMySqlEventStore, ClusteredEventStore);

_.extend(ClusteredMySqlEventStore.prototype, {
    getStores: function() {
        return this._stores;
    },
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
                const mysqlStore = new MySqlEventStore(config);
                Bluebird.promisifyAll(mysqlStore);
                await mysqlStore.connectAsync();
                this._stores.push(mysqlStore);
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
    
    clear: function(done) {
        const promises = [];
        this._stores.forEach((store) => {
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
            const shard = helpers.getShard(aggregateId, self._stores.length);
            await self._stores[shard].addEventsAsync(events);
        }
    },
    addEvents: function(events, callback) {
        this._addEvents(events).then((data) => callback(null, data)).catch(callback);
    },
    
    getPartitions: function(type) {
        const partitions = [];
        if (type === 'multi') {
            for (let i = 0; i < this._options.partitions; i++) {
                partitions.push(i);
            }
            return partitions;
        }
        return partitions;
    },
    
    _getEvents: async function(query, skips, limit) {
        const self = this;
        
        let shard = null;
        if (!Array.isArray(query) && !_.isNil(query.shard)) {
            shard = query.shard;
        } else if (Array.isArray(query) && query.length > 0 && !_.isNil(query[0].shard)) {
            shard = query[0].shard;
        } else if (!Array.isArray(query) && !_.isNil(query.aggregateId)) {
            shard = helpers.getShard(query.aggregateId, self._stores.length);
        }

        let offsets;
        if (skips) {
            offsets = helpers.deserializeProjectionOffset(skips);
        } else if (!skips && !_.isNil(shard) && shard >= 0) {
            offsets = this._stores[shard].getStartingOffset(null);
        } else if (!skips && (_.isNil(shard) || shard < 0)) {
            offsets = [];
            self._stores.forEach((store, index) => {
                offsets.push(store[index].getStartingOffset(null));
            })
        }
        
        if (!_.isNil(shard) && shard >= 0) {
            const offset = Array.isArray(offsets) ? offsets[shard] : offsets;
            const events = await self._stores[shard].getEventsAsync(query, offset, limit);
            let finalOffsets = _.clone(offsets);
            const finalResults = events.map((event) => {
                const objEventSequence = _.clone(event.eventSequence);
                if (Array.isArray(offsets)) {
                    finalOffsets[shard] = objEventSequence 
                } else {
                    finalOffsets = objEventSequence;
                }
                event.eventSequence = helpers.serializeProjectionOffset(finalOffsets);
                return event;
            });
            return finalResults;
        } else {
            const queryCombiner = new QueryCombiner({
                readableSortFieldNames: this._getSorting('asc')
            });
            let storeIndex = 0;
            for (const store of self._stores) {
                const offset =  Array.isArray(offsets) ? offsets[storeIndex] : offsets;
                queryCombiner.addRowResults(await store.getEventsAsync(query, offset, limit));
                storeIndex++
            }
            
            const results = await queryCombiner.getJoinedQueryRows(limit);
            let finalOffsets = _.clone(offsets);
            const finalResults = results.map((event) => {
                const shard = helpers.getShard(event.aggregateId, self._stores.length);
                const objEventSequence = _.clone(event.eventSequence);
                if (Array.isArray(offsets)) {
                    finalOffsets[shard] = objEventSequence 
                } else {
                    finalOffsets = objEventSequence;
                }
                event.eventSequence = Array.isArray(finalOffsets) ? helpers.serializeProjectionOffset(finalOffsets) : finalOffsets;
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
            offsets = helpers.deserializeProjectionOffset(skips);
        }
        
        const queryCombiner = new QueryCombiner({
            readableSortFieldNames: self._getSorting('asc')
        });
        let storeIndex = 0;
        for (const store of self._stores) {
            const offset = Array.isArray(offsets) ? offsets[storeIndex] : offsets;
            queryCombiner.addRowResults(await store.getEventsSinceAsync(date, offset, limit));
            storeIndex++
        }
        
        const results = await queryCombiner.getJoinedQueryRows(limit);
        let finalOffsets = _.clone(offsets);
        const finalResults = results.map((event) => {
            const shard = helpers.getShard(event.aggregateId, self._stores.length);
            const objEventSequence = _.clone(event.eventSequence);
            if (Array.isArray(offsets)) {
                finalOffsets[shard] = objEventSequence; 
            } else {
                finalOffsets = objEventSequence;
            }
            event.eventSequence = helpers.serializeProjectionOffset(finalOffsets);
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
            shard = helpers.getShard(query.aggregateId, self._stores.length);
        }
        
        if (!_.isNil(shard) && shard >= 0) {
            const events = await self._stores[shard].getEventsByRevisionAsync(query, revMin, revMax);
            return events;
        }
        const queryCombiner = new QueryCombiner({
            readableSortFieldNames: self._getSorting('asc')
        });
        
        for (const store of self._stores) {
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
            shard = helpers.getShard(query.aggregateId, self._stores.length);
        }
        if (!_.isNil(shard) && shard >= 0) {
            const lastEvent = await self._stores[shard].getLastEventAsync(query);
            return lastEvent;
        }
        const queryCombiner = new QueryCombiner({
            readableSortFieldNames: self._getSorting('desc')
        });
        
        for (const store of self._stores) {
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
        const shard = helpers.getShard(snapshot.aggregateId, self._stores.length);
        await self._stores[shard].addSnapshotAsync(snapshot);
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
            shard = helpers.getShard(query.aggregateId, self._stores.length);
        }
        if (!_.isNil(shard) && shard >= 0) {
            const latestSnapshot = await self._stores[shard].getSnapshotAsync(query, revMax);
            return latestSnapshot;
        }
        const queryCombiner = new QueryCombiner({
            readableSortFieldNames: self._getSorting('asc')
        });
        
        for (const store of self._stores) {
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
    getStartingOffset(partition) {
        if (partition === '__unpartitioned') { // single concurrency
            const finalOffsetArray = [];
            for (const store of this._stores) {
              finalOffsetArray.push(store.getStartingOffset(partition));
            }
            return helpers.serializeProjectionOffset(finalOffsetArray);
        } else {
            return helpers.serializeProjectionOffset(this._stores[0].getStartingOffset(partition));
        }
    },
    
    _getLatestOffset: async function(shard, partition) {
        const self = this;
        if (!_.isNil(shard) && !_.isNil(partition)) {
            return helpers.serializeProjectionOffset(await self._stores[shard].getLatestOffsetAsync(shard, partition));
        } else {
            const latestOffsets = [];
            for (const store of self._stores) {
                // this returns serialized offset
                const storeLatestOffset = await store.getLatestOffsetAsync(null, null);
                latestOffsets.push(storeLatestOffset);
            }
            
            return helpers.serializeProjectionOffset(latestOffsets);
        }
    },
    getLatestOffset: function(shard, partition, callback) {
        this._getLatestOffset(shard, partition).then(data => callback(null, data)).catch(callback);
    }
});

module.exports = ClusteredMySqlEventStore;
