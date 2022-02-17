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

    this._sortColumns = ['commit_stamp', 'id'];
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
        })
      });

      return sortColumnsForShards;
    },
    _connect: async function() {
        try {
            debug('connect');
            debug(this._options);
            for (const storeConfig of this._options.clusters) {
              const mysqlStore = new MySqlEventStore(storeConfig);
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
        this._addEvents(events).then(data => callback(null, data)).catch(callback);
    },

    getPartition: function(aggregateId, callback) {
      callback();
    },

    getPartitions: function() {
      return [];
    },

    _getEvents: async function(query, skips, limit) {
      // skip is an array offsets
      if (!Array.isArray(skips)) {
        throw new Error('skips must be an array!');
      }
      const queryCombiner = new QueryCombiner({
        readableSortFieldNames: this._getSorting('asc')
      });
      const self = this;
      let storeIndex = 0;
      for (const store of self._stores) {
        const offset = skips[storeIndex];
        queryCombiner.addRowResults(await store.getEventsAsync(query, offset, limit));
        storeIndex++
      }

      const results = await queryCombiner.getJoinedQueryRows(limit)
      return results;
    },

    getEvents: function(query, skip, limit, callback) {
      this._getEvents(query, skip, limit).then(data => {
        callback(null, data);
      }).catch(callback);
    },

    _getEventsSince: async function(date, skips, limit) {
      const self = this;
      // skip is an array offsets
      if (!Array.isArray(skips)) {
        throw new Error('skips must be an array!');
      }
      const queryCombiner = new QueryCombiner({
        readableSortFieldNames: self._getSorting('asc')
      });
      let storeIndex = 0;
      for (const store of self._stores) {
        const offset = skips[storeIndex];
        queryCombiner.addRowResults(await store.getEventsSinceAsync(date, offset, limit));
        storeIndex++
      }

      const results = await queryCombiner.getJoinedQueryRows(limit)
      return results;
    },

    getEventsSince: function(date, skip, limit, callback) {
      this._getEvents(date, skip, limit).then(data => {
        callback(null, data);
      }).catch(callback);
    },

    _getEventsByRevision: async function(query, revMin, revMax) {
      const self = this;
      let shard = null;
      if (query.aggregateId) {
        shard = helpers.getShard(query.aggregateId, self._stores.length);
      }
      if (shard || shard === 0) {
        const events = await self._stores[shard].getEventsByRevisionAsync(query, revMin, revMax);
        return events;
      }
      const queryCombiner = new QueryCombiner({
        readableSortFieldNames: self._getSorting('asc')
      });
      
      for (const store of self._stores) {
        queryCombiner.addRowResults(await store.getEventsByRevisionAsync(query, revMin, revMax));
      }

      const results = await queryCombiner.getJoinedQueryRows(null)
      return results;
    },

    getEventsByRevision: function(query, revMin, revMax, callback) {
      this._getEventsByRevision(query, revMin, revMax).then((data) => callback(null, data)).catch(callback);
    },

    _getLastEvent: async function(query) {
      const self = this;
      let shard = null;
      if (query.aggregateId) {
        shard = helpers.getShard(query.aggregateId, self._stores.length);
      }
      if (shard || shard === 0) {
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
      if (query.aggregateId) {
        shard = helpers.getShard(query.aggregateId, self._stores.length);
      }
      if (shard || shard === 0) {
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

    _getLatestOffset: async function() {
        const self = this;
        const latestOffsets = [];
        for (const store of self._stores) {
          const storeLatestOffset = await store.getLatestOffsetAsync(null);
          latestOffsets.push(storeLatestOffset || 0);
        }
        
        return latestOffsets;
    },
    getLatestOffset: function(partition, callback) {
        this._getLatestOffset().then(data => callback(null, data)).catch(callback);
    }
});

module.exports = ClusteredMySqlEventStore;
