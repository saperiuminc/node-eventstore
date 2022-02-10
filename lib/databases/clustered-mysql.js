/* eslint-disable require-jsdoc */
const util = require('util');
const Store = require('../base');
const _ = require('lodash');
const debug = require('debug')('eventstore:datastore:mysql');
const debugPP = require('debug')('eventstore:datastore:mysqlpp');
const StreamQueryAdapter = require('./streams/stream-query-adapter');
const MySqlEventStore = require('./mysql')
const Bluebird = require('bluebird');
const helpers = require('../helpers');

function ClusteredMySqlEventStore(options) {
    this._options = options;
    this._stores = [];
    Store.call(this, options);
}

util.inherits(ClusteredMySqlEventStore, MySqlEventStore);

_.extend(ClusteredMySqlEventStore.prototype, {
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
    addEvents: function(events, callback) {
      throw new Error('addEvents not supported');
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
      const self = this;
      const readableStreams = [];
      self._stores.forEach((store, index) => {
        const offset = skips[index];
        readableStreams.push(store.streamEvents(query, offset, limit));
      });
      const streamQueryAdapter = new StreamQueryAdapter({
        readableSortFieldNames:  [{fieldName: 'commit_stamp', sort: 'asc'}, {fieldName: 'id', sort: 'asc'}],
        readableStreams: readableStreams
      });

      const results = await streamQueryAdapter.getRowsFromReadableAsync(limit, (row) => {
        self._storedEventToEvent(row, query);
      })
      return results;
    },
    _streamEvents: async function(query, skips, limit) {
      const self = this;
      // skip is an array offsets
      if (!Array.isArray(skips)) {
        throw new Error('skips must be an array!');
      }
      const readableStreams = [];
      self._stores.forEach((store, index) => {
        const offset = skips[index];
        readableStreams.push(store.streamEvents(query, offset, limit));
      });
      const streamQueryAdapter = new StreamQueryAdapter({
        readableSortFieldNames:  [{fieldName: 'commit_stamp', sort: 'asc'}, {fieldName: 'id', sort: 'asc'}],
        readableStreams: readableStreams
      });
      return streamQueryAdapter.getJoinedReadableStreams();
    },

    getEvents: function(query, skip, limit, callback) {
      this._getEvents(query, skip, limit).then(data => {
        callback(null, data);
      }).catch(callback);
    },

    streamEvents: function(query, skip, limit) {
      return this._streamEvents(query, skip, limit)
    },
    getEventsSince: function(date, skip, limit, callback) {
      throw new Error('getEventsSince is not supported')
    },

    getEventsByRevision: function(query, revMin, revMax, callback) {
      throw new Error('getEventsByRevision is not supported')
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
      const readableStreams = [];
      for (const store of self._stores) {
        readableStreams.push(store.streamLastEvent(query));
      }
      const streamQueryAdapter = new StreamQueryAdapter({
        readableSortFieldNames: [{fieldName: 'commit_stamp', sort: 'desc'}, {fieldName: 'id', sort: 'desc'}],
        readableStreams: readableStreams
      });

      const results = await streamQueryAdapter.getRowsFromReadableAsync(null, (row) => {
        self._storedEventToEvent(row, query);
      })
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

    addSnapshot: function(snapshot, callback) {
      throw new Error('addSnapshot is not supported')
    },

    getSnapshot: function(query, revMax, callback) {
        throw new Error('getSnapshot not supported');
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
    },

    // Private Events
    _storedEventToEvent: function(storedEvent, query) {
      const logicalEvent = {
          id: storedEvent.event_id,
          eventSequence: storedEvent.id,
          context: query && query.context ? query.context : storedEvent.context,
          payload: JSON.parse(storedEvent.payload),
          aggregate: query && query.aggregate ? query.aggregate : storedEvent.aggregate,
          aggregateId: query && query.aggregateId ? query.aggregateId : storedEvent.aggregate_id,
          commitStamp: new Date(storedEvent.commit_stamp),
          streamRevision: storedEvent.stream_revision
      }

      return logicalEvent;
    },
  
    // _streamifyGetQuery: function(getQueryName, ...args) {
    //   const self = this;
    //   const queryBuilder = self['_' + getQueryName + 'QueryBuilder'](...args);
    //   const queryString = queryBuilder.queryString;
    //   const params = queryBuilder.params;
    
    //   const stream = self.pool.query({ sql: queryString, values: params }).stream();
    //   return stream;
    // }
});

module.exports = ClusteredMySqlEventStore;
