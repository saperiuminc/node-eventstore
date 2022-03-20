/* eslint-disable require-jsdoc */
const util = require('util');
const PartitionedStore = require('../partitioned-store');
const _ = require('lodash');
const jsondate = require('jsondate');
const debug = require('debug')('eventstore:store:inmemory');
const murmurhash = require('murmurhash');

function InMemory(options, offsetManager) {
  const defaults = {
      eventsTableName: 'events',
      snapshotsTableName: 'snapshots',
      usingEventDispatcher: true,
      eventTypeNameInPayload: 'name',
      partitions: 25
  };

  this.options = Object.assign(defaults, options);
  this.store = {};
  this.snapshots = {};
  this.undispatchedEvents = { _direct: {} };
  if (options.trackPosition) {
    this.position = 0;
  }
  PartitionedStore.call(this, options, offsetManager);
}

util.inherits(InMemory, PartitionedStore);

function deepFind(obj, pattern) {
  let found;

  if (pattern) {
    const parts = pattern.split('.');

    found = obj;
    for (var i in parts) {
      found = found[parts[i]];
      if (_.isArray(found)) {
        found = _.filter(found, function(item) {
          const deepFound = deepFind(item, parts.slice(i + 1).join('.'));
          return !!deepFound;
        });
        break;
      }

      if (!found) {
        break;
      }
    }
  }

  return found;
}

_.extend(InMemory.prototype, {

  connect: function(callback) {
    this.emit('connect');
    if (callback) callback(null, this);
  },

  disconnect: function(callback) {
    this.emit('disconnect');
    if (callback) callback(null);
  },

  clear: function(callback) {
    this.store = {};
    this.snapshots = {};
    this.undispatchedEvents = { _direct: {} };
    this.position = 0;
    if (callback) callback(null);
  },

  getNextPositions: function(positions, callback) {
    if (!this.options.trackPosition) {
      return callback(null);
    }

    const range = [];
    for (let i=0; i<positions; i++) {
      range.push(++this.position);
    }
    callback(null, range);
  },

  addEvents: function(events, callback) {
    if (!events || events.length === 0) {
      callback(null);
      return;
    }

    const found = _.find(events, function(evt) {
      return !evt.aggregateId;
    });

    if (found) {
      const errMsg = 'aggregateId not defined!';
      debug(errMsg);
      if (callback) callback(new Error(errMsg));
      return;
    }

    const aggregateId = events[0].aggregateId;
    const aggregate = events[0].aggregate || '_general';
    const context = events[0].context || '_general';

    this.store[context] = this.store[context] || {};
    this.store[context][aggregate] = this.store[context][aggregate] || {};
    this.store[context][aggregate][aggregateId] = this.store[context][aggregate][aggregateId] || [];
    this.store[context][aggregate][aggregateId] = this.store[context][aggregate][aggregateId].concat(events);

    this.undispatchedEvents[context] = this.undispatchedEvents[context] || {};
    this.undispatchedEvents[context][aggregate] = this.undispatchedEvents[context][aggregate] || {};
    this.undispatchedEvents[context][aggregate][aggregateId] = this.undispatchedEvents[context][aggregate][aggregateId] || [];
    this.undispatchedEvents[context][aggregate][aggregateId] = this.undispatchedEvents[context][aggregate][aggregateId].concat(events);

    const self = this;
    _.forEach(events, function(evt) {
      self.undispatchedEvents._direct[evt.id] = evt;
    });

    callback(null);
  },

  getEvents: function(query, skip, limit, callback) {
    let res = [];
    for (const s in this.store) {
      for (const ss in this.store[s]) {
        for (const sss in this.store[s][ss]) {
          res = res.concat(this.store[s][ss][sss]);
        }
      }
    }

    res = _.sortBy(res, function(e) {
      return e.commitStamp.getTime();
    });

    if (!_.isEmpty(query)) {
      res = _.filter(res, function(e) {
        const keys = _.keys(query);
        const values = _.values(query);
        let found = false;
        for (const i in keys) {
          const key = keys[i];
          const deepFound = deepFind(e, key);
          if (_.isArray(deepFound) && deepFound.length > 0) {
            found = true;
          } else if (deepFound === values[i]) {
            found = true;
          } else {
            found = false;
            break;
          }
        }
        return found;
      });
    }

    if (limit === -1) {
      return callback(null, _.cloneDeep(res.slice(skip)));
    }

    if (res.length <= skip) {
      return callback(null, []);
    }

    callback(null, _.cloneDeep(res.slice(skip, skip + limit)));
  },

  getPartitionId: function(aggregateId) {
      const partitionId = murmurhash(aggregateId, 2) % this.options.partitions;
      return partitionId;
  },

  getPartitions: function() {
      const partitions = [];
      for (let i = 0; i < this.options.partitions; i++) {
          partitions.push(i);
      }
      return partitions;
  },

  getEventsSince: function(date, skip, limit, callback) {
    let res = [];
    for (const s in this.store) {
      for (const ss in this.store[s]) {
        for (const sss in this.store[s][ss]) {
          res = res.concat(this.store[s][ss][sss]);
        }
      }
    }

    res = _.sortBy(res, function(e) {
      return e.commitStamp.getTime();
    });

    res = _.filter(res, function(e) {
      return e.commitStamp.getTime() >= date.getTime();
    });

    if (limit === -1) {
      return callback(null, _.cloneDeep(res.slice(skip)));
    }

    if (res.length <= skip) {
      return callback(null, []);
    }

    callback(null, _.cloneDeep(res.slice(skip, skip + limit)));
  },

  getEventsByRevision: function(query, revMin, revMax, callback) {
    let res = [];

    if (!query.aggregateId) {
      const errMsg = 'aggregateId not defined!';
      debug(errMsg);
      if (callback) callback(new Error(errMsg));
      return;
    }

    if (query.context && query.aggregate) {
      this.store[query.context] = this.store[query.context] || {};
      this.store[query.context][query.aggregate] = this.store[query.context][query.aggregate] || {};

      if (!this.store[query.context][query.aggregate][query.aggregateId]) {
        return callback(null, _.cloneDeep(res));
      } else {
        if (revMax === -1) {
          res = res.concat(this.store[query.context][query.aggregate][query.aggregateId].slice(revMin));
        } else {
          res = res.concat(this.store[query.context][query.aggregate][query.aggregateId].slice(revMin, revMax));
        }
      }
      return callback(null, _.cloneDeep(res));
    }

    if (!query.context && query.aggregate) {
      for (const s in this.store) {
        const c = this.store[s];
        if (c[query.aggregate] && c[query.aggregate][query.aggregateId]) {
          if (revMax === -1) {
            res = res.concat(c[query.aggregate][query.aggregateId].slice(revMin));
          } else {
            res = res.concat(c[query.aggregate][query.aggregateId].slice(revMin, revMax));
          }
        }
      }
      return callback(null, _.cloneDeep(res));
    }

    if (query.context && !query.aggregate) {
      const cc = this.store[query.context] || {};
      for (const ss in cc) {
        const a = cc[ss];
        if (a[query.aggregateId]) {
          if (revMax === -1) {
            res = res.concat(a[query.aggregateId].slice(revMin));
          } else {
            res = res.concat(a[query.aggregateId].slice(revMin, revMax));
          }
        }
      }
      return callback(null, _.cloneDeep(res));
    }

    if (!query.context && !query.aggregate) {
      for (const sc in this.store) {
        const cont = this.store[sc];
        for (const sa in cont) {
          const agg = cont[sa];
          if (agg[query.aggregateId]) {
            if (revMax === -1) {
              res = res.concat(agg[query.aggregateId].slice(revMin));
            } else {
              res = res.concat(agg[query.aggregateId].slice(revMin, revMax));
            }
          }
        }
      }
      return callback(null, _.cloneDeep(res));
    }
  },

  getLastEvent: function(query, callback) {
    if (!query.aggregateId) {
      const errMsg = 'aggregateId not defined!';
      debug(errMsg);
      if (callback) callback(new Error(errMsg));
      return;
    }

    let res = [];
    for (const s in this.store) {
      for (const ss in this.store[s]) {
        for (const sss in this.store[s][ss]) {
          res = res.concat(this.store[s][ss][sss]);
        }
      }
    }

    res = _.sortBy(res, function(e) {
      return e.commitStamp.getTime();
    });

    if (!_.isEmpty(query)) {
      res = _.filter(res, function(e) {
        const keys = _.keys(query);
        const values = _.values(query);
        let found = false;
        for (const i in keys) {
          const key = keys[i];
          const deepFound = deepFind(e, key);
          if (_.isArray(deepFound) && deepFound.length > 0) {
            found = true;
          } else if (deepFound === values[i]) {
            found = true;
          } else {
            found = false;
            break;
          }
        }
        return found;
      });
    }

    callback(null, res[res.length - 1]);
  },

  getUndispatchedEvents: function(query, callback) {
    let res = [];
    for (const s in this.undispatchedEvents) {
      if (s === '_direct') continue;
      for (const ss in this.undispatchedEvents[s]) {
        for (const sss in this.undispatchedEvents[s][ss]) {
          res = res.concat(this.undispatchedEvents[s][ss][sss]);
        }
      }
    }

    res = _.sortBy(res, function(e) {
      return e.commitStamp.getTime();
    });

    if (!_.isEmpty(query)) {
      res = _.filter(res, function(e) {
        const keys = _.keys(query);
        const values = _.values(query);
        let found = false;
        for (const i in keys) {
          const key = keys[i];
          const deepFound = deepFind(e, key);
          if (_.isArray(deepFound) && deepFound.length > 0) {
            found = true;
          } else if (deepFound === values[i]) {
            found = true;
          } else {
            found = false;
            break;
          }
        }
        return found;
      });
    }

    callback(null, res);
  },

  setEventToDispatched: function(id, callback) {
    const evt = this.undispatchedEvents._direct[id];
    const aggregateId = evt.aggregateId;
    const aggregate = evt.aggregate || '_general';
    const context = evt.context || '_general';

    this.undispatchedEvents[context][aggregate][aggregateId] = _.reject(this.undispatchedEvents[context][aggregate][aggregateId], evt);
    delete this.undispatchedEvents._direct[id];
    callback(null);
  },

  addSnapshot: function(snap, callback) {
    const aggregateId = snap.aggregateId;
    const aggregate = snap.aggregate || '_general';
    const context = snap.context || '_general';

    if (!snap.aggregateId) {
      const errMsg = 'aggregateId not defined!';
      debug(errMsg);
      if (callback) callback(new Error(errMsg));
      return;
    }

    this.snapshots[context] = this.snapshots[context] || {};
    this.snapshots[context][aggregate] = this.snapshots[context][aggregate] || {};
    this.snapshots[context][aggregate][aggregateId] = this.snapshots[context][aggregate][aggregateId] || [];

    this.snapshots[context][aggregate][aggregateId].push(snap);
    callback(null);
  },

  getSnapshot: function(query, revMax, callback) {
    if (!query.aggregateId) {
      const errMsg = 'aggregateId not defined!';
      debug(errMsg);
      if (callback) callback(new Error(errMsg));
      return;
    }

    let all = [];
    for (const s in this.snapshots) {
      for (const ss in this.snapshots[s]) {
        for (const sss in this.snapshots[s][ss]) {
          all = all.concat(this.snapshots[s][ss][sss]);
        }
      }
    }

//    all = _.sortBy(all, function (s) {
//      return [(-s.revision), (-s.version)].join('_');
//    });

    all = _.sortBy(all, function(s) {
      return (-s.commitStamp.getTime());
    });

    if (!_.isEmpty(query)) {
      all = _.filter(all, function(a) {
        const keys = _.keys(query);
        const values = _.values(query);
        let found = false;
        for (const i in keys) {
          const key = keys[i];
          const deepFound = deepFind(a, key);
          if (_.isArray(deepFound) && deepFound.length > 0) {
            found = true;
          } else if (deepFound === values[i]) {
            found = true;
          } else {
            found = false;
            break;
          }
        }
        return found;
      });
    }

    if (revMax === -1) {
      return callback(null, all[0] ? jsondate.parse(JSON.stringify(all[0])) : null);
    } else {
      for (let i = all.length - 1; i >= 0; i--) {
        if (all[i].revision <= revMax) {
          return callback(null, jsondate.parse(JSON.stringify(all[i])));
        }
      }
    }
    callback(null, null);
  },

  cleanSnapshots: function(query, callback) {
    const aggregateId = query.aggregateId;
    const aggregate = query.aggregate || '_general';
    const context = query.context || '_general';

    if (!aggregateId) {
      const errMsg = 'aggregateId not defined!';
      debug(errMsg);
      if (callback) callback(new Error(errMsg));
      return;
    }

    let snapshots = this.snapshots[context][aggregate][aggregateId] || [];
    const length = snapshots.length;
    snapshots = snapshots.slice(-1 * this.options.maxSnapshotsCount);
    this.snapshots[context][aggregate][aggregateId] = snapshots;

    callback(null, length - snapshots.length);
  },

  getLatestOffset: function(shard, partition, callback) {
      return null;
  },

  getOffsetManager: function() {
    return {
      getStartingSerializedOffset: (partition) => {
        return null;
      }
    }
  }

});

module.exports = InMemory;
