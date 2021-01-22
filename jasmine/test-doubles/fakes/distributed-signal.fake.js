const shortid = require('shortid');
const _ = require('lodash');

function WaitableFake(topic) {
    this.id = shortid.generate();
    this.topic = topic;

    this._waitList = [];
};

WaitableFake.prototype = {

    waitOnce: async function(callback) { 
        this._waitList.push(callback);
    },

    waitRecursive: async function(callback) {
        // do recursive waitMultiple calls after done
        const self = this;
        this.waitOnce(function(err, done) {
            callback(err, function(err) {
                done(err);
                self.waitRecursive(callback);
            });
        })
    },

    getTime: function() {
        return this._time;
    },

    setTime: function(time) {
        this._time = time;
    },

    dequeue: function() {
        return this._waitList.shift();
    }
}


function DistributedSignalFake() {
    this._groupWaitables = {};
    this._waitables = {};
};

DistributedSignalFake.prototype = {

    waitForSignal: async function(topic, group) { 
        const waitable = new WaitableFake(topic);
        if (!group) {
            group = 'default';
        }

        if (!this._groupWaitables[group]) {
            this._groupWaitables[group] = {};
            this._groupWaitables[group][topic] = [];
        }
        this._groupWaitables[group][topic].push(waitable);

        return waitable;
    },

    signal: async function(topic, group) {
        if (!group) {
            group = 'default';
        }

        if (this._groupWaitables[group]) {
            _.each(this._groupWaitables[group][topic], (waitable) => {
                const callback = waitable.dequeue();
                if (callback) {
                    callback(null, function() {
                        
                    });
                    return false;
                }
            });
        }
    },

    stopWaitingForSignal: async function(waitableId, group) {
        if (!group) {
            group = 'default';
        }

        if (this._groupWaitables[group]) {
            const topicsHash = this._groupWaitables[group];
            _.forOwn(topicsHash, (waitables, topic) => {
                _.remove(waitables, (waitable) => {
                    return waitable.id == waitableId;
                });
            })
            
        }
    },

    _sleep: async function(timeout) {
        return new Promise((resolve) => {
            setTimeout(resolve, timeout);
        })
    }
}

module.exports = DistributedSignalFake;