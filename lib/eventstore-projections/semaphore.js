const _ = require('lodash');
const shortid = require('shortid');

/**
 * Semaphore constructor
 * @class
 * @param {string} key key
 * @param {number} capacity maximum number of concurrent tasks
 * @constructor
 */
function Semaphore(key, capacity) {
    this.key = key;
    this.capacity = capacity;
    this.currentValue = 0;
    this.queue = [];
    this.processingTokens = {};
}

Semaphore.prototype = {
    /**
     * offer an event to the stream buffer
     * @param {Object} event
     */
    acquire: async function () {
        const self = this;
        
        return await new Promise((resolve) => {
            const token = shortid.generate();
            self.queue.push({
                token: token,
                task: resolve
            });
            self.currentValue++;
            if (self.currentValue <= self.capacity && self.queue.length > 0) {
                self._processTasksInQueue();
            }
        });
    },

    _processTasksInQueue: function() {
        const self = this;
        if (self.queue.length > 0) {
            const item = self.queue.shift();
            const token = item.token;
            const task = item.task;
            self.processingTokens[token] = true;
            task(token);
        }
    },

    release: function(token) {
        const self = this;
        if (self.processingTokens[token]) {
            self.currentValue--;
            delete self.processingTokens[token];
            self._processTasksInQueue();
        }
    }
};

module.exports = Semaphore;