/**
 * EventSubscriptionQueryManager constructor
 * @class
 * @constructor
 */
function EventSubscriptionQueryManager(options) {
    this.es = options.es;
    this._activeChannels = {};
}

EventSubscriptionQueryManager.prototype = {
    getEventStream: async function(query, revMin, revMax) {
        throw new Error('not implemented');
    },

    getLastEvent: async function(query) {
        throw new Error('not implemented');
    },

    clear: function() {
        throw new Error('not implemented');
    },

    setChannelActive: function(channel, isActive) {
        throw new Error('not implemented');
    },

    isChannelActive: function(channel) {
        throw new Error('not implemented');
    }
}

module.exports = EventSubscriptionQueryManager;