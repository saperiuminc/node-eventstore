const pubsub = require('../pubsub');
const Waitable = require('./waitable').Waitable;
const _ = require('lodash');

/**
 * 
 * @param {import('.').WaitableOptions} options 
 * @returns 
 */
const createWaitable = function(options) {
    if (!options.createRedisClient) {
        throw new Error('createRedisClient is required');
    }

    options = options || {};
    var defaults = {};
  
    this._options = Object.assign(defaults, options);

    const pubsubClient = pubsub.createPubSubClient({
        createRedisClient: this._options.createRedisClient
    });


    this._redisSubscriberClient = this._options.createRedisClient('subscriber');
    this._redisPublisherClient = this._options.createRedisClient('client');

    const waitable = new Waitable(pubsubClient);

    return waitable;
};

module.exports.createWaitable = createWaitable;