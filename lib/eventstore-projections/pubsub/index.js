const Redis = require('ioredis');
const PubSubClient = require('./pubsub-client').PubSubClient;
const _ = require('lodash');

/**
 * 
 * @param {import('.').PubSubOptions} options 
 * @returns 
 */
const createPubSubClient = function(options) {
    if (!options.createRedisClient) {
        throw new Error('createRedisClient is required');
    }

    options = options || {};
    var defaults = {};
  
    this._options = _.defaults(options, defaults);

    this._redisSubscriberClient = this._options.createRedisClient('subscriber');
    this._redisPublisherClient = this._options.createRedisClient('client');

    const client = new PubSubClient(this._redisPublisherClient, this._redisSubscriberClient);

    return client;
};

module.exports.createPubSubClient = createPubSubClient;