const Redis = require('ioredis');
const PubSubClient = require('./pubsub-client').PubSubClient;
const _ = require('lodash');
let _redisSubscriberClient;
let _redisPublisherClient;
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
    
    const _options = Object.assign(_.clone(defaults), options);

    if (!_redisSubscriberClient) {
        _redisSubscriberClient = _options.createRedisClient('subscriber');
    }
    if (!_redisPublisherClient) {
        _redisPublisherClient = _options.createRedisClient('client');
    }
    const client = new PubSubClient(_redisPublisherClient, _redisSubscriberClient);

    return client;
};

module.exports.createPubSubClient = createPubSubClient;