const PubSubClient = require('./pubsub-client').PubSubClient;
const _ = require('lodash');

const pubsubClients = [];
/**
 * 
 * @param {import('.').PubSubOptions} options 
 * @returns 
 */
const createPubSubClient = function(options) {
    if (!options.createRedisClient) {
        throw new Error('createRedisClient is required');
    }

    const redisSubscriberClient = options.createRedisClient('subscriber');

    const onSubcriberMessage = function(topic, message) {
        for (const pubsubClient of pubsubClients) {
            pubsubClient._onMessage(topic, message);
        }
    };

    redisSubscriberClient.off('message', onSubcriberMessage);
    redisSubscriberClient.on('message', onSubcriberMessage);

    const redisPublisherClient = options.createRedisClient('client');

    const client = new PubSubClient(redisPublisherClient, redisSubscriberClient);

    pubsubClients.push(client);

    return client;
};

module.exports.createPubSubClient = createPubSubClient;