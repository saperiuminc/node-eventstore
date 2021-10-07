const shortid = require('shortid');
const _ = require('lodash');

class PubSubClient {
    /**
     * 
     * @param {import("ioredis").Redis} redisPublisherClient 
     * @param {import("ioredis").Redis} redisSubscriberClient 
     */
    constructor(redisPublisherClient, redisSubscriberClient) {
        const self = this;
        this._redisSubscriberClient = redisSubscriberClient;
        this._redisPublisherClient = redisPublisherClient;

        this._redisSubscriberClient.on('message', function(topic, message) {
            const subscriptions = self._topicSubscriptions[topic];

            if (subscriptions) {
                for (const subscrptionToken in subscriptions) {
                    subscriptions[subscrptionToken].callback(null, message);
                }
            }
        });

        this._topicSubscriptions = {};
    }

    /**
     * 
     * @param {string} topic 
     * @param {string} message 
     */
    async publish(topic, message) {
        await this._redisPublisherClient.publish(topic, message);
    }

    /**
     * 
     * @param {string} topic 
     * @param {import('./pubsub-client').SubscribeCallback} callback 
     * @returns 
     */
    async subscribe(topic, callback) {
        const subscriptionToken = shortid.generate();

        if (!this._topicSubscriptions[topic]) {
            this._topicSubscriptions[topic] = {};
        }

        await this._redisSubscriberClient.subscribe(topic);

        this._topicSubscriptions[topic][subscriptionToken] = {
            callback: callback
        };

        return subscriptionToken;
    }

    /**
     * 
     * @param {string} subscriptionToken 
     */
    async unsubscribe(subscriptionToken) {
        for (const topic in this._topicSubscriptions) {
            const subscriptions = this._topicSubscriptions[topic];

            if (subscriptions[subscriptionToken]) {
                delete subscriptions[subscriptionToken];
                if (Object.keys(subscriptions).length == 0) {
                    await this._redisSubscriberClient.unsubscribe(topic);
                    delete this._topicSubscriptions[topic];
                }
            }
        }
    }
}

module.exports.PubSubClient = PubSubClient;