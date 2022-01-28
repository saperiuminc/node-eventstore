const shortid = require('shortid');
const _ = require('lodash');
const EventEmitter = require('events').EventEmitter,

class PubSubClient extends EventEmitter {
    /**
     * 
     * @param {import("ioredis").Redis} redisPublisherClient 
     * @param {import("ioredis").Redis} redisSubscriberClient 
     */
    constructor(redisPublisherClient, redisSubscriberClient) {
        const self = this;
        this._redisSubscriberClient = redisSubscriberClient;
        this._redisPublisherClient = redisPublisherClient;

        this._topicSubscriptions = {};
    }

    /**
     * 
     * @param {String} topic 
     * @param {String} message 
     */ 
    _onMessage(topic, message) {
        const subscriptions = this._topicSubscriptions[topic];

        if (subscriptions) {
            for (const subscriptionToken in subscriptions) {
                subscriptions[subscriptionToken].callback(null, message);
            }
        }
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

        this._topicSubscriptions[topic][subscriptionToken] = {
            callback: callback
        };

        await this._redisSubscriberClient.subscribe(topic);

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