const shortid = require('shortid');
const _ = require('lodash');

let _listenerClient;
let clients = [];

const onMessageCallback = function (topic, message) {
    for (const c of clients) {
        c._onMessage(topic, message);
    }
}

const _onStartListen = function(client) {
    if (!_listenerClient) {
        _listenerClient = client._redisSubscriberClient;
        _listenerClient.on('message', onMessageCallback);
    }

    if (!clients.includes(client)) {
        clients.push(client);
    }
}

const _onStopListen = function(client) {
    clients = clients.filter((c) => {
        return c !== client;
    });
    if (_listenerClient && clients.length === 0) {
        _listenerClient.off('message', onMessageCallback);
        _listenerClient = null;
    }
}

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

        this._topicSubscriptions = {};
    }

    /**
     * 
     * @param topic 
     * @param message 
     */
    _onMessage(topic, message) {
        const subscriptions = this._topicSubscriptions[topic];

        if (subscriptions) {
            for (const subscrptionToken in subscriptions) {
                subscriptions[subscrptionToken].callback(null, message);
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
        _onStartListen(this);
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

        if (Object.keys(this._topicSubscriptions).length === 0) {
            _onStopListen(this);
        }
    }
}

module.exports.PubSubClient = PubSubClient;