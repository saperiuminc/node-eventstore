const _ = require('lodash');

class Waitable {
    /**
     * 
     * @param {import("../pubsub/pubsub-client").PubSubClient} pubsubClient 
     */
    constructor(pubsubClient) {
        this._pubsubClient = pubsubClient;
    }

    /**
     * 
     * @param {string} topic 
     * @param {number} timeout 
     */
    async waitForSignal(topic, timeout) {
        const self = this;
        const timeoutTask = this._sleep(timeout);

        const subscribeTask = new Promise(async (resolve, reject) => {
            const subscriptionToken = await self._pubsubClient.subscribe(topic, async function(error, message) {
                await self._pubsubClient.unsubscribe(subscriptionToken);
                if (error) {
                    reject(new Error('error in subscribe'));
                } else {
                    resolve(message);
                }
            });
        });

        return Promise.race([timeoutTask, subscribeTask]);
    }

    /**
     * 
     * @param {string} topic 
     * @param {string} message 
     */
    async signal(topic, message) {
        await this._pubsubClient.publish(topic, message);
    }

    async _sleep(timeout) {
        return new Promise((resolve) => {
            const timeoutRef = setTimeout(() => {
                clearTimeout(timeoutRef);
                resolve();
            }, timeout);
        });
    }
}

module.exports.Waitable = Waitable;