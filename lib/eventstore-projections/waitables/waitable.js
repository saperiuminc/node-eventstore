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
        let subscriptionToken;
        let lateToken = false;
        return new Promise((resolve, reject) => {
            try {
                const timeoutRef = setTimeout(() => {
                    clearTimeout(timeoutRef);
                    resolve();
                }, timeout);

                self._pubsubClient.subscribe(topic, async function(error, message) {
                    clearTimeout(timeoutRef);
                    if (error) {
                        reject(new Error('error in subscribe'));
                    } else {
                        resolve(message);
                    }
                }).then((token) => {
                    subscriptionToken = token;
                    if(lateToken) {
                        if(subscriptionToken) {
                            // cleanup, fire and forget on unsub on error
                            self._pubsubClient.unsubscribe(subscriptionToken);
                        }
                    }
                })
                .catch(() => {
                    reject(new Error('error in subscribe'));
                });
            } catch (error) {
                reject(new Error('error in subscribe'));
            }
        }).then(() => {
            if(subscriptionToken) {
                // cleanup, make sure to wait on unsub to return
                return self._pubsubClient.unsubscribe(subscriptionToken);
            } else {
                // flag that token was late
                lateToken = true;
            }
        }).catch(() => {
            if(subscriptionToken) {
                // cleanup, fire and forget on unsub on error
                self._pubsubClient.unsubscribe(subscriptionToken);
            } else {
                // flag that token was late
                lateToken = true;
            }
        });
    }

    /**
     * 
     * @param {string} topic 
     * @param {string} message 
     */
    async signal(topic, message) {
        await this._pubsubClient.publish(topic, message);
    }
}

module.exports.Waitable = Waitable;