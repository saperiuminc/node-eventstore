const _ = require('lodash');

class WaitableConsumer {
    /**
     * 
     * @param {import("../pubsub/pubsub-client").PubSubClient} pubsubclient 
     * @param {string[]} topics 
     */
    constructor(pubsubclient, topics) {
        if (!pubsubclient) {
            throw new Error('pubsubClient is undefined');
        }

        if (!topics || !_.isArray(topics) || topics.length == 0) {
            throw new Error('topics should be defined and must be an array');
        }

        this._pubsubclient = pubsubclient;
        this._topics = topics;
        this._topicSubscriptionTokens = [];

        this._subscriptionSubscibers = [];
        this._gotMessageOnNoSubscribersCounter = 0;
    }

    async waitForSignal(timeout) {
        const self = this;
        const waitingSubscriptionTask = new Promise((resolve) => {
            const continueWithSubscription = function() {
                if (self._gotMessageOnNoSubscribersCounter > 0) {
                    // signal if we have messages when there are no subscribers
                    self._gotMessageOnNoSubscribersCounter = 0;
                    resolve('pending');
                } else {
                    self._subscriptionSubscibers.push(resolve);
                }
            }

            if (self._topicSubscriptionTokens.length == 0) {
                for (let i = 0; i < self._topics.length; i++) {
                    const topic = self._topics[i];
                    
                    const subscribeTasks = [];
                    
                    const task = self._pubsubclient.subscribe(topic, function() {
                        if (self._subscriptionSubscibers.length > 0) {
                            while(self._subscriptionSubscibers.length > 0) {
                                const sub = self._subscriptionSubscibers.pop();
                                sub('subscription');
                            }
                        } else {
                            // incr counter if no subscribers but a message is received
                            self._gotMessageOnNoSubscribersCounter++;
                        }
                    }).then((subscriptionToken) => {
                        self._topicSubscriptionTokens.push(subscriptionToken);
                    });

                    subscribeTasks.push(task);

                    Promise.all(subscribeTasks).then(continueWithSubscription);
                }
            } else {
                continueWithSubscription();
            }
        });

        const sleepTask = this._sleep(timeout);
        const response = await Promise.race([waitingSubscriptionTask, sleepTask]);
        return response;
    }

    async stopWaitingForSignal() {
        for (const subscriptionToken of this._topicSubscriptionTokens) {
            await this._pubsubclient.unsubscribe(subscriptionToken);
        }

        this._topicSubscriptionTokens = [];
    }

    async _sleep(timeout) {
        return new Promise((resolve) => {
            const timeoutRef = setTimeout(() => {
                clearTimeout(timeoutRef);
                resolve('sleep');
            }, timeout);
        })
    }
}

module.exports.WaitableConsumer = WaitableConsumer