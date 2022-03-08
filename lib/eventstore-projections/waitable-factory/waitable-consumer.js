const _ = require('lodash');
const shortid = require('shortid');

// TODO: create a separate public init function instead. the waitForSignal call becomes dual purpose. it initializes the subscription
// at the same time doing the actual waiting for signal. It would be better if initialization is called as a separate function.
// The code change for now does not change the current interface. 
// Will do interface code change after code release
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
        this._waitSubscriptions = {};
        // this._gotMessageOnNoSubscribersCounter = 0;
        this._gotMessageOnNoSubscribersMessages = [];
    }

    async _initializeSubscriptions() {
        const self = this;
        for (let i = 0; i < self._topics.length; i++) {
            const topic = self._topics[i];
            
            const subscriptionToken = await self._pubsubclient.subscribe(topic, function(err, message) {
                let waitSubscriptionsCalled = 0;
                for (const token in self._waitSubscriptions) {
                    const callback = self._waitSubscriptions[token];
                    callback({
                        type: 'subscription',
                        messages: [message]
                    });
                    waitSubscriptionsCalled++;
                }
                // self._gotMessageOnNoSubscribersMessages = [];

                if (waitSubscriptionsCalled == 0) {
                    // incr counter if no subscribers but a message is received
                    // self._gotMessageOnNoSubscribersCounter++;
                    self._gotMessageOnNoSubscribersMessages.push(message);
                }

                self._waitSubscriptions = {};
            })

            self._topicSubscriptionTokens.push(subscriptionToken);
        }
    }

    async waitForSignal(timeout) {
        const self = this;
        const waitToken = shortid.generate();
        const waitingSubscriptionTask = new Promise((resolve) => {
            const continueWithSubscription = function() {
                if (self._gotMessageOnNoSubscribersMessages.length > 0) {
                    // signal if we have messages when there are no subscribers
                    // self._gotMessageOnNoSubscribersCounter = 0;
                    // self._gotMessageOnNoSubscribersMessages = [];
                    resolve({
                        type: 'pending',
                        messages: self._gotMessageOnNoSubscribersMessages
                    });
                    self._gotMessageOnNoSubscribersMessages = [];
                } else {
                    self._waitSubscriptions[waitToken] = resolve;
                }
            }

            if (self._topicSubscriptionTokens.length == 0) {
                // TODO: move _initializeSubscriptions to a separate public function call that will be called by the factory.
                // this removes the dual purpose of the waitForSignal which initializes the subscriptions if there are no subs
                // yet and at the same time the actual subscription 
                self._initializeSubscriptions().then(continueWithSubscription);
            } else {
                continueWithSubscription();
            }
        });

        const sleepTask = this._sleep(timeout);
        const response = await Promise.race([waitingSubscriptionTask, sleepTask]);

        for (const token in self._waitSubscriptions) {
            if (token == waitToken) {
                delete self._waitSubscriptions[token];
                break;
            }
        }

        return response;
    }

    async stopWaitingForSignal() {
        for (const subscriptionToken of this._topicSubscriptionTokens) {
            await this._pubsubclient.unsubscribe(subscriptionToken);
        }

        this._topicSubscriptionTokens = [];
        this._waitSubscriptions = {};
    }

    async _sleep(timeout) {
        return new Promise((resolve) => {
            const timeoutRef = setTimeout(() => {
                clearTimeout(timeoutRef);
                resolve({
                    type: 'sleep',
                    messages: []
                });
            }, timeout);
        })
    }
}

module.exports.WaitableConsumer = WaitableConsumer