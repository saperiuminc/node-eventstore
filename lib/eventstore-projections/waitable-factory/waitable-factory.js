const pubsub = require('../pubsub/index');
const WaitableConsumer = require('./waitable-consumer').WaitableConsumer;
const WaitableProducer = require('./waitable-producer').WaitableProducer;

class WaitableFactory {
    /**
     * 
     * @param {import('./typings').WaitableFactoryOptions} options 
     */
    constructor(options) {
        const pubsubClient = pubsub.createPubSubClient({
            createRedisClient: options.createRedisClient
        });

        this._pubsubClient = pubsubClient;
    }

    /**
     * 
     * @param {string[]} topics 
     * @returns {WaitableConsumer}
     */
    async createWaitableConsumer(topics) {
        const consumer = new WaitableConsumer(this._pubsubClient, topics);
        await consumer.init();
        return consumer;
    }

    /**
     * 
     * @returns {WaitableProducer}
     */
    createWaitableProducer() {
        const producer = new WaitableProducer(this._pubsubClient);
        return producer;
    }
}

module.exports.WaitableFactory = WaitableFactory;