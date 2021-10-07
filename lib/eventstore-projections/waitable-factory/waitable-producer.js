class WaitableProducer {
    /**
     * 
     * @param {import('../pubsub/pubsub-client').PubSubClient} pubsubClient 
     */
    constructor(pubsubClient) {
        this._pubsubClient = pubsubClient;
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


module.exports.WaitableProducer = WaitableProducer