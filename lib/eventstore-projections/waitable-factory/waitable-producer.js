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
        const topicMessage = message ? message : topic;
        await this._pubsubClient.publish(topic, topicMessage);
    }
}


module.exports.WaitableProducer = WaitableProducer