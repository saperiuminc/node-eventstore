import { PubSubClient } from "../pubsub/pubsub-client";

export class WaitableProducer {
    constructor(pubsubClient: PubSubClient);
    signal(topic: string, message: string): Promise<void>;
}