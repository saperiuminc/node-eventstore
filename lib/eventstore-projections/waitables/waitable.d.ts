import { PubSubClient } from "../pubsub/pubsub-client";

export class Waitable {
    constructor(pubsubClient: PubSubClient);
    waitForSignal(topic: string, timeout: number): Promise<string>;
    signal(topic: string, message: string): Promise<void>;
}