import { PubSubClient } from "../pubsub/pubsub-client";

export class WaitableConsumer {
    constructor(pubsubClient: PubSubClient, topics: string[]);
    init(): Promise<void>;
    waitForSignal(timeout: number): Promise<void>;
    stopWaitingForSignal(): Promise<void>;
}