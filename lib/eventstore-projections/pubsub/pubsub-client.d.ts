import { Redis } from "ioredis";

export interface SubscribeCallback { (error: Error, message:string): void }

export class PubSubClient {
    constructor(redisPublisherClient: Redis, redisSubscriberClient: Redis);
    publish(topic: string, message: string): Promise<void>;
    subscribe(topic: string, subscribeCallback: SubscribeCallback): Promise<void>;
    unsubscribe(subscriptionToken: string): Promise<void>;
}