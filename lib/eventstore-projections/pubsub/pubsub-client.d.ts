import { Redis } from "ioredis";

export interface SubscribeCallback { (error: Error, message:string): void }

export class PubSubClient {
    constructor(redisPublisherClient: Redis, redisSubscriberClient: Redis);
    publish(topic: string, message: string): Promise<void>;
    subscribe(topic: string, subscribeCallback: SubscribeCallback): Promise<string>;
    unsubscribe(subscriptionToken: string): Promise<void>;
    /**
     * Only use on the factory class. Not intended for public
     * @param topic 
     * @param message 
     */
    _onMessage(topic: string, message: string): void;
}