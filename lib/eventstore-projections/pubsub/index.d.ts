import { Redis, RedisOptions } from "ioredis";
import { PubSubClient } from "./pubsub-client";

export interface PubSubOptions {
    createRedisClient?(type: 'client' | 'subscriber' | 'bclient', redisOpts?: RedisOptions): Redis;
}

export function createPubSubClient(options: PubSubOptions): PubSubClient {}