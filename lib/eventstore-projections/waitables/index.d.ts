import { Redis, RedisOptions } from "ioredis";
import { Waitable } from "./waitable";


export interface WaitableOptions {
    createRedisClient?(type: 'client' | 'subscriber' | 'bclient', redisOpts?: RedisOptions): Redis;
}

export function createWaitable(options: WaitableOptions): Waitable {}