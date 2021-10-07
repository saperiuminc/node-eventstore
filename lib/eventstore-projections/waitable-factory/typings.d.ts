export interface WaitableFactoryOptions {
    createRedisClient?(type: 'client' | 'subscriber' | 'bclient', redisOpts?: RedisOptions): Redis;
}