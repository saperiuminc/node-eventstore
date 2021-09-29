import { Redis, RedisOptions } from "ioredis";
import { EventEmitter } from "events";
import { DistributedLock } from "../distributed-signal";


export interface TaskAssignmentGroupOptions {
    createRedisClient?(type: 'client' | 'subscriber' | 'bclient', redisOpts?: RedisOptions): Redis;
    initialTasks: Array<string>;
    groupId: string;
    distributedLock: DistributedLock;
    membershipPollingTimeout?: number;
}

export class TaskAssignmentGroup extends EventEmitter {
    constructor(options: TaskAssignmentGroupOptions);
    initialize(): Promise<void>;
    join(): Promise<void>;
    leave(): Promise<void>;
}