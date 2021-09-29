const shortid = require('shortid');
const debug = require('debug')('eventstore:task-assignment-group');
const _ = require('lodash');
const EventEmitter = require('events');
const pubsub = require('../pubsub');

/**
 * EventstoreProjectionConfig
 * @typedef {Object} TaskAssignmentGroupOptions
 * @property {Function} createRedisClient function to create redis client
 * @property {Array<String>} initialTasks tasks to be balanced
 * @property {String} groupId groupId for the group of tasks
 * @property {DistributedLock} distributedLock DistributedSignal helper
 * @property {Number} membershipPollingTimeout polling time for membership
 */


class TaskAssignmentGroup extends EventEmitter {
    /**
     * @param {TaskAssignmentGroupOptions} options additional options for the Eventstore projection extension
     * @constructor
     */
    constructor(options) {
        super(options);
        options = options || {};

        var defaults = {
            membershipPollingTimeout: 10000
        };
    
        this._options = _.defaults(options, defaults);
    
        if (!this._options.createRedisClient) {
            throw new Error('createRedisClient is required');
        }
    
        if (!this._options.initialTasks || !Array.isArray(this._options.initialTasks) || this._options.initialTasks.length == 0) {
            throw new Error('initialTasks must be an array and should not be empty');
        }
    
        if (!this._options.groupId) {
            throw new Error('groupId is required');
        }
    
        this._tasks = _.clone(this._options.initialTasks);
        this._pubsubClient = pubsub.createPubSubClient({
            createRedisClient: this._options.createRedisClient
        });
        this._distributedLock = options.distributedLock;
        this._isLeaderActive = false;
        this._redisClient = this._options.createRedisClient('client');
        this._redisClient.defineCommand('sanitizemembers', {
            numberOfKeys: 1,
            lua: `
          local members_set = KEYS[1]
          local expiration_time = ARGV[1]
    
          redis.call('ZREMRANGEBYSCORE', members_set, '-inf', expiration_time)
          local active_members = redis.call('ZRANGEBYSCORE', members_set, expiration_time, '+inf');
          return active_members
          `
        });
    
        this._isMember = false;
        this._isInitialized = false;
    }

    /**
     * @returns {Promise<void>} - returns a Promise a Promise of type void
     */
    async initialize() {
        this._doLeaderJob();
        this._isInitialized = true;
    }

     /**
     * @returns {Promise<void>} - returns a Promise a Promise of type void
     */
    async join() {
        if (!this._isInitialized) {
            throw new Error('Group not yet initialized. Call initialize first');
        }
    
        if (this._isMember) {
            throw new Error('You cannot join when you are already a member');
        }
    
        // add me as a member in our group membership set
        const membershipId = shortid.generate();
        this._isMember = true;
        this._doMemberJob(membershipId);
    }

    /**
     * @returns {Promise<void>} - returns a Promise a Promise of type void
     */
    async leave() {
        this._isMember = false;
        this._isLeaderActive = false;
    }

    /**
     * @returns {String} - returns member key
     */
    _getMembersKey() {
        return `task-assignment-groups:${this._options.groupId}:members`;
    }

    /**
     * @returns {String} - returns leader lock key
     */
    _getLeaderLockKey() {
        return `task-assignment-groups:${this._options.groupId}:leader-lock`;
    }

    /**
     * @returns {String} - returns member stream id
     */
    _getMembersStreamId() {
        return `task-assignment-groups:${this._options.groupId}:members-stream`;
    }

    /**
     * @param {string} membershipId 
     * @returns {Promise<void>} - returns a Promise a Promise of type void
     */
    async _doMemberJob(membershipId) {
        // register to the control stream
        const self = this;
        const membersStreamId = this._getMembersStreamId();
        const subscriptionToken = this._pubsubClient.subscribe(membersStreamId, function(error, message) {
            const msg = JSON.parse(message);
            if (msg.type == 'rebalance') {
                const assignedTasks = msg.payload[membershipId];
                if (assignedTasks) {
                    self.emit('rebalance', assignedTasks);
                }
            }
        });
  
        while (this._isMember) {
            const taskAssignmentGroupKey = this._getMembersKey();
            await this._redisClient.zadd(taskAssignmentGroupKey, Date.now(), membershipId);
            await this._sleep(this._options.membershipPollingTimeout);
        }
  
        await this._pubsubClient.unsubscribe(subscriptionToken);
    }

    /**
     * @returns {Promise<void>} - returns a Promise a Promise of type void
     */
    async _doLeaderJob() {
        const ttlDuration = this._options.membershipPollingTimeout;
        try {
            const lockKey = this._getLeaderLockKey();
            let lockToken = await this._distributedLock.lock(lockKey, ttlDuration);

            debug('lock acquired. doing task assignment group leader job');

            this._isLeaderActive = true;
            let continueJob = true;
            let lastActiveMembers = [];
            while (continueJob && this._isLeaderActive) {
                try {
                    const maxMemberActiveTime = Date.now() - (this._options.membershipPollingTimeout * 1.5);
                    /**
                     * @type{Array<string>}
                     */
                    const activeMembers = await this._redisClient.sanitizemembers(this._getMembersKey(), maxMemberActiveTime);
                    const sortedActiveMembers = activeMembers.sort();

                    // FORREVIEW
                    // CHECK ALSO IF TASKS CHANGE
                    // CLONE BEFORE LOOP
                    if (!_.isEqual(lastActiveMembers, sortedActiveMembers)) {
                        // rebalance
                        const membersStreamId = this._getMembersStreamId();
                        const newAssignment = {};

                        // iterate and assign
                        for (let i = 0; i < this._tasks.length; i++) {
                            const taskId = this._tasks[i];
                        
                            const member = sortedActiveMembers[i % sortedActiveMembers.length];

                            if (!newAssignment[member]) {
                                newAssignment[member] = [];
                            }

                            newAssignment[member].push(taskId);
                        }

                        debug('got new assignment. rebalancing', newAssignment);

                        const message = {
                            type: 'rebalance',
                            payload: newAssignment
                        }

                        await this._pubsubClient.publish(membersStreamId, JSON.stringify(message))
                    }

                    lastActiveMembers = sortedActiveMembers;

                    await this._distributedLock.extend(lockToken, ttlDuration);
                    await this._sleep(ttlDuration / 2);
                } catch (error) {
                    console.error('error in while loop in shared job', error);
                    // if there is an error then exit while loop and then contend again
                    continueJob = false;
                }
            }
        } catch (error) {
            if (error.name == 'LockError') {
                // ignore this just try again later to acquire lock
                // debug('lost in lock contention. will try again in', ttlDuration);
            } else {
                console.error('error in doing shared timer job', error, error.name);
            }
        } finally {
            // sleep before contending again
            this._isLeaderActive = false;
            await this._sleep(ttlDuration + (Math.floor(Math.random() * (ttlDuration / 2))));
            await this._doLeaderJob();
        }
    }

    
    /**
     * @param {Number} timeout sleep timeout
     * @returns {Promise<void>} - returns a Promise a Promise of type void
     */
    async _sleep(timeout) {
        return new Promise((resolve) => {
            const timeoutRef = setTimeout(() => {
                clearTimeout(timeoutRef);
                resolve();
            }, timeout);
        });
    }
}

module.exports.TaskAssignmentGroup = TaskAssignmentGroup;