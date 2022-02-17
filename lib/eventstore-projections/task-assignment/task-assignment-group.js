const shortid = require('shortid');
const debug = require('debug')('eventstore:task-assignment-group');
const _ = require('lodash');
const EventEmitter = require('events');
const pubsub = require('../pubsub');
class TaskAssignmentGroup extends EventEmitter {
    constructor(options) {
        super(options);
        options = options || {};

        var defaults = {
            membershipPollingTimeout: 10000
        };
    
        this._options = Object.assign(_.clone(defaults), options);
    
        if (!this._options.createRedisClient) {
            throw new Error('createRedisClient is required');
        }
    
        if (!this._options.initialTasks || !Array.isArray(this._options.initialTasks) || this._options.initialTasks.length == 0) {
            this._options.initialTasks = [];
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

    async join() {
        if (!this._isMember) {
            // add me as a member in our group membership set
            const membershipId = shortid.generate();
            this._isMember = true;
            // this._doMemberJob(membershipId);
            await this._doMemberJob(membershipId);
        }

        this._doLeaderJob();
        this._isInitialized = true;
    }

    async leave() {
        this._isMember = false;
        this._isLeaderActive = false;
    }

    async addTasks(tasks) {
        if (this._isInitialized) {
            const leaderStreamId = this._getLeadersStreamId();

            const message = {
                type: 'addTasks',
                payload: tasks
            }

            await this._pubsubClient.publish(leaderStreamId, JSON.stringify(message));
        }
    }

    async removeTasks(tasks) {
        if (this._isInitialized) {
            const leaderStreamId = this._getLeadersStreamId();

            const message = {
                type: 'removeTasks',
                payload: tasks
            }

            await this._pubsubClient.publish(leaderStreamId, JSON.stringify(message));
        }
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
     * @returns {String} - returns member stream id
     */
    _getLeadersStreamId() {
        return `task-assignment-groups:${this._options.groupId}:leader-stream`;
    }

    /**
     * @param {string} membershipId 
     * @returns {Promise<void>} - returns a Promise a Promise of type void
     */
    async _doMemberJob(membershipId) {
        // register to the control stream
        const self = this;
        const membersStreamId = self._getMembersStreamId();
        const memberSubscriptionToken = await self._pubsubClient.subscribe(membersStreamId, function(error, message) {
            debug('WILL REBALANCE', membersStreamId, message);
            const msg = JSON.parse(message);
            if (msg.type == 'rebalance') {
                const assignedTasks = msg.payload[membershipId] || [];
                const rebalanceId = shortid.generate();
                self.emit('rebalance', assignedTasks, rebalanceId);
            }
        });

        const leaderStreamId = self._getLeadersStreamId();
        const taskSubscriptionToken = await self._pubsubClient.subscribe(leaderStreamId, function(error, message) {
            const msg = JSON.parse(message);
            if (msg.type == 'addTasks') {
                const newTasks = msg.payload;
                const clonedTasks = _.clone(self._tasks);
                if (newTasks && Array.isArray(newTasks) && newTasks.length > 0 
                    && clonedTasks && Array.isArray(clonedTasks)) 
                {
                    self._tasks = self._arrayUnique(clonedTasks.concat(newTasks));
                }
            } else if (msg.type == 'removeTasks') {
                const toRemoveTasks = msg.payload;
                const clonedTasks = _.clone(self._tasks);
                if (toRemoveTasks && Array.isArray(toRemoveTasks) && toRemoveTasks.length > 0 
                    && clonedTasks && Array.isArray(clonedTasks)) 
                {
                    self._tasks = clonedTasks.filter( ( el ) => !toRemoveTasks.includes( el ) );
                }
            }
        });
  
        if (self._isMember) {
            const taskAssignmentGroupKey = self._getMembersKey();
            await self._redisClient.zadd(taskAssignmentGroupKey, Date.now(), membershipId);
        }

        const startPolling = async function() {
            while (self._isMember) {
                debug('self._isMember', self._isMember);
                const taskAssignmentGroupKey = self._getMembersKey();
                await self._redisClient.zadd(taskAssignmentGroupKey, Date.now(), membershipId);
                await self._sleep(self._options.membershipPollingTimeout);
            }
    
            await self._pubsubClient.unsubscribe(memberSubscriptionToken);
            await self._pubsubClient.unsubscribe(taskSubscriptionToken);
        }

        startPolling();
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
            let lastActiveMembers = [];
            let sortedOriginalTasks = _.clone(this._tasks);

            while (this._isLeaderActive) {
                try {
                    const maxMemberActiveTime = Date.now() - (this._options.membershipPollingTimeout * 1.5);
                    /**
                     * @type{Array<string>}
                     */
                    const activeMembers = await this._redisClient.sanitizemembers(this._getMembersKey(), maxMemberActiveTime);
                    const sortedActiveMembers = activeMembers.sort();
                    const sortedTasks = this._tasks.sort();

                    if (!_.isEqual(lastActiveMembers, sortedActiveMembers) || !_.isEqual(sortedOriginalTasks, sortedTasks)) {
                        // rebalance
                        const membersStreamId = this._getMembersStreamId();
                        const newAssignment = {};

                        // iterate and assign
                        for (let i = 0; i < this._tasks.length; i++) {
                            const taskId = this._tasks[i];
                        
                            debug('sortedActiveMembers', sortedActiveMembers);
                            const member = sortedActiveMembers[i % sortedActiveMembers.length];

                            if(member != undefined) {
                                if (!newAssignment[member]) {
                                    newAssignment[member] = [];
                                }

                                newAssignment[member].push(taskId);
                            }
                        }

                        const message = {
                            type: 'rebalance',
                            payload: newAssignment
                        }

                        debug('got new assignment. rebalancing', newAssignment, membersStreamId, message);

                        await this._pubsubClient.publish(membersStreamId, JSON.stringify(message));
                    }

                    lastActiveMembers = sortedActiveMembers;
                    sortedOriginalTasks = sortedTasks;

                    await this._distributedLock.extend(lockToken, ttlDuration);
                    await this._sleep(ttlDuration / 2);
                } catch (error) {
                    console.error('error in while loop in shared job', error);
                    // if there is an error then exit while loop and then contend again
                    this._isLeaderActive = false;
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

    _arrayUnique(array) {
        var a = array.concat();
        for(var i=0; i<a.length; ++i) {
            for(var j=i+1; j<a.length; ++j) {
                if(a[i] === a[j])
                    a.splice(j--, 1);
            }
        }
    
        return a;
    }
}

module.exports.TaskAssignmentGroup = TaskAssignmentGroup;