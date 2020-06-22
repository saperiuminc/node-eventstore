const _ = require('lodash');
const debug = require('debug')('eventstore:jobs-manager');

/**
 * RedisConfig
 * @typedef {Object} RedisConfig
 * @property {String} host the redis host
 * @property {Number} port the redis port
 * @property {Password} password the redis password
 */

/**
 * Redis
 * @typedef {Object} IORedis
 */

/**
 * JobsManagerOptions
 * @typedef {Object} JobsManagerOptions
 * @property {RedisConfig} redisConfig the redis configuration
 * @property {Object} BullQueue the bull queue
 * @property {IORedis} ioredis the ioredis library
 * @property {Number} concurrency the number of concurrent jobs in a jog group
 */

/**
 * Job
 * @typedef {Object} Job
 * @property {String} id unique id of the job
 * @property {String} group group of this job
 * @property {Object} payload payload of the job
 */

/**
 * JobsManager
 * @typedef {Object} JobsManager
 * @property {QueueJob} queueJob queue a job
 * @property {ProcessJobGroup} processJobGroup process a job by group
 */


/**
 * JobDoneCallback
 * @callback JobDoneCallback
 * @param {Error} error optional error if an error occurred in processing the job
 * @param {Object} result result to save for this job. can be used when processing the same job with the same id again
 */

/**
 * OnProcessJob
 * @callback OnProcessJob
 * @param {Job} job the job object toe queue
 * @param {JobDoneCallback} done the callback to say that the job is done
 */

/**
 * OnProcessJobCompleted
 * @callback OnProcessJobCompleted
 * @param {Job} job the job object toe queue
 * @param {Object} result the job object toe queue
 */

/**
 * ProcessJobGroup
 * @callback ProcessJobGroup
 * @param {String} jobGroup the job object toe queue
 * @param {OnProcessJob} jobGroup the job object toe queue
 * @param {OnProcessJobCompleted} jobGroup the job object toe queue
 */


/**
 * QueueJob
 * @callback QueueJob
 * @param {Job} job the job object toe queue
 * @param {JobOptions} options some configuration/options for this job
 */


/**
 * JobOptions
 * @typedef {Object} JobOptions
 * @property {Number} delay number of milliseconds to sleep before processing this job
 */


/**
 * JobsManager constructor
 * @class
 * @param {JobsManagerOptions} options additional options for the jobs manager
 * @constructor
 */
function JobsManager(options) {
    options = options || {};
    var defaults = {
        redisConfig: {
            host: 'localhost',
            port: 6379,
            password: undefined
        },
        concurrency: 1
    };

    this.options = _.defaults(options, defaults);

    debug('jobs-manager constructor with options', this.options);
    this._jobGroupsQueue = {};
    this._jobs = {};
}


/**
 * @type {JobsManagerOptions}
 */
JobsManager.prototype.options;

/**
 * @type {Object.<string, Job>}
 */
JobsManager.prototype._jobs;

/**
 * @type {Object}
 */
JobsManager.prototype._jobGroupsQueue;

/**
 * @param {String} jobGroup the job group to process
 * @param {OnProcessJob} onProcessJob callback to be called when a job is to be processed
 * @param {OnProcessJobCompleted} onCompletedJob callback to be called when a job is completed
 * @returns {Promise<void>} - returns a Promise of type void
 */
JobsManager.prototype.processJobGroup = async function(jobGroup, onProcessJob, onCompletedJob) {
    try {
        debug('processJobGroup called with params:', jobGroup);


        if (!jobGroup) {
            throw new Error('jobGroup is required');
        }

        if (!onProcessJob || !_.isFunction(onProcessJob)) {
            throw new Error('onProcessJob is missing or is not a function');
        }

        if (!onCompletedJob || !_.isFunction(onCompletedJob)) {
            throw new Error('onCompletedJob is missing or is not a function');
        }
        const self = this;
        let queue = this._jobGroupsQueue[jobGroup];

        if (queue) {
            queue.on('error', function(err) {
                // An error occured.
                console.error('ON error:', err);
            });
            queue.on('waiting', function(jobId) {
                // A Job is waiting to be processed as soon as a worker is idling.
                debug('ON waiting:', jobId);
            });
            queue.on('active', function(job, jobPromise) {
                // A job has started. You can use `jobPromise.cancel()`` to abort it.
                debug('ON active:', job.id);
            });
            queue.on('progress', function(job, progress) {
                // A job's progress was updated!
                debug('ON progress:', job.id, (progress * 100));
            });
            queue.on('paused', function() {
                // The queue has been paused.
                debug('ON paused');
            });
            queue.on('resumed', function(job) {
                // The queue has been resumed.
                debug('ON resumed:', job.id);
            });
            queue.on('cleaned', function(jobs, type) {
                // Old jobs have been cleaned from the queue. `jobs` is an array of cleaned
                // jobs, and `type` is the type of jobs cleaned.
                debug('ON cleaned:', type, jobs);
            });
            queue.on('drained', function() {
                // Emitted every time the queue has processed all the waiting jobs (even if there can be some delayed jobs not yet processed)
                debug('ON drained');
            });
            queue.on('removed', function(job) {
                // A job successfully removed.
                debug('ON removed:', job.id);
            });

            queue.on('stalled', function(job) {
                // A job has been marked as stalled. This is useful for debugging job
                // workers that crash or pause the event loop.
                debug('ON stalled:', job.id);
                // TODO: handle STALLED event
                // self._changeStatus(job.data.key, 'STALLED');
            });
        
            queue.on('failed', function(job, err) {
                // A job failed with reason `err`!
                debug('ON failed:', job.id, err);
                // TODO: handle FAILED event
                // self._changeStatus(job.data.key, 'FAILED', err);
            });

            queue.on('completed', function(job, result) {
                // A job successfully completed with a `result`.
                debug('ON completed:', job.id, result);

                onCompletedJob(self._jobs[job.id], result);
            });

            queue.process(self.options.concurrency, function(job, done) {
                onProcessJob(self._jobs[job.id], (error, result) => {
                    if (error) {
                        console.error(error);
                        console.error('onProcessJob callback failed with error', error);
                        done(error);
                    } else {
                        if (result) {
                            // TODO: add the result to redis to pass to the next job with the same id
                            done(null, result);
                        }
                    }
                });
            });
        }
    } catch (error) {
        console.error('error in processJobGroup with params and error:', jobGroup, error);
        throw error;
    }
};

/**
 * @param {Job} job the job to queue
 * @returns {Promise<void>} - returns a Promise of type void
 */
JobsManager.prototype.queueJob = async function(job) {
    try {
        debug('queueJob called with params:', job);

        if (!job) {
            throw new Error('job is required');
        }

        if (!job.id) {
            throw new Error('id is required');
        }

        if (!job.group) {
            throw new Error('group is required');
        }

        if (!job.payload) {
            throw new Error('payload is required');
        }

        let queue = this._jobGroupsQueue[job.group];

        if (!queue) {
            queue = this._jobGroupsQueue[job.group] = new this.options.BullQueue(job.group, { redis: this.options.redisConfig });
        }

        this._jobs[job.id] = job;

        await queue.add(job.payload, {
            jobId: job.id,
            removeOnComplete: true
        });
    } catch (error) {
        console.error('error in queueJob with params and error:', job, error);
        throw error;
    }
};

module.exports = JobsManager;