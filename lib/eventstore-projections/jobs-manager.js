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
 * JobsManagerOptions
 * @typedef {Object} JobsManagerOptions
 * @property {RedisConfig} redisConfig the redis configuration
 * @property {Object} BullQueue the bull queue
 */

/**
 * Job
 * @typedef {Object} Job
 * @property {String} jobId unique id of the job
 * @property {String} jobGroup group of this job
 * @property {Object} jobPayload payload of the job
 */

/**
 * @class JobsManager
 * @param {JobsManagerOptions} options additional options for the jobs manager
 * @returns {JobsManager}
 */
function JobsManager(options) {
    options = options || {};
    var defaults = {
        redisConfig: {
            host: 'localhost',
            port: 6379,
            password: undefined
        }
    };

    this.options = _.defaults(options, defaults);
    this._jobGroups = {};
}


/**
 * @type {JobsManagerOptions}
 */
JobsManager.prototype.options;

/**
 * @type {Object}
 */
JobsManager.prototype._jobGroups;

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

        if (!job.jobId) {
            throw new Error('jobId is required');
        }

        if (!job.jobGroup) {
            throw new Error('jobGroup is required');
        }

        if (!job.jobPayload) {
            throw new Error('jobPayload is required');
        }

        const jobOpts = {

        };

        let queue = this._jobGroups[job.jobGroup];

        if (!queue) {
            queue = this._jobGroups[job.jobGroup] = new this.options.BullQueue(job.jobGroup, { redis: this.options.redisConfig });
        }

        await queue.add(job.jobPayload, {
            jobId: job.jobId,
            removeOnComplete: true
        });
    } catch (error) {
        console.error('error in queueJob with params and error:', job, error);
        throw error;
    }
};

module.exports = JobsManager;