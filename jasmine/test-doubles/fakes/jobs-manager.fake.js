const _ = require('lodash');
const { subscribe } = require('pubsub-js');

function JobsManagerFake () {

}

JobsManagerFake.prototype = {
    _jobs: {},
    _subscribers: [],
    _jobResults: {},
    queueJob: async function(job, options) {
        if (this._jobs) {
            this._jobs[job.id] = {
                job: job,
                options: options
            };
    
            const delay = options.delay || 100;
            setTimeout(() => {
                _.forEach(this._subscribers, async (subscriber) => {
                    if (subscriber.jobGroup == job.group) {
                        const result = await subscriber.onProcessJob.call(subscriber.owner, job.id, job.payload, this._jobResults[job.id]);
    
                        this._jobResults[job.id] = result;
                        await subscriber.onCompletedJob.call(subscriber.owner, job.id, job.payload);
                    }
                    
                });
            }, delay);
        }
    },

    processJobGroup: async function(owner, jobGroup, onProcessJob, onCompletedJob) {
        if (this._subscribers) {
            this._subscribers.push({
                owner: owner,
                jobGroup, jobGroup,
                onProcessJob: onProcessJob,
                onCompletedJob: onCompletedJob
            });
        }
    },
    reset: function() {
        // NOTE: simple destroy function just nullifies the private fields
        this._subscribers = null;
        this._jobs = null;
    }
};

module.exports = JobsManagerFake;