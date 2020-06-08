const JobsManager = require('../lib/eventstore-projections/jobs-manager');




fdescribe('jobs-manager tests', () => {
    // just instantiating for vscode jsdoc intellisense
    let jobsManager = new JobsManager();
    let options;
    let redis;
    let bullsQueue;
    let queueInstance;
    beforeEach(() => {
        redis = jasmine.createSpyObj('redis', ['']);
        bullQueue = jasmine.createSpyObj('bullQueue', ['Queue']);
        queueInstance = jasmine.createSpyObj('queueInstance', ['add']);
        bullQueue.Queue.and.returnValue(queueInstance);
        queueInstance.add.and.returnValue(Promise.resolve());

        options = {
            redis: redis,
            BullQueue: bullQueue.Queue,
            redisConfig: {
                host: 'localhost',
                port: 6379,
                password: 'secret'
            }
        };

        jobsManager = new JobsManager(options);
    });

    describe('queueJob', () => {
        describe('validating params and output', () => {
            it('should validate required paramater job', async(done) => {
                try {
                    await jobsManager.queueJob();
                } catch (error) {
                    expect(error.message).toEqual('job is required');
                    done();
                }
            })

            it('should validate required paramater jobId', async(done) => {
                try {
                    await jobsManager.queueJob({});
                } catch (error) {
                    expect(error.message).toEqual('jobId is required');
                    done();
                }
            })

            it('should validate required paramater jobGroup', async(done) => {
                try {
                    await jobsManager.queueJob({
                        jobId: 'job_id'
                    });
                } catch (error) {
                    expect(error.message).toEqual('jobGroup is required');
                    done();
                }
            })

            it('should validate required paramater jobPayload', async(done) => {
                try {
                    await jobsManager.queueJob({
                        jobId: 'job_id',
                        jobGroup: 'job_group'
                    });
                } catch (error) {
                    expect(error.message).toEqual('jobPayload is required');
                    done();
                }
            })

            it('should return a Promise', async(done) => {
                const promise = jobsManager.queueJob({
                    jobId: 'job_id',
                    jobGroup: 'job_group',
                    jobPayload: 'job_payload'
                });
                expect(promise).toBeInstanceOf(Promise);
                const result = await promise;
                expect(result).toBeUndefined();
                done();
            })
        })

        describe('using the bulls queue', () => {
            it('should pass the correct params to the Queue constructor', async(done) => {
                const job = {
                    jobId: 'job_id',
                    jobGroup: 'job_group',
                    jobPayload: 'job_payload'
                };
                await jobsManager.queueJob(job);

                expect(bullQueue.Queue).toHaveBeenCalledWith(job.jobGroup, {
                    redis: options.redisConfig
                });
                done();
            })

            it('should pass the correct params to the Queue.add', async(done) => {
                const job = {
                    jobId: 'job_id',
                    jobGroup: 'job_group',
                    jobPayload: 'job_payload'
                };
                await jobsManager.queueJob(job);

                expect(queueInstance.add).toHaveBeenCalledWith(job.jobPayload, {
                    jobId: job.jobId,
                    removeOnComplete: true
                });
                done();
            })

            it('should not create another queue when the same jobGroup is passed again', async(done) => {
                const job = {
                    jobId: 'job_id',
                    jobGroup: 'job_group',
                    jobPayload: 'job_payload'
                };

                const job2 = {
                    jobId: 'job_id_2',
                    jobGroup: 'job_group',
                    jobPayload: 'job_payload'
                };

                await jobsManager.queueJob(job);
                await jobsManager.queueJob(job2);

                expect(bullQueue.Queue).toHaveBeenCalledTimes(1);
                done();
            })

            it('should create another queue when a different jobGroup is passed', async(done) => {
                const job = {
                    jobId: 'job_id',
                    jobGroup: 'job_group',
                    jobPayload: 'job_payload'
                };

                const job2 = {
                    jobId: 'job_id_2',
                    jobGroup: 'job_group_2',
                    jobPayload: 'job_payload'
                };

                await jobsManager.queueJob(job);
                await jobsManager.queueJob(job2);

                expect(bullQueue.Queue).toHaveBeenCalledTimes(2);
                done();
            })
        })
    });
})