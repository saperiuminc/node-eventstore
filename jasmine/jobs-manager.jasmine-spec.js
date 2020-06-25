const JobsManager = require('../lib/eventstore-projections/jobs-manager');




describe('jobs-manager tests', () => {
    // just instantiating for vscode jsdoc intellisense
    let jobsManager = new JobsManager();
    let options;
    let ioredis;
    let bullsQueue;
    let queueInstance;
    beforeEach(() => {
        ioredis = jasmine.createSpyObj('ioredis', ['redisOptions','keys','hmset','hgetall']);
        bullQueue = jasmine.createSpyObj('bullQueue', ['Queue']);
        queueInstance = jasmine.createSpyObj('queueInstance', ['add', 'on', 'process']);
        bullQueue.Queue.and.returnValue(queueInstance);
        queueInstance.add.and.returnValue(Promise.resolve());
        ioredis.redisOptions.and.returnValue({
            host: 'localhost',
            port: 6379,
            password: 'secret'
        });
        
        options = {
            ioredis: ioredis,
            BullQueue: bullQueue.Queue
        };

        jobsManager = new JobsManager(options);
    });

    describe('queueJob', () => {
        describe('validating params and output', () => {
            it('should validate required paramater job', async (done) => {
                try {
                    await jobsManager.queueJob();
                } catch (error) {
                    expect(error.message).toEqual('job is required');
                    done();
                }
            })

            it('should validate required paramater id', async (done) => {
                try {
                    await jobsManager.queueJob({});
                } catch (error) {
                    expect(error.message).toEqual('id is required');
                    done();
                }
            })

            it('should validate required paramater group', async (done) => {
                try {
                    await jobsManager.queueJob({
                        id: 'job_id'
                    });
                } catch (error) {
                    expect(error.message).toEqual('group is required');
                    done();
                }
            })

            it('should validate required paramater payload', async (done) => {
                try {
                    await jobsManager.queueJob({
                        id: 'job_id',
                        group: 'job_group'
                    });
                } catch (error) {
                    expect(error.message).toEqual('payload is required');
                    done();
                }
            })

            it('should return a Promise', async (done) => {
                const promise = jobsManager.queueJob({
                    id: 'job_id',
                    group: 'job_group',
                    payload: 'job_payload'
                });
                expect(promise).toBeInstanceOf(Promise);
                const result = await promise;
                expect(result).toBeUndefined();
                done();
            })
        })

        describe('using the bulls queue', () => {
            it('should pass the correct params to the Queue constructor', async (done) => {
                const job = {
                    id: 'job_id',
                    group: 'job_group',
                    payload: 'job_payload'
                };
                await jobsManager.queueJob(job);

                expect(bullQueue.Queue).toHaveBeenCalledWith(job.group, {
                    redis: options.ioredis.redisOptions
                });
                done();
            })

            it('should pass the correct params to the Queue.add', async (done) => {
                const job = {
                    id: 'job_id',
                    group: 'job_group',
                    payload: 'job_payload'
                };
                await jobsManager.queueJob(job);

                expect(queueInstance.add).toHaveBeenCalledWith(job.payload, {
                    jobId: job.id,
                    delay: undefined,
                    removeOnComplete: true,
                    timeout: 10000
                });
                done();
            })

            it('should not create another queue when the same group is passed again', async (done) => {
                const job = {
                    id: 'job_id',
                    group: 'job_group',
                    payload: 'job_payload'
                };

                const job2 = {
                    id: 'job_id_2',
                    group: 'job_group',
                    payload: 'job_payload'
                };

                await jobsManager.queueJob(job);
                await jobsManager.queueJob(job2);

                expect(bullQueue.Queue).toHaveBeenCalledTimes(1);
                done();
            })

            it('should create another queue when a different group is passed', async (done) => {
                const job = {
                    id: 'job_id',
                    group: 'job_group',
                    payload: 'job_payload'
                };

                const job2 = {
                    id: 'job_id_2',
                    group: 'job_group_2',
                    payload: 'job_payload'
                };

                await jobsManager.queueJob(job);
                await jobsManager.queueJob(job2);

                expect(bullQueue.Queue).toHaveBeenCalledTimes(2);
                done();
            })
        })
    });

    describe('processJobGroup', () => {
        describe('validating params and output', () => {
            it('should validate required paramater jobGroup', async (done) => {
                try {
                    await jobsManager.processJobGroup();
                } catch (error) {
                    expect(error.message).toEqual('owner is required');
                    done();
                }
            })


            it('should validate required paramater owner', async (done) => {
                try {
                    const owner = this;
                    await jobsManager.processJobGroup();
                } catch (error) {
                    expect(error.message).toEqual('owner is required');
                    done();
                }
            })

            it('should validate required paramater onProcessJob as a function', async (done) => {
                try {
                    const owner = this;
                    const jobGroup = 'job_group';
                    await jobsManager.processJobGroup(owner, jobGroup, 'not a function');
                } catch (error) {
                    expect(error.message).toEqual('onProcessJob is missing or is not a function');
                    done();
                }
            })

            it('should validate required paramater onProcessJob as defined', async (done) => {
                try {
                    const owner = this;
                    const jobGroup = 'job_group';
                    await jobsManager.processJobGroup(owner, jobGroup);
                } catch (error) {
                    expect(error.message).toEqual('onProcessJob is missing or is not a function');
                    done();
                }
            })

            it('should validate required paramater onCompletedJob as a function', async (done) => {
                try {
                    const owner = this;
                    const jobGroup = 'job_group';
                    const onProcessJob = (jobId, jobData, lastResult, jobDone) => {
                        jobDone();
                    };
                    await jobsManager.processJobGroup(owner, jobGroup, onProcessJob, 'not a function');
                } catch (error) {
                    expect(error.message).toEqual('onCompletedJob is missing or is not a function');
                    done();
                }
            })

            it('should validate required paramater onCompletedJob as defined', async (done) => {
                try {
                    const owner = this;
                    const jobGroup = 'job_group';
                    const onProcessJob = (jobId, jobData, lastResult, jobDone) => {
                        jobDone();
                    };
                    await jobsManager.processJobGroup(owner, jobGroup, onProcessJob);
                } catch (error) {
                    expect(error.message).toEqual('onCompletedJob is missing or is not a function');
                    done();
                }
            })

            it('should return a Promise of type void', async (done) => {
                try {
                    const owner = this;
                    const jobGroup = 'job_group';
                    const onProcessJob = (jobId, jobData, lastResult, jobDone) => {
                        jobDone();
                    };

                    const onCompletedJob = (jobId, jobData) => {}
                    const result = jobsManager.processJobGroup(owner, jobGroup, onProcessJob, onCompletedJob);

                    expect(result).toBeInstanceOf(Promise);
                    result.then((result) => {
                        expect(result).toBeUndefined();
                        done();
                    })

                } catch (error) {
                    expect(error.message).toEqual('onCompletedJob is missing or is not a function');
                    done();
                }
            })
        })

        describe('subscribing to bulls queue events', () => {
            it('should subscribe to error, waiting, active, progress, paused, resumed, cleaned, drained, removed, stalled, failed and completed events', async (done) => {
                const job = {
                    id: 'job_id',
                    group: 'job_group',
                    payload: 'job_payload'
                }

                // queue a job first
                const promise = jobsManager.queueJob(job);

                const onProcessJob = (jobId, jobData, lastResult, jobDone) => {
                    jobDone();
                };
                const onCompletedJob = (jobId, jobData) => {}

                const owner = this;
                await jobsManager.processJobGroup(owner, job.group, onProcessJob, onCompletedJob);

                expect(queueInstance.on).toHaveBeenCalledWith('error', jasmine.any(Function));
                expect(queueInstance.on).toHaveBeenCalledWith('waiting', jasmine.any(Function));
                expect(queueInstance.on).toHaveBeenCalledWith('active', jasmine.any(Function));
                expect(queueInstance.on).toHaveBeenCalledWith('progress', jasmine.any(Function));
                expect(queueInstance.on).toHaveBeenCalledWith('paused', jasmine.any(Function));
                expect(queueInstance.on).toHaveBeenCalledWith('resumed', jasmine.any(Function));
                expect(queueInstance.on).toHaveBeenCalledWith('cleaned', jasmine.any(Function));
                expect(queueInstance.on).toHaveBeenCalledWith('drained', jasmine.any(Function));
                expect(queueInstance.on).toHaveBeenCalledWith('removed', jasmine.any(Function));
                expect(queueInstance.on).toHaveBeenCalledWith('stalled', jasmine.any(Function));
                expect(queueInstance.on).toHaveBeenCalledWith('failed', jasmine.any(Function));
                expect(queueInstance.on).toHaveBeenCalledWith('completed', jasmine.any(Function));

                done();
            })

            it('should call queue.process with correct params', async (done) => {
                const job = {
                    id: 'job_id',
                    group: 'job_group',
                    payload: 'job_payload'
                }

                // queue a job first
                const promise = jobsManager.queueJob(job);

                const onProcessJob = (jobId, jobData, lastResult, jobDone) => {
                    jobDone();
                };
                const onCompletedJob = (jobId, jobData) => {}

                const owner = this;
                await jobsManager.processJobGroup(owner, job.group, onProcessJob, onCompletedJob);

                expect(queueInstance.process).toHaveBeenCalledWith(1, jasmine.any(Function));
                done();
            })

            it('should call the onProcessJob and get the correct result', async (done) => {
                const job = {
                    id: 'job_id',
                    group: 'job_group',
                    payload: 'job_payload'
                }

                const jobResult = {
                    lastOffset: 1
                }

                // queue a job first
                const promise = jobsManager.queueJob(job);

                const onProcessJob = async (jobId, jobData, lastResult) => {
                    return jobResult;
                };
                const onCompletedJob = (jobId, jobData) => {}

                queueInstance.process.and.callFake(async (concurrency, cb) => {
                    const result = await cb(job);
                    expect(result).toEqual(jobResult);
                    done();
                })

                const owner = this;
                await jobsManager.processJobGroup(owner, job.group, onProcessJob, onCompletedJob);
            })

            it('should get an error as part of the done callback if there is an error in the onProcessJob callback', async (done) => {
                const job = {
                    id: 'job_id',
                    group: 'job_group',
                    payload: 'job_payload'
                }

                const jobResult = {
                    lastOffset: 1
                }

                const expectedError = new Error('an error in onProcessJob callback');
                // queue a job first
                const promise = jobsManager.queueJob(job);

                const onProcessJob = (jobId, jobData, lastResult) => {
                    throw expectedError;
                };
                const onCompletedJob = (jobId, jobData) => {}

                queueInstance.process.and.callFake(async (concurrency, cb) => {
                    try {
                        const result = await cb(job);
                    } catch(err) {
                        expect(err).toEqual(expectedError);
                        done();
                    }
                })

                const owner = this;
                await jobsManager.processJobGroup(owner, job.group, onProcessJob, onCompletedJob);
            })

            it('should call onCompletedJob when a complted event is received from bulls queue', async (done) => {
                const job = {
                    id: 'job_id',
                    group: 'job_group',
                    payload: 'job_payload'
                }

                const bullJob = {
                    id: job.id,
                    data: job.payload
                }

                const jobResult = {
                    lastOffset: 1
                }

                // queue a job first
                const promise = jobsManager.queueJob(job);

                const onProcessJob = (jobId, jobData, lastResult, jobDone) => {
                    jobDone(null, jobResult);
                };
                const onCompletedJob = (jobId, jobData) => {
                    console.log('onCompletedJob result', jobId, jobData);
                    expect(jobId).toEqual(bullJob.id);
                    expect(jobData).toEqual(bullJob.data);
                    done();
                }

                queueInstance.on.and.callFake((event, cb) => {
                    if (event == 'completed') {
                        cb(bullJob, jobResult);
                    }
                })

                const owner = this;
                await jobsManager.processJobGroup(owner, job.group, onProcessJob, onCompletedJob);
            })
            
            it('should call _getJobResult and _setJobResult on process.queue', async (done) => {
                // arrange
                const job = {
                    id: 'job_id',
                    group: 'job_group',
                    payload: 'job_payload'
                };

                const jobResult = {
                    lastOffset: 1
                };

                let processCallback = () => {};
                let completedCallback = () => {};
                queueInstance.on.and.callFake((event, cb) => {
                    if (event == 'completed') {
                        completedCallback = cb;
                    }
                });
                queueInstance.process.and.callFake((concurrency, pcb) => {
                    processCallback = pcb;
                });
                queueInstance.add.and.callFake(async (data, options) => {
                    const result = await processCallback({
                        data: data,
                        opts: options,
                        id: options.jobId
                    });
                    completedCallback(job, result);
                });

                const onProcessJob = async (jobId, jobData, lastResult) => {
                    return jobResult;
                };
                const onCompletedJob = (jobId, jobData) => {
                    // assert
                    expect(ioredis.keys).toHaveBeenCalledWith(`eventstore-projection-job:${job.id}`);
                    expect(ioredis.hmset).toHaveBeenCalled();
                    done();
                };

                const owner = this;
                await jobsManager.processJobGroup(owner, job.group, onProcessJob, onCompletedJob);
                // end arrange

                // act
                await jobsManager.queueJob(job);
            });

            it('should call _getJobResult and _setJobResult on process.queue with ioredis.keys value', async (done) => {
                // arrange
                const job = {
                    id: 'job_id',
                    group: 'job_group',
                    payload: 'job_payload'
                };

                const jobResult = {
                    lastOffset: 2
                };

                let processCallback = () => {};
                let completedCallback = () => {};
                queueInstance.on.and.callFake((event, cb) => {
                    if (event == 'completed') {
                        completedCallback = cb;
                    }
                });
                queueInstance.process.and.callFake((concurrency, pcb) => {
                    processCallback = pcb;
                });
                queueInstance.add.and.callFake(async (data, options) => {
                    const result = await processCallback({
                        data: data,
                        opts: options,
                        id: options.jobId
                    });
                    // assert
                    expect(result).toEqual(jobResult);
                    completedCallback(job, result);
                });
                ioredis.keys.and.returnValue(Promise.resolve([`eventstore-projection-job:${job.id}`]));
                ioredis.hgetall.and.returnValue(Promise.resolve({
                    lastResult: JSON.stringify({ lastOffset: 1 })
                }));

                const onProcessJob = async (jobId, jobData, lastResult) => {
                    return jobResult;
                };
                const onCompletedJob = (jobId, jobData) => {
                    // assert
                    expect(ioredis.keys).toHaveBeenCalledWith(`eventstore-projection-job:${job.id}`);
                    expect(ioredis.hgetall).toHaveBeenCalled();
                    expect(ioredis.hmset).toHaveBeenCalled();
                    done();
                };

                const owner = this;
                await jobsManager.processJobGroup(owner, job.group, onProcessJob, onCompletedJob);
                // end arrange

                // act
                await jobsManager.queueJob(job);
            });


            it('should call _getJobResult and _setJobResult on process.queue with last result null', async (done) => {
                // arrange
                const job = {
                    id: 'job_id',
                    group: 'job_group',
                    payload: 'job_payload'
                };

                const jobResult = {
                    lastOffset: 2
                };

                let processCallback = () => {};
                let completedCallback = () => {};
                queueInstance.on.and.callFake((event, cb) => {
                    if (event == 'completed') {
                        completedCallback = cb;
                    }
                });
                queueInstance.process.and.callFake((concurrency, pcb) => {
                    processCallback = pcb;
                });
                queueInstance.add.and.callFake(async (data, options) => {
                    const result = await processCallback({
                        data: data,
                        opts: options,
                        id: options.jobId
                    });
                    // assert
                    expect(result).toEqual(jobResult);
                    completedCallback(job, result);
                });
                ioredis.keys.and.returnValue(Promise.resolve([`eventstore-projection-job:${job.id}`]));
                ioredis.hgetall.and.returnValue(Promise.resolve({
                    lastResult: null
                }));

                const onProcessJob = async (jobId, jobData, lastResult) => {
                    return jobResult;
                };
                const onCompletedJob = (jobId, jobData) => {
                    // assert
                    expect(ioredis.keys).toHaveBeenCalledWith(`eventstore-projection-job:${job.id}`);
                    expect(ioredis.hgetall).toHaveBeenCalled();
                    expect(ioredis.hmset).toHaveBeenCalled();
                    done();
                };

                const owner = this;
                await jobsManager.processJobGroup(owner, job.group, onProcessJob, onCompletedJob);
                // end arrange

                // act
                await jobsManager.queueJob(job);
            });
        });
    });
});