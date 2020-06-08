const DistributedLock = require('../lib/eventstore-projections/distributed-lock');


describe('distributed-lock tests', () => {
    // just instantiating for vscode jsdoc intellisense
    let distributedLock = new DistributedLock();
    let options;
    let redis;
    let redidsLock;
    beforeEach(() => {
        redidsLock = jasmine.createSpyObj('redisLock', ['unlock']);
        redis = jasmine.createSpyObj('redis', ['lock']);
        redis.lock.and.callFake((key, ttl, cb) => {
            cb(null, redidsLock);
        })

        redidsLock.unlock.and.callFake((cb) => {
            cb();
        });

        options = {
            redis: redis,
            lockTimeToLive: 1000,
            acquireLockTimeout: 1000,
            releaseLockTimeout: 1000
        };

        distributedLock = new DistributedLock(options);
    });

    describe('lock', () => {
        describe('validating params and output', () => {
            it('should validate required paramater key', async(done) => {
                try {
                    const token = await distributedLock.lock();
                } catch (error) {
                    expect(error.message).toEqual('key is required');
                    done();
                }
            })

            it('should return a Promise', async(done) => {
                const key = 'the_key';
                const token = distributedLock.lock(key);
                expect(token).toBeInstanceOf(Promise);
                done();
            })

            it('should return a Promise of type string', async(done) => {
                const key = 'the_key';
                const token = await distributedLock.lock(key);
                expect(token).toBeInstanceOf(String);
                expect(token).toBeTruthy();
                done();
            })
        })

        describe('calling external redis library', () => {
            it('should call redis.lock with correct params', async(done) => {
                const key = 'the_key';
                const token = await distributedLock.lock(key);
                expect(redis.lock).toHaveBeenCalledWith(key, options.lockTimeToLive, jasmine.any(Function));
                done();
            })

            it('should throw error if redis library has an error', async(done) => {
                redis.lock.and.callFake((key, ttl, cb) => {
                    cb(new Error('some error in the library'));
                })

                try {
                    const key = 'the_key';
                    const token = await distributedLock.lock(key);
                } catch (error) {
                    expect(error).toBeInstanceOf(Error);
                    done();
                }
            })

            it('should have an timeout if redis.lock did not resolve in time', async(done) => {
                redis.lock.and.callFake((key, ttl, cb) => {
                    // do not resolve
                })
                distributedLock.options.acquireLockTimeout = 0;
                try {
                    const key = 'the_key';
                    const token = await distributedLock.lock(key);
                } catch (error) {
                    expect(error).toBeInstanceOf(Error);
                    expect(error.message).toEqual('timeout in _lock');
                    done();
                }
            })
        })
    });

    describe('unlock', () => {
        describe('validating params and output', () => {
            it('should validate the required param lockToken', async(done) => {
                try {
                    const token = await distributedLock.unlock();
                } catch (error) {
                    expect(error.message).toEqual('lockToken is required');
                    done();
                }
            });

            it('should return a Promise of void', async(done) => {
                const lockToken = 'the_lock_token';
                const promise = distributedLock.unlock(lockToken);
                expect(promise).toBeInstanceOf(Promise);

                const result = await promise;
                expect(result).toBeUndefined();
                done();
            })
        });

        describe('calling the external redis library for unlock', () => {
            it('should call redisLock.unlock', async(done) => {
                // do lock first
                const lockKey = 'the_key';
                const lockToken = await distributedLock.lock(lockKey);

                // then call unlock
                await distributedLock.unlock(lockToken);

                expect(redidsLock.unlock).toHaveBeenCalledTimes(1);
                done();
            });

            it('should resolve if lockToken is not found', async(done) => {
                await distributedLock.unlock('some_token');

                expect(redidsLock.unlock).toHaveBeenCalledTimes(0);
                done();
            });

            it('should throw error if there is an error in redis external librarys unlock', async(done) => {
                redidsLock.unlock.and.callFake((cb) => {
                    cb(new Error('some error in unlock'));
                });

                try {
                    // do lock first
                    const lockKey = 'the_key';
                    const lockToken = await distributedLock.lock(lockKey);

                    // then call unlock
                    await distributedLock.unlock(lockToken);

                } catch (error) {
                    expect(error.message).toEqual('some error in unlock');
                    done();
                }
            });

            it('should throw timeout error if redislock.unlock did not resolve in time', async(done) => {
                redidsLock.unlock.and.callFake((cb) => {
                    // do not resolve, let it time out
                });

                try {
                    distributedLock.options.releaseLockTimeout = 0;
                    // do lock first
                    const lockKey = 'the_key';
                    const lockToken = await distributedLock.lock(lockKey);

                    // then call unlock
                    await distributedLock.unlock(lockToken);

                } catch (error) {
                    expect(error.message).toEqual('timeout in _unlock');
                    done();
                }
            });
        });
    })

})