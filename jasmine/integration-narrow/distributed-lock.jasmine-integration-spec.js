const shortid = require('shortid');
const Redis = require('ioredis');

const sleepAsync = function(sleepUntil) {
    return new Promise((resolve) => {
        setTimeout(resolve, sleepUntil);
    });
};

const redisConfig = {
    host: 'localhost',
    port: 16379
}

const containerName = 'i_narrow_dist_lock_redis';

const redisServer = (function() {
    

    const exec = require('child_process').exec;

    return {
        up: async function() {
            const isUp = await this._isUp();
            if (!isUp) {
                const command = `docker run --name ${containerName} -p ${redisConfig.port}:6379 -d redis`;
                const process = exec(command);

                // wait until process has exited
                console.log('   env: redisAuctionServer: downloading redis image or creating a container. waiting for child process to return an exit code');
                do {
                    await sleepAsync(1000);
                } while (process.exitCode == null);

                console.log('   env: redisAuctionServer: child process exited with exit code: ', process.exitCode);

                console.info('   env: redisAuctionServer: waiting for redis to start...');

                let redis = null;
                try {
                    await new Promise((resolve) => {
                        redis = new Redis(redisConfig);

                        redis.on('connect', () => {
                            console.log('   env: redisAuctionServer: redis connected!');
                            resolve();
                        });
                    });
                    console.log('   env: redisAuctionServer: successfully connected to redis');
                } catch (error) {
                    console.error('   env: redisAuctionServer: given up connecting to redis');
                    console.error('   env: redisAuctionServer: abandoning tests');
                }
            }
        },
        down: async function() {
            exec(`docker rm ${containerName} --force`);
        },
        clear: async function() {
            let redis = null;
            try {
                await new Promise((resolve) => {
                    redis = new Redis(redisConfig);

                    redis.on('connect', () => {
                        redis.flushall(resolve);
                    });
                });
            } catch (error) {
                console.error('   env: redisAuctionServer: redis clear error', error);
            }
        },
        _isUp: async function() {
            let redis = null;
            try {
                return await new Promise((resolve) => {
                    redis = new Redis(redisConfig);

                    redis.on('connect', () => {
                        resolve(true);
                    });

                    redis.on('error', () => {
                        resolve(false);
                    });
                });
            } catch (error) {
                return false;
            }
        }
    };
})();

const DistributedLock = require('../../lib/eventstore-projections/distributed-lock');

describe('distributed-lock tests', () => {
    describe('options validations', () => {
        it('should validate redis', (done) => {
            try {
                const lock  = new DistributedLock({});
            } catch (error) {
                expect(error.message).toEqual('redis parameter is required');
                done();
            }
        });
        
    })

    describe('distributed-lock methods', () => {
        let distributedLock;
        let options;

        beforeAll(async () => {
            await redisServer.up();
        }, 60000);
    
        beforeEach(async () => {
            options = {
                redis: new Redis(redisConfig),
                lock: {
                    driftFactor: 0.01, // time in ms
                    retryCount: 1,
                    retryDelay: 200, // time in ms
                    retryJitter: 200 // time in ms
                }
            };
    
            distributedLock = new DistributedLock(options);
        });

        describe('lock', () => {
            it('should validate key', async () => {
                try {
                    await distributedLock.lock();
                } catch (error) {
                    expect(error.message).toEqual('key is required');
                }
            });

            it('should validate lockTimeToLive', async () => {
                try {
                    await distributedLock.lock(shortid.generate());
                } catch (error) {
                    expect(error.message).toEqual('lockTimeToLive is required');
                }
            });

            it('should return a lock token', async () => {
                try {
                    const lockToken = await distributedLock.lock(shortid.generate(), 10000);
                    expect(lockToken).toBeTruthy();
                } catch (error) {
                    expect(error.message).toEqual('lockTimeToLive is required');
                }
            });

            it('should lock a resource', async () => {
                try {
                    const lockKey = shortid.generate();
                    const lockToken = await distributedLock.lock(lockKey, 10000);

                    // try to lock again. should throw error
                    await distributedLock.lock(lockKey, 10000);
                    expect(lockToken).toBeTruthy();
                } catch (error) {
                    expect(error.name).toEqual('LockError');
                }
            });
        })

        describe('unlock', () => {
            it('should validate lockToken', async () => {
                try {
                    await distributedLock.unlock();
                } catch (error) {
                    expect(error.message).toEqual('lockToken is required');
                }
            });

            it('should unlock a resource', async () => {
                const lockKey = shortid.generate();
                const lockToken = await distributedLock.lock(lockKey, 1000);

                const hasUnlocked = await distributedLock.unlock(lockToken);
                expect(hasUnlocked).toEqual(true);

                // should not throw an error
                await distributedLock.lock(lockKey, 1000);
            });
        })

        describe('extend', () => {
            it('should validate lockToken', async () => {
                try {
                    await distributedLock.extend();
                } catch (error) {
                    expect(error.message).toEqual('lockToken is required');
                }
            });

            it('should validate lockTimeToLive', async () => {
                try {
                    await distributedLock.extend('abc');
                } catch (error) {
                    expect(error.message).toEqual('lockTimeToLive is required');
                }
            });

            it('should extend the lock of a resource', async () => {
                try {
                    const lockKey = shortid.generate();
                    const lockToken = await distributedLock.lock(lockKey, 100);

                    const hasExtended = await distributedLock.extend(lockToken, 200);
                    expect(hasExtended).toEqual(true);

                    // sleep for 150.
                    await sleepAsync(150);

                    // should throw an error
                    await distributedLock.lock(lockKey, 1000);
                } catch (error) {
                    expect(error.name).toEqual('LockError');
                }
            });
        })
    
        afterEach(async () => {
        })
    });
   

    afterAll(async () => {
        // NOTE: uncomment if we need to terminate the mysql every test
        // for now, it is okay since we are using a non-standard port (13306) and a fixed docker container name
        // not terminating will make the tests faster by around 11 secs
        // await redisServer.down();
    })
});