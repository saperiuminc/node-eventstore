const shortid = require('shortid');

const sleepAsync = function(sleepUntil) {
    return new Promise((resolve) => {
        setTimeout(resolve, sleepUntil);
    });
};

const redisConfig = {
    host: 'localhost',
    port: 26379
}

const containerName = 'i_narrow_dist_signal_redis';

const redisServer = (function() {
    const exec = require('child_process').exec;
    const Redis = require('ioredis');

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

const DistributedSignal = require('../../lib/eventstore-projections/distributed-signal');
const Redis = require('ioredis');

describe('distributed-signal tests', () => {
    describe('options validations', () => {
        it('should validate createClient', (done) => {
            try {
                new DistributedSignal({});
            } catch (error) {
                expect(error.message).toEqual('createClient should be passed as an option');
                done();
            }
        });
        
    })

    describe('distributed-signal methods', () => {
        let distributedSignal;
        let options;

        beforeAll(async () => {
            await redisServer.up();
            console.log('beforeAll done');
        }, 60000);
    
        beforeEach(async () => {
            options = {
                createClient: function(type) {
                    switch (type) {
                        default:
                            // just always create a new one
                            return new Redis(redisConfig);
                    }
                }
            };
    
            distributedSignal = new DistributedSignal(options);
        });

        describe('signaling', () => {
            it('should validate topic for waitForSignal', async () => {
                try {
                    await distributedSignal.waitForSignal();
                } catch (error) {
                    expect(error.message).toEqual('topic is required');
                }
            });

            it('should validate topic for signal', async () => {
                try {
                    await distributedSignal.signal();
                } catch (error) {
                    expect(error.message).toEqual('topic is required');
                }
            });

            it('should validate waitableId for stopWaitingForSignal', async () => {
                try {
                    await distributedSignal.stopWaitingForSignal();
                } catch (error) {
                    expect(error.message).toEqual('waitableId is required');
                }
            });


            it('should receive signal', async (testDone) => {
                const topic = shortid.generate();
                const waitable = await distributedSignal.waitForSignal(topic);

                waitable.waitOnce(function(err, done) {
                    expect(1).toEqual(1);
                    done();
                    testDone();
                });

                await distributedSignal.signal(topic);
            });

            it('should receive signal for group', async (testDone) => {
                const group = shortid.generate();
                const topic = shortid.generate();
                const waitable = await distributedSignal.waitForSignal(topic, group);

                waitable.waitOnce(function(err, done) {
                    expect(1).toEqual(1);
                    done();
                    testDone();
                });

                await distributedSignal.signal(topic, group);
            });

            it('should stop receiving signal when stopWaitingForSignal is called', async () => {
                const topic = shortid.generate();
                const waitable = await distributedSignal.waitForSignal(topic);

                let gotSignal = false;
                waitable.waitOnce(function(err, done) {
                    gotSignal = true;
                    done();
                });

                await distributedSignal.stopWaitingForSignal(waitable.id);
                await distributedSignal.signal(topic);

                await sleepAsync(100);
                expect(gotSignal).toEqual(false);
            });

            it('should stop receiving signal when stopWaitingForSignal is called for group', async () => {
                const topic = shortid.generate();
                const group = shortid.generate();
                const waitable = await distributedSignal.waitForSignal(topic, group);

                let gotSignal = false;
                waitable.waitOnce(function(err, done) {
                    gotSignal = true;
                    done();
                });

                await distributedSignal.stopWaitingForSignal(waitable.id, group);
                await distributedSignal.signal(topic, group);

                await sleepAsync(100);
                expect(gotSignal).toEqual(false);
            });

        })
    });
   

    afterAll(async () => {
        // NOTE: uncomment if we need to terminate the mysql every test
        // for now, it is okay since we are using a non-standard port (13306) and a fixed docker container name
        // not terminating will make the tests faster by around 11 secs
        await redisServer.down();

    })
});