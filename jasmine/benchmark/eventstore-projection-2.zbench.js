const zbench = require('@saperiuminc/zbench');
const Docker = require('dockerode');
const docker = new Docker();
const mysql = require('mysql');
const debug = require('debug')('DEBUG');
const Bluebird = require('bluebird');
const _ = require('lodash');
const nanoid = require('nanoid');
const Redis = require('ioredis');
const async = require('async');
const fs = require('fs/promises');

const sleep = function(timeout) {
    return new Promise((resolve) => {
        setTimeout(resolve, timeout);
    })
}

const mysqlConfig = {
    host: 'localhost',
    port: '3306',
    user: 'root',
    password: 'root',
    database: 'eventstore',
    connectionPoolLimit: 50
}

const redisConfig = {
    host: 'localhost',
    port: 6379
}

const seedFileName = 'vehicle-created-events.csv';
const numberOfVehicles = _.isNaN(process.env.NUM_VEHICLES) ? 1000000 : _.parseInt(process.env.NUM_VEHICLES);

// NOTE: remove done callback when Promise returned callback is implemented
zbench('bench eventstore-projection', (z) => {
    let mysqlContainer;
    let mysqlConnection;
    let redisContainer;
    let redisConnection;
    let eventstoreSetup;
    z.setupOnce(async (done, b) => {
        try {
            const setupMysql = async function() {
                mysqlContainer = await docker.createContainer({
                    Image: 'mysql:5.7.32',
                    Tty: true,
                    Cmd: '--default-authentication-plugin=mysql_native_password',
                    Env: [
                        `MYSQL_ROOT_PASSWORD=${mysqlConfig.password}`,
                        `MYSQL_DATABASE=${mysqlConfig.database}`
                    ],
                    HostConfig: {
                        Binds: [`${__dirname}/conf.d:/etc/mysql/conf.d`],
                        PortBindings: {
                            '3306/tcp': [{
                                HostPort: mysqlConfig.port
                            }]
                        }
                    },
                    name: 'local-mysql'
                });

                await mysqlContainer.start();

                const retryInterval = 1000;
                let connectCounter = 0;
                const retryLimit = 20;
                while (connectCounter < retryLimit) {
                    try {
                        mysqlConnection = mysql.createConnection({
                            user: mysqlConfig.user,
                            password: mysqlConfig.password
                        });
                        Bluebird.promisifyAll(mysqlConnection);
                        await mysqlConnection.connectAsync();
                        break;
                    } catch (error) {

                        debug(`cannot connect to mysql. sleeping for ${retryInterval}ms`);
                        connectCounter++;
                        await sleep(retryInterval);
                    }
                }

                if (connectCounter == retryLimit) {
                    throw new Error('cannot connect to mysql');
                }

                const pollForStats = async function() {
                    let prevInserts = 0;
                    let prevUpdates = 0;
                    let prevMultiUpdates = 0;
                    let prevDeletes = 0;
                    let prevMultiDeletes = 0;
                    let prevSelects = 0;
                    let continuePolling = true;
                    while (continuePolling) {
                        try {
                            if (mysqlConnection) {
                                const connectionCountResult = await mysqlConnection.queryAsync(`select count(*) as connectionCount from sys.processlist where DB="${mysqlConfig.database}";`);
                                b.addStat('Mysql Connections', connectionCountResult[0].connectionCount);

                                const otherStatsResult = await mysqlConnection.queryAsync('show global status where Variable_name in ("Com_insert", "Com_update", "Com_delete", "Com_select", "Com_delete_multi", "Com_update_multi");');

                                const curInserts = parseInt(otherStatsResult.find(x => x.Variable_name === 'Com_insert').Value);
                                const curUpdates = parseInt(otherStatsResult.find(x => x.Variable_name === 'Com_update').Value);
                                const curMultiUpdates = parseInt(otherStatsResult.find(x => x.Variable_name === 'Com_update_multi').Value);
                                const curDeletes = parseInt(otherStatsResult.find(x => x.Variable_name === 'Com_delete').Value);
                                const curMultiDeletes = parseInt(otherStatsResult.find(x => x.Variable_name === 'Com_delete_multi').Value);
                                const curSelects = parseInt(otherStatsResult.find(x => x.Variable_name === 'Com_select').Value);

                                b.addStat('Mysql Inserts', (curInserts - prevInserts));
                                b.addStat('Mysql Updates', (curUpdates - prevUpdates) + (curMultiUpdates - prevMultiUpdates));
                                b.addStat('Mysql Deletes', (curDeletes - prevDeletes) + (curMultiDeletes - prevMultiDeletes));
                                b.addStat('Mysql Selects', (curSelects - prevSelects));

                                prevInserts = curInserts;
                                prevUpdates = curUpdates;
                                prevMultiUpdates = curMultiUpdates;
                                prevDeletes = curDeletes;
                                prevMultiDeletes = curMultiDeletes;
                                prevSelects = curSelects;

                                await sleep(1000);
                            } else {
                                continuePolling = false;
                            }

                        } catch (error) {
                            continuePolling = false;
                        }
                    }
                }

                pollForStats();
            }

            const setupRedis = async function() {
                redisContainer = await docker.createContainer({
                    Image: 'redis:6.2',
                    Tty: true,
                    HostConfig: {
                        PortBindings: {
                            '6379/tcp': [{
                                HostPort: `${redisConfig.port}`
                            }]
                        }
                    },
                    name: 'local-redis'
                });

                await redisContainer.start();

                const retryInterval = 1000;
                let connectCounter = 0;
                const retryLimit = 20;
                while (connectCounter < retryLimit) {
                    try {
                        redisConnection = new Redis({
                            host: 'localhost',
                            port: 6379
                        });
                        await redisConnection.get('probeForReadyKey');
                        break;
                    } catch (error) {

                        debug(`cannot connect to redis. sleeping for ${retryInterval}ms`, error);
                        connectCounter++;
                        await sleep(retryInterval);
                    }
                }

                if (connectCounter == retryLimit) {
                    throw new Error('cannot connect to redis');
                }

                debug('connected to redis');

                const pollForStats = async function() {
                    let continuePolling = true;
                    while (continuePolling) {
                        try {
                            if (redisConnection) {
                                const results = await redisConnection.info();

                                const stats = results.split('\r\n').reduce((result, x) => {
                                    const pair = x.split(':');
                                    if (pair.length > 1) {
                                        result.push({
                                            name: pair[0],
                                            value: (isNaN(pair[1]) ? pair[1] : parseFloat(pair[1]))
                                        });
                                    }
                                    return result;
                                }, []);
                                // console.log('REDIS stats:', stats);
                                b.addStat('Redis Connected Clients', stats.find(x => x.name === 'connected_clients').value);
                                b.addStat('Redis Blocked Clients', stats.find(x => x.name === 'blocked_clients').value);
                                b.addStat('Redis Instantaneous OPS', stats.find(x => x.name === 'instantaneous_ops_per_sec').value);

                                await sleep(1000);
                            } else {
                                continuePolling = false;
                            }

                        } catch (error) {
                            continuePolling = false;
                        }
                    }
                }

                pollForStats();
            };

            const setupEventstore = async function() {
                eventstoreSetup = require('../../index')({
                    type: 'mysql',
                    host: mysqlConfig.host,
                    port: mysqlConfig.port,
                    user: mysqlConfig.user,
                    password: mysqlConfig.password,
                    database: mysqlConfig.database,
                    connectionPoolLimit: mysqlConfig.connectionPoolLimit
                });

                Bluebird.promisifyAll(eventstoreSetup);

                await eventstoreSetup.initAsync();
            }

            const loadTestFile = async function() {
                const sql = `
                    LOAD DATA LOCAL INFILE '${seedFileName}' INTO TABLE ${mysqlConfig.database}.events
                    FIELDS TERMINATED BY ',' 
                    ENCLOSED BY '"' 
                    LINES TERMINATED BY '\r\n'
                    (event_id, aggregate_id, aggregate, context, payload, commit_stamp, stream_revision);
                `;

                const conn = mysql.createConnection(mysqlConfig);

                Bluebird.promisifyAll(conn);

                debug('seeding test file');
                await conn.connectAsync();
                await conn.queryAsync(sql);
                await conn.endAsync();
                debug('seeding test file done')
            }

            const createTestFile = async function() {
                debug(`creating test file of ${numberOfVehicles} vehicle_created events`);
                const async = require('async');
                const q = async.queue(async (index) => {
                    const vehicleId = nanoid();
                    const data = {
                        event_id: nanoid(),
                        context: 'vehicle',
                        payload: {
                            name: "VEHICLE_CREATED",
                            payload: {
                                vehicleId: vehicleId,
                                year: 2012,
                                make: "Honda",
                                model: "Jazz",
                                mileage: 1245
                            }
                        },
                        commit_id: nanoid(),
                        position: null,
                        stream_id: vehicleId,
                        aggregate: 'vehicle',
                        aggregate_id: vehicleId,
                        commit_stamp: Date.now(),
                        commit_sequence: 0,
                        stream_revision: 0,
                        rest_in_commit_stream: 0,
                        dispatched: 1
                    }

                    await fs.appendFile(seedFileName, `"${data.event_id}","${data.aggregate_id}","${data.aggregate}","${data.context}",${JSON.stringify(JSON.stringify(data.payload))},"${data.commit_stamp}","${data.stream_revision}"\r\n`);

                }, 100);

                for (let index = 0; index < numberOfVehicles; index++) {
                    q.push(index);
                }

                const waitToDrain = async function() {
                    return new Promise((resolve) => {
                        q.drain = resolve;
                    });
                }

                await waitToDrain();
            }

            await Promise.all([
                setupMysql(),
                setupRedis()
            ]);

            await setupEventstore();

            if (!require('fs').existsSync(seedFileName)) {
                debug(`file ${seedFileName} does not exist. creating.`);
                await createTestFile();
            } else {
                debug(`file ${seedFileName} exists. skipping.`);
            }

            await loadTestFile();

            done();
        } catch (error) {
            console.error('error in setupOnce', error);
            done(error);
        }
    });

    z.teardownOnce(async (done, b) => {
        try {
            debug('teardown once start');
            mysqlConnection.destroy();
            mysqlConnection = undefined;
            await redisConnection.quit();
            redisConnection = undefined;
            // await redisFactory.destroy();
            // await eventstoreSetup.destroyAsync();
            b.clearStats();
            // await mysqlContainer.stop();
            // await mysqlContainer.remove();
            // await redisContainer.stop();
            // await redisContainer.remove();
            debug('teardown once done')
            done();
        } catch (error) {
            console.error('error in teardownOnce', error);
            done(error);
        }
    });

    let redisClient;
    let redisSubscriber;
    z.setup(async (done, b) => {
        try {
            redisClient = new Redis(redisConfig.port, redisConfig.host);
            redisSubscriber = new Redis(redisConfig.port, redisConfig.host);
            debug('setup complete');
            done();
        } catch (error) {
            console.error('error in setup', error);
            done(error);
        }
    });
    z.teardown(async (done) => {
        debug('TEARDOWN complete');
        await sleep(1000);
        // await redisClient.quit();
        // await redisSubscriber.quit();
        done();
    });

    z.test('test projections', async (b) => {
        let eventstore;

        b.hold(async () => {
            const projectionIndex = await redisClient.incr('projection-id');
            const projectionId = `vehicle-projection-${projectionIndex}`;
            const aggregate = `vehicle-aggregate-${projectionIndex}`;

            eventstore = require('../../index')({
                type: 'mysql',
                host: mysqlConfig.host,
                port: mysqlConfig.port,
                user: mysqlConfig.user,
                password: mysqlConfig.password,
                database: mysqlConfig.database,
                connectionPoolLimit: mysqlConfig.connectionPoolLimit,
                // projections-specific configuration below
                redisCreateClient: function(type) {
                    switch (type) {
                        case 'client':
                            return redisClient;
                    
                        case 'subscriber':
                            return redisSubscriber; 
                    }
                },
                listStore: {
                    connection: {
                        host: mysqlConfig.host,
                        port: mysqlConfig.port,
                        user: mysqlConfig.user,
                        password: mysqlConfig.password,
                        database: mysqlConfig.database
                    },
                    pool: {
                        min: mysqlConfig.connectionPoolLimit,
                        max: mysqlConfig.connectionPoolLimit
                    }
                }, // required
                projectionStore: {
                    connection: {
                        host: mysqlConfig.host,
                        port: mysqlConfig.port,
                        user: mysqlConfig.user,
                        password: mysqlConfig.password,
                        database: mysqlConfig.database
                    },
                    pool: {
                        min: mysqlConfig.connectionPoolLimit,
                        max: mysqlConfig.connectionPoolLimit
                    }
                }, // required
                enableProjection: true,
                eventCallbackTimeout: 1000,
                lockTimeToLive: 1000,
                pollingTimeout: 1000, // optional,
                pollingMaxRevisions: 1,
                errorMaxRetryCount: 2,
                errorRetryExponent: 2,
                playbackEventJobCount: 10,
                context: projectionId,
                projectionGroup: projectionId
            });

            Bluebird.promisifyAll(eventstore);

            await eventstore.initAsync();

            debug('initializing projections');

          
            eventstore.on('rebalance', function(assignments) {
                debug('got assignments', assignments, projectionId);
            });
            
            const projectionConfig = {
                projectionId: projectionId,
                projectionName: `Vehicle Domain ${projectionIndex} Projection`,
                playbackInterface: {
                    $init: function() {
                        return {
                            count: 0
                        }
                    },
                    VEHICLE_CREATED: async function(state, event, funcs) {
                        const vehicleId = event.aggregateId;
                        funcs.end(vehicleId);
                    }
                },
                query: {
                    context: 'vehicle',
                    aggregate: aggregate
                },
                partitionBy: '',
                outputState: 'false',
                stateList: [{
                    name: `vehicle_domain_${projectionIndex}_state_list`,
                    fields: [{
                        name: 'vehicleId',
                        type: 'string'
                    }],
                    secondaryKeys: {
                        idx_vehicleId: [{
                            name: 'vehicleId',
                            sort: 'ASC'
                        }]
                    }
                }],
                playbackList: {
                    name: `vehicle_${projectionIndex}_list`,
                    fields: [{
                        name: 'vehicleId',
                        type: 'string'
                    }]
                },
                fromOffset: 'latest'
            };

            await eventstore.projectAsync(projectionConfig);
            await eventstore.runProjectionAsync(projectionConfig.projectionId, false);

            await eventstore.startAllProjectionsAsync();

            while (eventstore) {
                const measureAsync = async function() {
                    return new Promise((resolve) => {
                        b.measure(async (m) => {
                            m.start(); 
                            const vehicleId = nanoid();

                            // const onPlaybackSuccess = async function() {
                            //     if (eventstore) {
                            //         eventstore.off('playbackSuccess', onPlaybackSuccess);
                            //         m.end();
                            //         resolve();
                            //     }
                            // }

                            // eventstore.on('playbackSuccess', onPlaybackSuccess);

                            eventstore.registerFunction('end', function(aggregateId) {
                                if (aggregateId == vehicleId) {
                                    m.end();
                                    resolve();
                                }
                            })
                            
                            const stream = await eventstore.getLastEventAsStreamAsync({
                                context: 'vehicle',
                                aggregate: aggregate,
                                aggregateId: vehicleId
                            });
                        
                            Bluebird.promisifyAll(stream);
                        
                            const event = {
                                name: "VEHICLE_CREATED",
                                payload: {
                                    vehicleId: vehicleId,
                                    year: 2012,
                                    make: "Honda",
                                    model: "Jazz",
                                    mileage: 1245
                                }
                            }
                        
                            try {
                                stream.addEvent(event);
                                await stream.commitAsync();
                            } catch (error) {
                                b.incrStat(`Duplicate inserts`);
                            }
                            
                        })
                    });
                    
                };

                await measureAsync();
            }
            
        }, async (done) => {
            // await redisFactory.destroy();
            // await eventstore.destroyAsync();
            eventstore = undefined;
            done();
        });
    })
});