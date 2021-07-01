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
    connectionPoolLimit: 30
}

const redisConfig = {
    host: 'localhost',
    port: 6379
}

const RedisFactory = function() {
    const options = redisConfig;
    const redisClient = new Redis(options);
    const redisSubscriber = new Redis(options);
    const blockingClients = [];
    const otherClients = [];

    this.createClient = function(type) {
        switch (type) {
            case 'client':
                return redisClient;
            case 'bclient':
                const bRedis = new Redis(options);
                blockingClients.push(bRedis);
                return bRedis;
            case 'subscriber':
                return redisSubscriber;
            default:
                const redis = new Redis(options);
                otherClients.push(redis);
                return redis;
        }
    };

    this.destroy = async function() {
        await redisClient.quit();
        await redisSubscriber.quit();
        for (let i = 0; i < blockingClients.length; i++) {
            const redis = blockingClients[i];
            await redis.quit();
        }

        for (let i = 0; i < otherClients.length; i++) {
            const redis = otherClients[i];
            await redis.quit();
        }
    };
};

let redisFactory;

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
                        // Binds: [`/home/ryan/code/df_pocs/151_zbench/conf.d:/etc/mysql/conf.d`],
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

                                var osu = require('node-os-utils');

                                const cpuInfo = await osu.cpu.usage();

                                b.addStat('CPU', cpuInfo);

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
                    Image: 'redis:5.0',
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
                eventstoreSetup = require('@saperiuminc/eventstore')({
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

            await Promise.all([
                setupMysql(),
                setupRedis()
            ]);

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
            b.clearStats();
            await mysqlContainer.stop();
            await mysqlContainer.remove();
            await redisContainer.stop();
            await redisContainer.remove();
            debug('teardown once done')
            done();
        } catch (error) {
            console.error('error in teardownOnce', error);
            done(error);
        }
    });

    let eventstore;
    let redisFactory;
    let projectionConfig;
    let projectionConfigurations = [];
    z.setup(async (done, b) => {
        try {
            if (!redisFactory) {
                redisFactory = new RedisFactory();
            }

            eventstore = require('../../index')({
                type: 'mysql',
                host: mysqlConfig.host,
                port: mysqlConfig.port,
                user: mysqlConfig.user,
                password: mysqlConfig.password,
                database: mysqlConfig.database,
                connectionPoolLimit: mysqlConfig.connectionPoolLimit,
                // projections-specific configuration below
                redisCreateClient: redisFactory.createClient,
                listStore: {
                    type: 'inmemory'
                }, // required
                projectionStore: {
                    type: 'inmemory'
                }, // required
                enableProjection: true,
                eventCallbackTimeout: 1000,
                lockTimeToLive: 1000,
                pollingTimeout: 1000, // optional,
                pollingMaxRevisions: 10,
                errorMaxRetryCount: 2,
                errorRetryExponent: 2,
                playbackEventJobCount: 10,
                context: 'vehicle'
            });

            Bluebird.promisifyAll(eventstore);

            await eventstore.initAsync();

            // call and override private functions. just temp shortcuts
            eventstore._getProjectionState = async function() {
                // return empty object
                return {};
            }

            eventstore.registerFunction('emit', async function(targetQuery, event, done) {
                // NOTE: just be careful here as this might be an indirect infinite loop if the source of the emit also handles the 
                // the same event that it emitted

                const doEmit = async function() {
                    for (let i = 0; i < projectionConfigurations.length; i++) {
                        const projectionConfig = projectionConfigurations[i];
    
                        if (projectionConfig.query.aggregate == targetQuery.aggregate && projectionConfig.query.context == targetQuery.context) {
                            const eventToPlayback = {
                                id: 1,
                                context: targetQuery.context,
                                payload: event,
                                commitId: nanoid(),
                                position: 0,
                                streamId: targetQuery.aggregateId,
                                aggregate: targetQuery.aggregate,
                                aggregateId: targetQuery.aggregateId,
                                commitStamp: Date.now(),
                                commitSequence: 0,
                                streamRevision: 0,
                                restInCommitStream: 0,
                                eventSequence: 0
                            }
        
                            await eventstore._playbackEvent(eventToPlayback, projectionConfig, 100);
                        }
                        
                    }
                }
                
                if (done) {
                    doEmit().then(done).catch(done);
                } else {
                    return doEmit();
                }
            })

            const projectionConfig1 = {
                projectionId: `vehicle-domain-projection`,
                projectionName: `Vehicle Domain Projection`,
                playbackInterface: {
                    $init: function() {
                        return {
                            count: 0
                        }
                    },
                    vehicle_created: async function(state, event, funcs) {
                        // console.time('vehicle_created');
                        const eventPayload = event.payload.payload;
                        const stateList = await funcs.getStateList(`vehicle_domain_state_list`);

                        const newStateData = {
                            vehicleId: eventPayload.vehicleId,
                            year: eventPayload.year,
                            make: eventPayload.make,
                            model: eventPayload.model,
                            mileage: eventPayload.mileage
                        }

                        await stateList.push(newStateData);

                        const filters = [{
                            field: 'vehicleId',
                            operator: 'is',
                            value: eventPayload.vehicleId
                        }];

                        await Promise.all[
                            stateList.find(filters),
                            stateList.find(filters),
                            stateList.find(filters),
                            stateList.find(filters)
                        ];

                        const targetQuery = {
                            context: 'vehicle',
                            aggregate: 'vehicleListItem',
                            aggregateId: eventPayload.vehicleId
                        };
        
                        const newEvent = {
                            name: 'vehicle_list_item_created',
                            payload: eventPayload
                        };
        
                        await funcs.emit(targetQuery, newEvent);
                    }
                },
                query: {
                    context: 'vehicle',
                    aggregate: 'vehicle'
                },
                partitionBy: '',
                outputState: 'true',
                stateList: [{
                    name: `vehicle_domain_state_list`,
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
                }]
            };

            const projectionConfig2 = {
                projectionId: `vehicle-list-projection`,
                projectionName: `Vehicle List Projection`,
                playbackInterface: {
                    $init: function() {
                        return {
                            count: 0
                        }
                    },
                    vehicle_list_item_created: async function(state, event, funcs) {
                        // console.time('vehicle_list_item_created');
                        const eventPayload = event.payload.payload;
                        const playbackList = await funcs.getPlaybackList('vehicle_list');

                        await playbackList.add(event.aggregateId, event.streamRevision, eventPayload, {});
                    }
                },
                query: {
                    context: 'vehicle',
                    aggregate: 'vehicleListItem'
                },
                partitionBy: '',
                outputState: 'false',
                playbackList: {
                    name: `vehicle_list`,
                    fields: [{
                        name: 'vehicleId',
                        type: 'string'
                    }]
                }
            };

            projectionConfigurations.push(projectionConfig1);
            projectionConfigurations.push(projectionConfig2);

            for (let i = 0; i < projectionConfigurations.length; i++) {
                const projectionConfig = projectionConfigurations[i];

                const projection = {
                    configuration: projectionConfig
                }
                await eventstore._initStateList(projection);
                await eventstore._initPlaybackList(projection);
            }

            debug('setup complete');
            done();
        } catch (error) {
            console.error('error in setup', error);
            done(error);
        }
    });
    z.teardown(async (done) => {
        console.log('number of keys in state list', eventstore._stateListStore._lists['vehicle_domain_state_list'].length);
        debug('TEARDOWN complete');
        done();
    });

    let autoIncrementId = 0;
    z.test('bench manual playback of events', async (b) => {
        try {
            b.start();
            const vehicleId = nanoid();
            const event = {
                id: autoIncrementId++,
                context: 'vehicle',
                payload: {
                    name: "vehicle_created",
                    payload: {
                        vehicleId: vehicleId,
                        year: 2012,
                        make: "Honda",
                        model: "Jazz",
                        mileage: 1245
                    }
                },
                commitId: nanoid(),
                position: 0,
                streamId: vehicleId,
                aggregate: 'vehicle',
                aggregateId: vehicleId,
                commitStamp: Date.now(),
                commitSequence: 0,
                streamRevision: 0,
                restInCommitStream: 0,
                eventSequence: 0
            }

            for (let i = 0; i < projectionConfigurations.length; i++) {
                const projectionConfig = projectionConfigurations[i];
                if (projectionConfig.query.aggregate == event.aggregate && projectionConfig.query.context == event.context) {
                    await eventstore._playbackEvent(event, projectionConfig, 100);
                }
            }

            b.end();
        } catch (error) {
            console.error('error in bench', error);
            b.end(error);
        }

    })
});