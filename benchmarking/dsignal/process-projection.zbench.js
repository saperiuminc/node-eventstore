const zbench = require('@saperiuminc/zbench');
const compose = require('docker-compose');
const path = require('path');
const Redis = require('ioredis');
const mysql = require('mysql2');
const Bluebird = require('bluebird');
const { nanoid } = require('nanoid');
const murmurhash = require('murmurhash');
const dockerstats = require('dockerstats');
const clusteredES = require('../../clustered');
// const clusteredES = require('../lib/eventstore-projections/clustered-eventstore');
const _ = require('lodash');

const mysqlConfig = {
    host: process.env.mysql_HOSTNAME || 'localhost',
    port: process.env.mysql_PORT ||'13306',
    user: process.env.mysql_USER || 'root',
    password: process.env.mysql_PASS || 'root',
    database: process.env.mysql_DBNAME || 'read_model'
};

const mysqlConfig2 = {
    host: process.env.mysql2_HOSTNAME || 'localhost',
    port: process.env.mysql2_PORT ||'23306',
    user: process.env.mysql2_USER || 'root',
    password: process.env.mysql2_PASS || 'root',
    database: process.env.mysql2_DBNAME || 'eventstore'
};

const mysqlConfig3 = {
    host: process.env.mysql3_HOSTNAME || 'localhost',
    port: process.env.mysql3_PORT ||'33306',
    user: process.env.mysql3_USER || 'root',
    password: process.env.mysql3_PASS || 'root',
    database: process.env.mysql3_DBNAME || 'eventstore'
}

const NUM_SHARDS = +process.env.NUM_SHARDS || 2;
const NUM_PARTITIONS = process.env.NUM_PARTITIONS ? +process.env.NUM_PARTITIONS : 2;
const ADD_EVENT_INTERVAL = process.env.ADD_EVENT_INTERVAL || 1;
const RUN_IN_CONTAINER = process.env.RUN_IN_CONTAINER;
const PROJECTION_TYPE = process.env.PROJECTION_TYPE || 'multi';
const PROJECTION_TASKS_COUNT = process.env.PROJECTION_TASKS_COUNT || 1;

const redisConfig = {
    host: process.env.REDIS_HOST || '127.0.0.1'
}

zbench('process projection', (z) => {
    let statsTimerId;
    let redisStats;
    let redisClient;
    let redisSubscriber;
    let redisClients = [];

    let mysqlConnection;
    let mysqlConnection2;
    let mysqlConnection3;

    let es;

    let projectionTasks;
    let measureMapping;
    let modelProjectionConfig;

    let isTestDone;

    const sleep = function(timeout) {
        return new Promise((resolve) => {
            setTimeout(resolve, timeout);
        })
    };
    
    const addEvent = async function (event) {
        if (es) {
            const stream = await es.getLastEventAsStreamAsync({
                context: event.context,
                aggregate: event.aggregate,
                aggregateId: event.aggregateId
            });

            Bluebird.promisifyAll(stream);

            stream.addEvent(event);
            await stream.commitAsync();
        }
    }

    z.setupOnce(async (done, b) => {
        if (!RUN_IN_CONTAINER) {
            await compose.upAll({
                cwd: path.join(__dirname)
            });
        }

        const retryInterval = 1000;
        const retryLimit = 30;

        // read model
        let mysqlConnectCounter = 0;
        while (mysqlConnectCounter < retryLimit) {
            try {
                mysqlConnection = mysql.createConnection({
                    host: mysqlConfig.host,
                    port: mysqlConfig.port,
                    user: mysqlConfig.user,
                    password: mysqlConfig.password,
                    database: 'information_schema',
                    multipleStatements: true
                });
                Bluebird.promisifyAll(mysqlConnection);
                await mysqlConnection.connectAsync();
                break;
            } catch (error) {
                console.log(`cannot connect to mysql. sleeping for ${retryInterval}ms`);
                mysqlConnectCounter++;
                await sleep(retryInterval);
            }
        }

        if (mysqlConnectCounter == retryLimit) {
            throw new Error('cannot connect to mysql database');
        }

        // first shard
        let mysqlConnectCounter2 = 0;
        while (mysqlConnectCounter2 < retryLimit) {
            try {
                mysqlConnection2 = mysql.createConnection({
                    host: mysqlConfig2.host,
                    port: mysqlConfig2.port,
                    user: mysqlConfig2.user,
                    password: mysqlConfig2.password,
                    database: 'information_schema',
                    multipleStatements: true
                });
                Bluebird.promisifyAll(mysqlConnection2);
                await mysqlConnection2.connectAsync();
                break;
            } catch (error) {
                console.log(`cannot connect to mysql. sleeping for ${retryInterval}ms`);
                mysqlConnectCounter2++;
                await sleep(retryInterval);
            }
        }

        if (mysqlConnectCounter2 == retryLimit) {
            throw new Error('cannot connect to mysql database 2');
        }

        // second shard
        let mysqlConnectCounter3 = 0;
        while (mysqlConnectCounter3 < retryLimit) {
            try {
                mysqlConnection3 = mysql.createConnection({
                    host: mysqlConfig3.host,
                    port: mysqlConfig3.port,
                    user: mysqlConfig3.user,
                    password: mysqlConfig3.password,
                    database: 'information_schema',
                    multipleStatements: true
                });
                Bluebird.promisifyAll(mysqlConnection3);
                await mysqlConnection3.connectAsync();
                break;
            } catch (error) {
                console.log(`cannot connect to mysql. sleeping for ${retryInterval}ms`);
                mysqlConnectCounter3++;
                await sleep(retryInterval);
            }
        }

        if (mysqlConnectCounter3 == retryLimit) {
            throw new Error('cannot connect to mysql database 2');
        }

        const connStats = [mysqlConnection, mysqlConnection2, mysqlConnection3];

        redisStats = new Redis(redisConfig);

        let prevConnections = -1;
        let prevInserts = -1;
        let prevUpdates = -1;
        let prevMultiUpdates = -1;
        let prevDeletes = -1;
        let prevMultiDeletes = -1;
        let prevSelects = -1;

        statsTimerId = setInterval(async () => {
            for (let i = 0; i < 3; i++) {
                const connection = connStats[i];

                let query = `select count(*) as connectionCount from processlist where DB="${ i === 0 ? 'read_model' : 'eventstore'}";` +
                'show global status where Variable_name in ("Connections", "Com_insert", "Com_update", "Com_delete", "Com_select", "Com_delete_multi", "Com_update_multi", "Slow_queries", "Threads_connected", "Threads_running");' +
                'show global variables where Variable_name in ("max_connections");';

                if (i === 0) {
                    query = query + 'SELECT COUNT(1) FROM read_model.vehicle_list;';
                }

                if (i === 1 || (NUM_SHARDS != 1 && i === 2)) {
                    query = query + 'SELECT COUNT(1) FROM eventstore.events;';
                }

                connection.query(query, (err, results) => {
                    if (err) {
                        console.error('Connection Stats Error', err);
                    }

                    const rconns = results[0];
                    const rstatus = results[1];
                    const rvars = results[2];
                    const reventsorvehicles = results[3];

                    const curConnections = parseInt(rstatus.find(x => x.Variable_name === 'Connections').Value);
                    const curInserts = parseInt(rstatus.find(x => x.Variable_name === 'Com_insert').Value);
                    const curUpdates = parseInt(rstatus.find(x => x.Variable_name === 'Com_update').Value);
                    const curMultiUpdates = parseInt(rstatus.find(x => x.Variable_name === 'Com_update_multi').Value);
                    const curDeletes = parseInt(rstatus.find(x => x.Variable_name === 'Com_delete').Value);
                    const curMultiDeletes = parseInt(rstatus.find(x => x.Variable_name === 'Com_delete_multi').Value);
                    const curSelects = parseInt(rstatus.find(x => x.Variable_name === 'Com_select').Value);
                    const slowQueries = parseInt(rstatus.find(x => x.Variable_name === 'Slow_queries').Value);
                    const threadsConnected = parseInt(rstatus.find(x => x.Variable_name === 'Threads_connected').Value);
                    const threadsRunning = parseInt(rstatus.find(x => x.Variable_name === 'Threads_running').Value);
                    
                    const maxConnections = parseInt(rvars.find(x => x.Variable_name === 'max_connections').Value);

                    b.addStat(`${ i === 0 ? 'Read Model' : 'Shard'+i } Mysql Processes`, rconns[0].connectionCount);
                    b.addStat(`${ i === 0 ? 'Read Model' : 'Shard'+i } Mysql Conns Made`, prevConnections == -1 ? curConnections : (curConnections - prevConnections));
                    b.addStat(`${ i === 0 ? 'Read Model' : 'Shard'+i } Mysql Used Conns %`, 100.0 * threadsConnected / maxConnections);
                    b.addStat(`${ i === 0 ? 'Read Model' : 'Shard'+i } Mysql Conns Active`, threadsRunning);
                    b.addStat(`${ i === 0 ? 'Read Model' : 'Shard'+i } Mysql Inserts`, prevInserts == -1 ? curInserts : (curInserts - prevInserts));
                    b.addStat(`${ i === 0 ? 'Read Model' : 'Shard'+i } Mysql Updates`, prevUpdates == -1 ? (curUpdates + curMultiUpdates) : (curUpdates - prevUpdates) + (curMultiUpdates - prevMultiUpdates));
                    b.addStat(`${ i === 0 ? 'Read Model' : 'Shard'+i } Mysql Deletes`, prevDeletes == -1 ? (curDeletes + curMultiDeletes) : (curDeletes - prevDeletes) + (curMultiDeletes - prevMultiDeletes));
                    b.addStat(`${ i === 0 ? 'Read Model' : 'Shard'+i } Mysql Selects`, prevSelects == -1 ? curSelects : (curSelects - prevSelects));
                    b.addStat(`${ i === 0 ? 'Read Model' : 'Shard'+i } Mysql Slow Queries`, slowQueries);
                    b.addStat(`${ i === 0 ? 'Read Model' : 'Shard'+i } Total Mysql Inserts`, curInserts);
                    b.addStat(`${ i === 0 ? 'Read Model' : 'Shard'+i } Total Mysql Updates`, curUpdates);
                    b.addStat(`${ i === 0 ? 'Read Model' : 'Shard'+i } Total Mysql Selects`, curSelects);


                    if (reventsorvehicles) {
                        if (i === 0) {
                            b.addStat(`Read Model Vehicle Rows`, reventsorvehicles[0]['COUNT(1)']);
                        } else {
                            b.addStat(`Shard${i} Event Rows`, reventsorvehicles[0]['COUNT(1)']);
                        }
                    }
            
                    prevConnections = curConnections;
                    prevInserts = curInserts;
                    prevUpdates = curUpdates;
                    prevMultiUpdates = curMultiUpdates;
                    prevDeletes = curDeletes;
                    prevMultiDeletes = curMultiDeletes;
                    prevSelects = curSelects;
                });
            }

            redisStats.info(/*'clients',*/ (err, results) => {
                if (!err) {
                    const stats = results.split('\r\n').reduce((result, x) => {
                        const pair = x.split(':');
                        if (pair.length > 1) {
                            result.push({ name: pair[0], value: (isNaN(pair[1]) ? pair[1] : parseFloat(pair[1])) });
                        }
                        return result;
                    }, []);
                    b.addStat('Connected Clients', stats.find(x => x.name === 'connected_clients').value);
                    b.addStat('Blocked Clients', stats.find(x => x.name === 'blocked_clients').value);
                    b.addStat('Instantaneous OPS', stats.find(x => x.name === 'instantaneous_ops_per_sec').value);
                    b.addStat('Used CPU Sys', stats.find(x => x.name === 'used_cpu_sys').value);
                    b.addStat('Used CPU User', stats.find(x => x.name === 'used_cpu_user').value);
                    b.addStat('Used CPU Sys Children', stats.find(x => x.name === 'used_cpu_sys_children').value);
                    b.addStat('Used CPU User Children', stats.find(x => x.name === 'used_cpu_user_children').value);
                }
            });

            if (!RUN_IN_CONTAINER) {
                const containers = await dockerstats.dockerContainers();
    
                let redisContainerId;
    
                for (let container of containers) {
                    if (container.name === 'redis') {
                        redisContainerId = container.id;
                    }
                }
    
                const redisContainerStats = await dockerstats.dockerContainerStats(redisContainerId);
    
                if (redisContainerStats[0]) {
                    b.addStat('Redis CPU', redisContainerStats[0].cpuPercent);
                }
            }
        }, 1000);

        done();
    });

    z.teardownOnce(async (done, b) => {
        if (mysqlConnection) {
            mysqlConnection.destroy();
            mysqlConnection = undefined;
        }

        if (mysqlConnection2) {
            mysqlConnection2.destroy();
            mysqlConnection2 = undefined;
        }

        if (redisStats) {
            redisStats.disconnect();
            redisStats = undefined;
        }

        if (statsTimerId) {
            clearInterval(statsTimerId);
            statsTimerId = undefined;
        }

        b.clearStats();

        if (!RUN_IN_CONTAINER) {
            await compose.down({
                cwd: path.join(__dirname)
            });
        }
        
        done();
    });

    z.setup('process projection', async (done, b) => {
        redisClient = new Redis(redisConfig);
        redisSubscriber = new Redis(redisConfig);
        const createRedisClient = function(type) {
            switch (type) {
                case 'client':
                    return redisClient;
                case 'bclient': {
                    const client = new Redis(redisConfig);
                    redisClients.push(client);
                    return client;
                }
                case 'subscriber':
                    return redisSubscriber;
                default: {
                    const client = new Redis(redisConfig);
                    redisClients.push(client);
                    return client;
                }
            }
        }

        es = clusteredES({
            clusters: NUM_SHARDS === 1 ? [
                {
                    type: 'mysql',
                    host: mysqlConfig2.host,
                    port: mysqlConfig2.port,
                    user: mysqlConfig2.user,
                    password: mysqlConfig2.password,
                    database: mysqlConfig2.database,
                    connectionPoolLimit: 5
                }
            ] : [
                {
                    type: 'mysql',
                    host: mysqlConfig2.host,
                    port: mysqlConfig2.port,
                    user: mysqlConfig2.user,
                    password: mysqlConfig2.password,
                    database: mysqlConfig2.database,
                    connectionPoolLimit: 5
                },
                {
                    type: 'mysql',
                    host: mysqlConfig3.host,
                    port: mysqlConfig3.port,
                    user: mysqlConfig3.user,
                    password: mysqlConfig3.password,
                    database: mysqlConfig3.database,
                    connectionPoolLimit: 5
                }
            ],
            partitions: NUM_PARTITIONS,
            shouldDoTaskAssignment: false,
            redisCreateClient: createRedisClient,
            listStore: {
                type: 'mysql',
                connection: {
                    host: mysqlConfig.host,
                    port: mysqlConfig.port,
                    user: mysqlConfig.user,
                    password: mysqlConfig.password,
                    database: mysqlConfig.database
                },
                pool: {
                    min: 5,
                    max: 5
                }
            },
            projectionStore: {
                type: 'mysql',
                connection: {
                    host: mysqlConfig.host,
                    port: mysqlConfig.port,
                    user: mysqlConfig.user,
                    password: mysqlConfig.password,
                    database: mysqlConfig.database
                },
                pool: {
                    min: 5,
                    max: 5
                }
            },
            enableProjection: true,
            enableProjectionEventStreamBuffer: false,
            eventCallbackTimeout: 1000,
            lockTimeToLive: 1000,
            pollingTimeout: 500,
            pollingMaxRevisions: 100,
            errorMaxRetryCount: 2,
            errorRetryExponent: 2,
            playbackEventJobCount: 10,
            context: 'vehicle',
            projectionGroup: 'vehicle',
            membershipPollingTimeout: 500
        });

        Bluebird.promisifyAll(es);
        Bluebird.promisifyAll(es.store);
        await es.initAsync();

        projectionTasks = [];
        measureMapping = {};

        es.registerFunction('getMeasureMapping', () => {
            return measureMapping;
        })

        es.registerFunction('deleteMeasureMapping', (measureKey) => {
            delete measureMapping[measureKey];
        })

        es.registerFunction('addProjectionTask', (projectionTask) => {
            projectionTasks.push(projectionTask)
        })

        let projectionConfigs = [];

        // projection configuration
        modelProjectionConfig = {
            projectionId: 'vehicle',
            projectionName: 'Vehicle Listing',
            playbackInterface: {
                $init: function() {
                    return {
                        count: 0
                    }
                },
                VEHICLE_CREATED: async function(state, event, funcs) {
                    const playbackList = await funcs.getPlaybackList('vehicle_list');
                    const eventPayload = event.payload.payload;
                    const data = {
                        vehicleId: eventPayload.vehicleId,
                        vin: eventPayload.vin,
                        year: eventPayload.year,
                        make: eventPayload.make,
                        model: eventPayload.model
                    };

                    await playbackList.add(event.aggregateId, event.streamRevision, data, {});
                },
                VEHICLE_UPDATED: async function(state, event, funcs) {
                    const eventPayload = event.payload.payload;
                    const playbackList = await funcs.getPlaybackList('vehicle_list');

                    const oldData = await playbackList.get(event.aggregateId);
                    const data = oldData && oldData.data ? oldData.data : {};

                    const newData = {
                        vehicleId: eventPayload.vehicleId,
                        year: eventPayload.year,
                        make: eventPayload.make,
                        model: eventPayload.model,
                        mileage: eventPayload.mileage
                    };

                    // console.log('VEHICLE UPDATE', event.aggregateId);
                    await playbackList.update(event.aggregateId, event.streamRevision, data, newData, {});

                    const measureMapping = funcs.getMeasureMapping();
                    const m = measureMapping[event.aggregateId];

                    if (m) {
                        m.end();
                        // funcs.deleteMeasureMapping(event.aggregateId);
                    } else {
                        console.error('m not found');
                    }
                }
            },
            query: {
                context: 'vehicle'
            },
            partitionBy: PROJECTION_TYPE === 'multi' ? 'stream' : '',
            outputState: 'true',
            playbackList: {
                name: 'vehicle_list',
                fields: [{
                    name: 'vehicleId',
                    type: 'string'
                }]
            }
        };

        if (PROJECTION_TYPE === 'single') {
            for (let i = 0; i < PROJECTION_TASKS_COUNT; i++) {
                const projectionTaskConfig = _.cloneDeep(modelProjectionConfig);
                projectionTaskConfig.projectionId = `${projectionTaskConfig.projectionId}-${i}`

                projectionConfigs.push(projectionTaskConfig);
            }
        } else {
            projectionConfigs = [modelProjectionConfig];
        }

        es.on('rebalance', function(updatedAssignments) {
            console.log('got updated assignments...', updatedAssignments);
            projectionTasks = updatedAssignments ? updatedAssignments : [];
        });

        for (const projectionConfig of projectionConfigs) {
            await es.projectAsync(projectionConfig);
            await es.runProjectionAsync(projectionConfig.projectionId, false);
        }

        await es.startAllProjectionsAsync();

        done();
    });

    z.teardown('process projection', async (done) => {
        isTestDone = true;

        if (es) {
            es.removeAllListeners('rebalance');
            await es.closeAsync();
            es = undefined;
        }

        redisClient.disconnect();
        redisClient = undefined;
        redisSubscriber.disconnect();
        redisSubscriber = undefined;

        for (const redisClient of redisClients) {
            redisClient.disconnect();
        }
        redisClients = [];
        done();
    });

    z.test('process projection', async (b) => {
        b.hold(async function() {    
            while(!isTestDone) {
                const measure = function() {
                    return new Promise((resolve) => {
                        b.measure(async (m) => {
                            let projectionTask;

                            if (PROJECTION_TYPE === 'multi') {
                                const rand = Math.floor(Math.random() * NUM_SHARDS * NUM_PARTITIONS);
                                projectionTask = projectionTasks[rand];
                            } else {
                                const rand = Math.floor(Math.random() * PROJECTION_TASKS_COUNT);
                                projectionTask = projectionTasks[rand];
                            }

                            if (projectionTask) {
                                let aggregateId;
                                let shard;
                                let partition;

                                if (PROJECTION_TYPE === 'multi') {
                                    const projectionShard = +projectionTask.split('shard')[1][0];
                                    const projectionPartition = +projectionTask.split('partition')[1][0];

                                    while (shard != projectionShard || partition != projectionPartition) {
                                        aggregateId = nanoid();
                                        shard = murmurhash(aggregateId, 1) % NUM_SHARDS;
                                        partition = murmurhash(aggregateId, 2) % NUM_PARTITIONS;
                                    }
                                } else {
                                    aggregateId = nanoid();
                                }

                                const vehicle = {
                                    vehicleId: aggregateId,
                                    vin: 'thisisatestvin1234567890',
                                    year: '2022',
                                    make: 'Mitsubushi',
                                    model: 'Montero'
                                };
                    
                                const event1 = {
                                    payload: vehicle,
                                    name: 'VEHICLE_CREATED',
                                    context: 'vehicle',
                                    aggregate: 'vehicle',
                                    aggregateId: aggregateId
                                };

                                const event2 = {
                                    payload: {
                                        vehicleId: aggregateId,
                                        vin: 'thisisatestvin1234567890123',
                                        year: '2018',
                                        make: 'Honda',
                                        model: 'Civic'
                                    },
                                    name: 'VEHICLE_UPDATED',
                                    context: 'vehicle',
                                    aggregate: 'vehicle',
                                    aggregateId: aggregateId
                                };
                                
    
                                if (PROJECTION_TYPE === 'multi' || (PROJECTION_TYPE === 'single' && projectionTask.split('-')[1] === '0')) {
                                    measureMapping[aggregateId] = m;
                                    m.start();
                                    await addEvent(event1);
                                    await addEvent(event2);
                                }
                            }

                            await sleep(ADD_EVENT_INTERVAL);
                            return resolve();
                        })
                    })
                }
                
                await measure();
            }
        })
    });
});