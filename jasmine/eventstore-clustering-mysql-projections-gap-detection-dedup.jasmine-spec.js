const compose = require('docker-compose');
const path = require('path');
const debug = require('debug')('eventstore:clustering:single');
const mysql2 = require('mysql2/promise');
const clusteredEs = require('../clustered');
const Bluebird = require('bluebird');
const shortid = require('shortid');
const _ = require('lodash');
const Redis = require('ioredis');
const OffsetManager = require('../lib/offsetManager');

const testTimeout = 60000;
const redisConfig = {
    host: 'localhost',
    port: 6379
}

const mysqlConfig = {
    host: 'localhost',
    port: 3306,
    user: 'root',
    password: 'root',
    database: 'eventstore',
    connectionPoolLimit: 10
}

const mysqlConfig2 = {
    host: 'localhost',
    port: 3307,
    user: 'root',
    password: 'root',
    database: 'eventstore',
    connectionPoolLimit: 10
}

const eventstoreConfig = {
    pollingTimeout: 500
}

const retryInterval = 1000;
describe('Gap Detection and Deduplication', () => {
    const sleep = async function(timeout) {
        debug('sleeping for ', timeout);
        return new Promise((resolve) => {
            setTimeout(resolve, timeout);
        });
    }

    const redisFactory = function() {
        const options = redisConfig;
        return {
            createClient: function(type) {
                switch (type) {
                    case 'client':
                    return new Redis(options);
                    case 'bclient':
                    return new Redis(options); // always create a new one
                    case 'subscriber':
                    return new Redis(options);
                    default:
                    return new Redis(options);
                }
            }
        };
    };

    beforeAll(async () => {
        await compose.upAll({
            cwd: path.join(__dirname),
            callback: (chunk) => {
                debug('compose in progres: ', chunk.toString())
            }
        });

        // await sleep(10000);
        console.log('waiting');

        let mysqlConnectCounter = 0;
        let redisConnectCounter = 0;
        const connectCounterThreshold = 30;
        while (mysqlConnectCounter < connectCounterThreshold) {
            try {
                const mysqlConnection = await mysql2.createConnection({
                    host: 'localhost',
                    port: '3306',
                    user: 'root',
                    password: 'root'
                });
                await mysqlConnection.connect();
                await mysqlConnection.end();

                const mysqlConnection2 = await mysql2.createConnection({
                    host: 'localhost',
                    port: '3307',
                    user: 'root',
                    password: 'root'
                });
                await mysqlConnection2.connect();
                await mysqlConnection2.end();

                break;
            } catch (error) {
                console.log(`cannot connect to mysql. sleeping for ${retryInterval}ms`);
                mysqlConnectCounter++;
                await sleep(retryInterval);
            }
        }

        const _redisConnect = async function() {
            return new Promise((resolve, reject) => {
                let redisConn = new Redis({
                    host: redisConfig.host,
                    port: redisConfig.port
                });
                redisConn.get('probeForReadyKey', (err) => {
                    if (err) {
                        reject(err);
                    } else {
                        redisConn.quit();
                        resolve();
                    }
                });
            })
        };
        
        if (mysqlConnectCounter == connectCounterThreshold) {
            throw new Error('cannot connect to mysql');
        }

        console.log('successfully connected to mysql');

        while (redisConnectCounter < connectCounterThreshold) {
            try {
                await _redisConnect();
                break;
            } catch (error) {
                console.log(`cannot connect to redis. sleeping for ${retryInterval}ms`);
                redisConnectCounter++;
                await sleep(retryInterval);
            }
        }

        if (redisConnectCounter == connectCounterThreshold) {
            throw new Error('cannot connect to mysql');
        }

        console.log('successfully connected to redis');
        await sleep(5000);
    }, testTimeout);

    afterAll(async () => {
        debug('docker compose down started');
        await compose.down({
            cwd: path.join(__dirname)
        });
        debug('docker compose down finished');
    }, testTimeout);

    describe('Gap Detection', () => {
        let clusteredEventstore;
        beforeEach(async function() {
            try {
                const config = {
                    clusters: [{
                        type: 'mysql',
                        host: mysqlConfig.host,
                        port: mysqlConfig.port,
                        user: mysqlConfig.user,
                        password: mysqlConfig.password,
                        database: mysqlConfig.database,
                        connectionPoolLimit: mysqlConfig.connectionPoolLimit
                    }, {
                        type: 'mysql',
                        host: mysqlConfig2.host,
                        port: mysqlConfig2.port,
                        user: mysqlConfig2.user,
                        password: mysqlConfig2.password,
                        database: mysqlConfig2.database,
                        connectionPoolLimit: mysqlConfig2.connectionPoolLimit
                    }],
                    partitions: 2,
                    shouldDoTaskAssignment: false,
                    // projections-specific configuration below
                    redisCreateClient: redisFactory().createClient,
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
                            min: 10,
                            max: 10
                        }
                    }, // required
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
                            min: 10,
                            max: 10
                        }
                    }, // required
                    enableProjection: true,
                    enableProjectionEventStreamBuffer: false,
                    eventCallbackTimeout: 1000,
                    lockTimeToLive: 1000,
                    pollingTimeout: eventstoreConfig.pollingTimeout, // optional,
                    pollingMaxRevisions: 100,
                    errorMaxRetryCount: 2,
                    errorRetryExponent: 2,
                    playbackEventJobCount: 10,
                    context: 'vehicle',
                    projectionGroup: shortid.generate(),
                    membershipPollingTimeout: 500
                };
                clusteredEventstore = clusteredEs(config);
                Bluebird.promisifyAll(clusteredEventstore);
                Bluebird.promisifyAll(clusteredEventstore.store);
                await clusteredEventstore.initAsync();
            } catch (error) {
                console.error('error in beforeAll', error);
            }
        }, testTimeout);

        afterEach(async function() {        
            const projections = await clusteredEventstore.getProjectionsAsync();
            for (const projection of projections) {
                const projectionId = projection.projectionId;
                await clusteredEventstore.resetProjectionAsync(projectionId);
                await clusteredEventstore.deleteProjectionAsync(projectionId);
            }

            await clusteredEventstore.store.clearAsync();

            clusteredEventstore.removeAllListeners('rebalance');
            await clusteredEventstore.closeAsync();
        }, testTimeout);

        it('should update the projection checkpoint', async function() {
            let context = `vehicle`
            const projectionConfig = {
                projectionId: context + '-projection',
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
                            year: eventPayload.year,
                            make: eventPayload.make,
                            model: eventPayload.model,
                            mileage: eventPayload.mileage
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

                        await playbackList.update(event.aggregateId, event.streamRevision, data, newData, {});
                    }
                },
                query: {
                    context: context,
                    aggregate: 'vehicle'
                },
                partitionBy: '',
                outputState: 'true',
                playbackList: {
                    name: 'vehicle_list',
                    fields: [{
                        name: 'vehicleId',
                        type: 'string'
                    }]
                }
            };

            await clusteredEventstore.projectAsync(projectionConfig);
            await clusteredEventstore.runProjectionAsync(projectionConfig.projectionId, true);
            await clusteredEventstore.startAllProjectionsAsync();

            const vehicleId = shortid.generate();
            const vehicleCreatedEvent = {
                name: "VEHICLE_CREATED",
                payload: {
                    vehicleId: vehicleId,
                    year: 2012,
                    make: "Honda",
                    model: "Jazz",
                    mileage: 1245
                }
            };

            const stream = await clusteredEventstore.getLastEventAsStreamAsync({
                context: context,
                aggregate: 'vehicle',
                aggregateId: vehicleId
            });

            Bluebird.promisifyAll(stream);

            stream.addEvent(vehicleCreatedEvent);
            const updateEventsCount = 10;
            await stream.commitAsync();
            let testModelName;
            for(let i = 0; i < updateEventsCount; i++) {
                testModelName = `model-${shortid.generate()}`;
                const event = {
                    name: "VEHICLE_UPDATED",
                    payload: {
                        vehicleId: vehicleId,
                        year: 2012,
                        make: "Honda",
                        model: testModelName,
                        mileage: 9999
                    }
                }
                stream.addEvent(event);
                await stream.commitAsync();
            }

            let pollCounter = 0;
            while (pollCounter < 10) {
                pollCounter += 1;
                debug('polling');

                const playbackList = clusteredEventstore.getPlaybackList('vehicle_list');
                const filteredResults = await playbackList.query(0, 10, null, null);
                const rows = filteredResults.rows;
                if (rows.length >= 0 && rows[0].data.model == testModelName) {
                    break;
                } else {
                    console.log(`projection has not processed yet. trying again in 1000ms`);
                    await sleep(retryInterval);
                }
            }

            const mysqlConnection = await mysql2.createConnection({
                host: 'localhost',
                port: '3306',
                user: 'root',
                password: 'root'
            });
            await mysqlConnection.connect();

            await mysqlConnection.query('USE eventstore;');
            const queryResult = await mysqlConnection.query(`
                SELECT * FROM projection_checkpoint WHERE projection_id = ?
            `, [projectionConfig.projectionId]);

            const projectionCheckpointResult = queryResult[0][0];
            await mysqlConnection.end(); 

            expect(pollCounter).toBeLessThan(10)
            expect(projectionCheckpointResult.projection_stream_version).toBe(10);
        }, testTimeout);
    }, testTimeout);


    describe('Deduplication', () => {
        let clusteredEventstore;
        beforeEach(async () => {
            try {
                const config = {
                    clusters: [{
                        type: 'mysql',
                        host: mysqlConfig.host,
                        port: mysqlConfig.port,
                        user: mysqlConfig.user,
                        password: mysqlConfig.password,
                        database: mysqlConfig.database,
                        connectionPoolLimit: mysqlConfig.connectionPoolLimit
                    }, {
                        type: 'mysql',
                        host: mysqlConfig2.host,
                        port: mysqlConfig2.port,
                        user: mysqlConfig2.user,
                        password: mysqlConfig2.password,
                        database: mysqlConfig2.database,
                        connectionPoolLimit: mysqlConfig2.connectionPoolLimit
                    }],
                    partitions: 2,
                    shouldDoTaskAssignment: false,
                    // projections-specific configuration below
                    redisCreateClient: redisFactory().createClient,
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
                            min: 10,
                            max: 10
                        }
                    }, // required
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
                            min: 10,
                            max: 10
                        }
                    }, // required
                    enableProjection: true,
                    enableProjectionEventStreamBuffer: false,
                    eventCallbackTimeout: 1000,
                    lockTimeToLive: 1000,
                    pollingTimeout: eventstoreConfig.pollingTimeout, // optional,
                    pollingMaxRevisions: 100,
                    errorMaxRetryCount: 2,
                    errorRetryExponent: 2,
                    playbackEventJobCount: 10,
                    context: 'vehicle',
                    projectionGroup: shortid.generate(),
                    membershipPollingTimeout: 500
                };
                clusteredEventstore = clusteredEs(config);
                Bluebird.promisifyAll(clusteredEventstore);
                Bluebird.promisifyAll(clusteredEventstore.store);
                await clusteredEventstore.initAsync();
            } catch (error) {
                console.error('error in beforeAll', error);
            }
        });
        afterEach(async function() {
            const projections = await clusteredEventstore.getProjectionsAsync();
            for (const projection of projections) {
                const projectionId = projection.projectionId;
                await clusteredEventstore.resetProjectionAsync(projectionId);
                await clusteredEventstore.deleteProjectionAsync(projectionId);
            }
    
            await clusteredEventstore.store.clearAsync();
    
            clusteredEventstore.removeAllListeners('rebalance');
            await clusteredEventstore.closeAsync();
            // console.log('AFTER EACH DONE');
        }, testTimeout);
    
        // TODO: Review intentions of test
        xit('should be able to detect gaps', async function() {
            let context = `vehicle`
            const projectionConfig = {
                projectionId: context + '-projection',
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
                            year: eventPayload.year,
                            make: eventPayload.make,
                            model: eventPayload.model,
                            mileage: eventPayload.mileage
                        };
                        await playbackList.add(event.aggregateId, event.streamRevision, data, {});
                    },
                    VEHICLE_CLEARED: async function(state, event, funcs) {
                        const eventPayload = event.payload.payload;
                        const playbackList = await funcs.getPlaybackList('vehicle_list');
    
                        const oldData = await playbackList.get(event.aggregateId);
                        const data = oldData && oldData.data ? oldData.data : {};
    
                        const newData = {
                            vehicleId: eventPayload.vehicleId,
                            timesRun: (oldData ? oldData.timesRun || 0 : 0) + 1
                        };
    
                        await playbackList.update(event.aggregateId, event.streamRevision, data, newData, {});
                    }
                },
                query: {
                    context: context,
                    aggregate: 'vehicle'
                },
                partitionBy: '',
                outputState: 'true',
                playbackList: {
                    name: 'vehicle_list',
                    fields: [{
                        name: 'vehicleId',
                        type: 'string'
                    }]
                }
            };
            
            const vehicleId = shortid.generate();
    
            const mysqlConnection = await mysql2.createConnection({
                host: 'localhost',
                port: '3306',
                user: 'root',
                password: 'root'
            });
            await mysqlConnection.connect();
            await mysqlConnection.query('USE eventstore;');
            await mysqlConnection.query(`
                INSERT INTO events (event_id, aggregate_id, aggregate, context, payload, commit_stamp, stream_revision, partition_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
            `, ['c64e545f-5c36-4088-ad2a-dbd20a110a110', vehicleId, 'vehicle', 'vehicle', '{"name":"VEHICLE_CREATED","payload":{"vehicleId":"aerqRHne2u","year":2012,"make":"Honda","model":"Jazz","mileage":1245},"context":"vehicle","aggregate":"vehicle","aggregateId":"aerqRHne2u"}', '1646639234802', '0', '1']);
    
            await mysqlConnection.query(`
                INSERT INTO events (event_id, aggregate_id, aggregate, context, payload, commit_stamp, stream_revision, partition_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
            `, ['c9c2094b-25eb-4665-8670-e9677788d7060', vehicleId, 'vehicle', 'vehicle', `{"name":"VEHICLE_CLEARED","payload":{"vehicleId":"aerqRHne2u"},"context":"vehicle","aggregate":"vehicle","aggregateId":"aerqRHne2u"}`, '1646639234802', '1', '1']);

            await mysqlConnection.query(`
                INSERT INTO events (event_id, aggregate_id, aggregate, context, payload, commit_stamp, stream_revision, partition_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
            `, ['c9c2094b-25eb-4665-8670-e9677788d7061', vehicleId, 'vehicle', 'vehicle', `{"name":"VEHICLE_CLEARED","payload":{"vehicleId":"aerqRHne2u"},"context":"vehicle","aggregate":"vehicle","aggregateId":"aerqRHne2u"}`, '16466392348023', '2', '1']);
    
        //     await mysqlConnection.query(`
        //     INSERT INTO projection_checkpoint (projection_id, context, aggregate, aggregate_id, projection_stream_version) VALUES (?, ?, ?, ?, ?);
        // `, ['vehicle-projection', 'vehicle', 'vehicle', vehicleId, 0]);
    
    
            await clusteredEventstore.projectAsync(projectionConfig);
            await clusteredEventstore.runProjectionAsync(projectionConfig.projectionId, true);
            await clusteredEventstore.startAllProjectionsAsync();
    
            let rows;
            let pollCounter = 0;
            while (pollCounter < 10) {
                pollCounter += 1;
                debug('polling');
    
                const playbackList = clusteredEventstore.getPlaybackList('vehicle_list');
                const filteredResults = await playbackList.query(0, 10, null, null);
                rows = filteredResults.rows;
                if (pollCounter > 5) {
                    break;
                } else {
                    console.log(`Waiting for a while before checking`);
                    await sleep(retryInterval);
                }
            }
    
            await mysqlConnection.query('USE eventstore;');
            const queryResult = await mysqlConnection.query(`
                SELECT * FROM projection_checkpoint WHERE projection_id = ?
            `, [projectionConfig.projectionId]);
    
            const projectionCheckpointResult = queryResult[0][0];
            await mysqlConnection.end(); 
            expect(projectionCheckpointResult.projection_stream_version).toBe(1);
            expect(rows[0].data.timesRun).toBe(1);
        }, testTimeout);
       
    }, testTimeout);
    

}, testTimeout);
