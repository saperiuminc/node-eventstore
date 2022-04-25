const compose = require('docker-compose');
const path = require('path');
const debug = require('debug')('eventstore:clustering:single');
const mysql2 = require('mysql2/promise');
const clusteredEs = require('../clustered');
const Bluebird = require('bluebird');
const shortid = require('shortid');
const _ = require('lodash');
const Redis = require('ioredis');
const { nanoid } = require('nanoid');

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
describe('Single Concurrency -- eventstore clustering mysql projection tests', () => {
    const sleep = function(timeout) {
        debug('sleeping for ', timeout);
        return new Promise((resolve) => {
            setTimeout(resolve, timeout);
        })
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

        let connectCounter = 0;
        const connectCounterThreshold = 30;
        while (connectCounter < connectCounterThreshold) {
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
                debug(`cannot connect to mysql. sleeping for ${retryInterval}ms`);
                connectCounter++;
                await sleep(retryInterval);
            }
        }

        if (connectCounter == connectCounterThreshold) {
            throw new Error('cannot connect to mysql');
        }

        debug('successfully connected to mysql');
    });

    afterAll(async () => {
        debug('docker compose down started');
        await compose.down({
            cwd: path.join(__dirname)
        });
        debug('docker compose down finished');
    });

    describe('handling surge', () => {
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
                    }, 
                    // {
                    //     type: 'mysql',
                    //     host: mysqlConfig2.host,
                    //     port: mysqlConfig2.port,
                    //     user: mysqlConfig2.user,
                    //     password: mysqlConfig2.password,
                    //     database: mysqlConfig2.database,
                    //     connectionPoolLimit: mysqlConfig2.connectionPoolLimit
                    // }
                    ],
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
        }, 60000);

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
        }, 60000);

        it('should update the playbacklist data for multiple events', async function() {
            let context = `${nanoid()}`
            const projectionConfig = {
                projectionId: context,
                projectionName: 'Vehicle Surge Listing',
                playbackInterface: {
                    $init: function() {
                        return {
                            count: 0
                        }
                    },
                    VEHICLE_CREATED: async function(state, event, funcs) {
                        const playbackList = await funcs.getPlaybackList('vehicle_list_surge');
                        const eventPayload = event.payload.payload;
                        const data = {
                            vehicleId: eventPayload.vehicleId,
                            year: eventPayload.year,
                            make: eventPayload.make,
                            model: eventPayload.model,
                            mileage: eventPayload.mileage
                        };
                        // console.log('VEHICLE CREATE', event.aggregateId);
                        await playbackList.add(event.aggregateId, event.streamRevision, data, {});
                    },
                    VEHICLE_UPDATED: async function(state, event, funcs) {
                        const eventPayload = event.payload.payload;
                        const playbackList = await funcs.getPlaybackList('vehicle_list_surge');

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
                    }
                },
                query: {
                    context: context,
                    aggregate: 'vehicle'
                },
                partitionBy: '',
                outputState: 'true',
                playbackList: {
                    name: 'vehicle_list_surge',
                    fields: [{
                        name: 'vehicleId',
                        type: 'string'
                    }, {
                        name: 'make',
                        type: 'string'
                    }]
                }
            };

            let hasRebalanced = false;
            clusteredEventstore.on('rebalance', function(updatedAssignments) {
                for(const projectionTaskId of updatedAssignments) {
                    if (projectionTaskId.startsWith(projectionConfig.projectionId)) {
                        hasRebalanced = true;
                    }
                }
            });
            await clusteredEventstore.projectAsync(projectionConfig);
            await clusteredEventstore.runProjectionAsync(projectionConfig.projectionId, false);
            await clusteredEventstore.startAllProjectionsAsync();

            let pollCounter = 0;
            while (pollCounter < 5) {
                pollCounter += 1;
                if (hasRebalanced) {
                    // console.log(`clustered-es has rebalanced with the proper projection id. continuing with test`, projectionConfig.projectionId);
                    break;
                } else {
                    // console.log(`clustered-es has not yet rebalanced with the proper projection id. trying again in 1000ms`, projectionConfig.projectionId);
                    await sleep(retryInterval);
                }
            }
            expect(pollCounter).toBeLessThan(5);

            const emit = async function(id) {
                const vehicleId = `${nanoid()}-${id}`;

                const stream = await clusteredEventstore.getLastEventAsStreamAsync({
                    context: context,
                    aggregate: 'vehicle',
                    aggregateId: vehicleId
                });

                Bluebird.promisifyAll(stream);

                const event = {
                    name: "VEHICLE_CREATED",
                    payload: {
                        vehicleId: vehicleId,
                        year: 2012,
                        make: "Toyota",
                        model: "Corolla",
                        mileage: 1245
                    }
                }
                stream.addEvent(event);

                await stream.commitAsync();
                const event2 = {
                    name: "VEHICLE_UPDATED",
                    payload: {
                        vehicleId: vehicleId,
                        year: 2012,
                        make: "Honda",
                        model: "Jazz",
                        mileage: 9999
                    }
                }
                stream.addEvent(event2);

                await stream.commitAsync();
            }

            const produceEvents = async function() {
                const date1 = new Date('2022-04-12T03:00:00');
                const date2 = new Date('2022-04-12T03:05:00');
                const date3 = new Date('2022-04-12T03:03:00');
                const date4 = new Date('2022-04-12T03:06:00');

                try {

                    jasmine.clock().install();
                    jasmine.clock().mockDate(date1);
                    await emit(1);
                    jasmine.clock().uninstall();
                    console.log('event 1');
                    await sleep(500);

                    jasmine.clock().install();
                    jasmine.clock().mockDate(date2);
                    await emit(2);
                    jasmine.clock().uninstall();
                    console.log('event 2');
                    await sleep(500);

                    jasmine.clock().install();
                    jasmine.clock().mockDate(date3);
                    await emit(3);
                    jasmine.clock().uninstall();
                    console.log('event 3');
                    await sleep(500);

                    jasmine.clock().install();
                    jasmine.clock().mockDate(date4);
                    await emit(4);
                    jasmine.clock().uninstall();
                    console.log('event 4');
                    await sleep(500);


                } catch (ex) {
                    console.log('ex', ex)
                }

            }

            await produceEvents();
            pollCounter = 0;
            let result = null;
            while (pollCounter < 10) {
                pollCounter += 1;
                console.log('polling');

                const playbackList = clusteredEventstore.getPlaybackList('vehicle_list_surge');
                const filteredResults = await playbackList.query(0, 2000, [{
                    field: 'make',
                    operator: 'is',
                    value: 'Honda'
                }], null);

                result = filteredResults.rows;

                console.log('result', result.length);

                if (result && result.length === 4) {
                    break;
                } else {
                    console.log('Current length:', result.length);
                    debug(`projection has not processed yet. trying again in 1000ms`);
                    console.log('sleeping for ', retryInterval)
                    await sleep(retryInterval);
                }
            }
            expect(pollCounter).toBeLessThan(10);
            expect(result.length).toEqual(4);
            await sleep(30000);
        }, 50000);
    });
});
