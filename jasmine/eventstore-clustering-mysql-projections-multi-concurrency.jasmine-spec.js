const compose = require('docker-compose');
const path = require('path');
const debug = require('debug')('eventstore:clustered');
const mysql2 = require('mysql2/promise');
const clusteredEs = require('../clustered');
const Bluebird = require('bluebird');
const shortid = require('shortid');
const Redis = require('ioredis');
const _ = require('lodash');

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
describe('Multi Concurrency -- eventstore clustering mysql projection tests', () => {
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

    describe('projections - stream buffer off', () => {
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

        it('should run the projection', async function() {
            let context = `vehicle${shortid.generate()}`
            const projectionConfig = {
                projectionId: context,
                projectionName: 'Vehicle Listing',
                playbackInterface: {
                    $init: function() {
                        return {
                            count: 0
                        }
                    },
                    VEHICLE_CREATED: async function(state, event, funcs) {
                    }
                },
                query: [{
                    context: context
                }],
                partitionBy: 'stream',
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
            await clusteredEventstore.runProjectionAsync(projectionConfig.projectionId, false);
            await clusteredEventstore.startAllProjectionsAsync();

            const vehicleId = shortid.generate();
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
                    make: "Honda",
                    model: "Jazz",
                    mileage: 1245
                }
            }
            stream.addEvent(event);
            await stream.commitAsync();

            let pollCounter = 0;
            let projection;
            let projectionTasks;
            while (pollCounter < 10) {
                pollCounter += 1;
                projection = await clusteredEventstore.getProjectionAsync(projectionConfig.projectionId);
                projectionTasks = await clusteredEventstore.getProjectionTasksAsync(projectionConfig.projectionId);

                if (projection && projectionTasks.length > 0) {
                    let hasPassed = false;
                    for (const pj of projectionTasks) {
                        if (pj.processedDate && projection.state == 'running') {
                            hasPassed = true;
                        }
                    }
                    if (hasPassed) {
                        break;
                    } else {
                        debug(`projection has not processed yet. trying again in 1000ms`);
                        await sleep(retryInterval);
                    }
                } else {
                    debug(`projection has not processed yet. trying again in 1000ms`);
                    await sleep(retryInterval);
                }
            }

            expect(pollCounter).toBeLessThan(10);

            expect(projection.state).toEqual('running');
            expect(projectionTasks).toBeTruthy();
            expect(projectionTasks.length).toEqual(4);
            expect(projectionTasks[0].processedDate).toBeTruthy();
        });

        it('should reset the projection', async function() {
            let context = `vehicle${shortid.generate()}`

            const projectionConfig = {
                projectionId: context,
                projectionName: 'Vehicle Listing',
                playbackInterface: {
                    $init: function() {
                        return {
                            count: 0
                        }
                    }
                },
                query: {
                    context: context,
                    aggregate: 'vehicle'
                },
                partitionBy: 'stream',
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
            await clusteredEventstore.resetProjectionAsync(projectionConfig.projectionId);

            const projectionTasks = await clusteredEventstore.getProjectionTasksAsync(projectionConfig.projectionId);
            if (projectionTasks.length > 0) {
                for (const pj of projectionTasks) {
                    if (pj) {
                        expect(pj.offset).toEqual(null);
                    }
                }
            }

        });

        it('should delete the projection', async function() {
            let context = `vehicle${shortid.generate()}`

            const projectionConfig = {
                projectionId: context,
                projectionName: 'Vehicle Listing',
                playbackInterface: {
                    $init: function() {
                        return {
                            count: 0
                        }
                    }
                },
                query: {
                    context: context,
                    aggregate: 'vehicle'
                },
                partitionBy: 'stream',
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
            await clusteredEventstore.deleteProjectionAsync(projectionConfig.projectionId);

            const projection = await clusteredEventstore.getProjectionAsync(projectionConfig.projectionId);
            expect(projection).toEqual(null);

            const projectionTasks = await clusteredEventstore.getProjectionTasksAsync(projectionConfig.projectionId);
            expect(projectionTasks).toEqual([]);
        });

        it('should pause the projection', async function() {
            let context = `vehicle${shortid.generate()}`

            const projectionConfig = {
                projectionId: context,
                projectionName: 'Vehicle Listing',
                playbackInterface: {
                    $init: function() {
                        return {
                            count: 0
                        }
                    }
                },
                query: {
                    context: context,
                    aggregate: 'vehicle'
                },
                partitionBy: 'stream',
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
            await clusteredEventstore.pauseProjectionAsync(projectionConfig.projectionId);

            const storedProjection = await clusteredEventstore.getProjectionAsync(projectionConfig.projectionId);

            expect(storedProjection.state).toEqual('paused');
        });

        it('should set the projection to faulted and save the error data if there is an event handler error', async function() {
            let context = `vehicle${shortid.generate()}`
            const projectionConfig = {
                projectionId: context,
                projectionName: 'Vehicle Listing',
                playbackInterface: {
                    $init: function() {
                        return {
                            count: 0
                        }
                    },
                    VEHICLE_CREATED: async function(state, event, funcs) {

                    },
                    VEHICLE_UPDATED: async function(state, event, funcs) {
                        throw new Error('your fault!');
                    }
                },
                query: {
                    context: context,
                    aggregate: 'vehicle'
                },
                partitionBy: 'stream',
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
            await clusteredEventstore.runProjectionAsync(projectionConfig.projectionId, false);
            await clusteredEventstore.startAllProjectionsAsync();

            const vehicleId = shortid.generate();
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
                    make: "Honda",
                    model: "Jazz",
                    mileage: 1245
                }
            }
            const event2 = {
                name: "VEHICLE_UPDATED",
                payload: {
                    vehicleId: vehicleId,
                    year: 2012,
                    make: "Honda",
                    model: "Jazz",
                    mileage: 1245
                }
            }
            stream.addEvent(event);
            stream.addEvent(event2);
            await stream.commitAsync();

            let pollCounter = 0;
            let projection;
            let faultedProjectionTask;
            while (pollCounter < 10) {
                pollCounter += 1;
                let hasPassed = false;
                debug('polling');
                projection = await clusteredEventstore.getProjectionAsync(projectionConfig.projectionId);
                const projectionTasks = await clusteredEventstore.getProjectionTasksAsync(projectionConfig.projectionId);

                if (projection && projectionTasks.length > 0) {
                    if (projection.state == 'faulted') {
                        for (const pj of projectionTasks) {
                            if (pj.errorOffset) {
                                faultedProjectionTask = pj;
                                hasPassed = true;
                            }
                        }
                        console.log('projection.state is faulted. continuing with test');
                        break;
                    }
                    if (hasPassed) {
                        break;
                    } else {
                        console.log(`projection.state ${projection.state} is not faulted. trying again in 1000ms`);
                        await sleep(retryInterval);
                    }
                } else {
                    console.log(`projection has not processed yet. trying again in 1000ms`);
                    await sleep(retryInterval);
                }

                if (faultedProjectionTask != undefined) {
                    break;
                }
            }

            expect(pollCounter).toBeLessThan(10);
            expect(projection.state).toEqual('faulted');
            expect(faultedProjectionTask.error).toBeTruthy();
            expect(faultedProjectionTask.errorOffset).toBeTruthy();
            expect(faultedProjectionTask.errorEvent).toBeTruthy();
            const errorEvent = JSON.parse(faultedProjectionTask.errorEvent);
            expect(errorEvent.payload.name).toEqual(event2.name);
            expect(errorEvent.payload.payload).toEqual(event2.payload);
        });

        it('should add and update data to the stateList', async function() {
            let context = `vehicle${shortid.generate()}`
            const projectionConfig = {
                projectionId: context,
                projectionName: 'Vehicle Listing',
                playbackInterface: {
                    $init: function() {
                        return {
                            count: 0
                        }
                    },
                    VEHICLE_CREATED: async function(state, event, funcs) {
                        const eventPayload = event.payload.payload;
                        const data = {
                            vehicleId: eventPayload.vehicleId,
                            year: eventPayload.year,
                            make: eventPayload.make,
                            model: eventPayload.model,
                            mileage: eventPayload.mileage
                        };

                        const stateList = await funcs.getStateList('vehicle_state_list');
                        await stateList.push(data);
                    },
                    VEHICLE_UPDATED: async function(state, event, funcs) {
                        const eventPayload = event.payload.payload;

                        const stateList = await funcs.getStateList('vehicle_state_list');

                        const filters = [{
                            field: 'vehicleId',
                            operator: 'is',
                            value: event.aggregateId
                        }];

                        const row = await stateList.find(filters);

                        if (!row || !row.value) {
                            throw new Error('statelist test error. vehicle not inserted to state list');
                        } else {
                            const updatedStateData = row.value;
                            updatedStateData.year = eventPayload.year;
                            updatedStateData.make = eventPayload.make;
                            updatedStateData.model = eventPayload.model;
                            updatedStateData.mileage = eventPayload.mileage;

                            await stateList.set(row.index, updatedStateData, row.meta);

                            const playbackList = await funcs.getPlaybackList('vehicle_list');
                            await playbackList.add(event.aggregateId, event.streamRevision, updatedStateData, {});
                        }
                    }
                },
                query: {
                    context: context,
                    aggregate: 'vehicle'
                },
                partitionBy: 'stream',
                outputState: 'true',
                stateList: [{
                    name: 'vehicle_state_list',
                    fields: [{
                        name: 'vehicleId',
                        type: 'string'
                    }]
                }],
                playbackList: {
                    name: 'vehicle_list',
                    fields: [{
                        name: 'vehicleId',
                        type: 'string'
                    }]
                }
            };

            await clusteredEventstore.projectAsync(projectionConfig);
            await clusteredEventstore.runProjectionAsync(projectionConfig.projectionId, false);
            await clusteredEventstore.startAllProjectionsAsync();

            // VEHICLE 1
            const vehicleId = shortid.generate();
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
                    make: "Honda",
                    model: "Jazz",
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

            // VEHICLE 2
            const vehicleId2 = shortid.generate();
            const stream2 = await clusteredEventstore.getLastEventAsStreamAsync({
                context: context,
                aggregate: 'vehicle',
                aggregateId: vehicleId2
            });

            Bluebird.promisifyAll(stream2);

            const event11 = {
                name: "VEHICLE_CREATED",
                payload: {
                    vehicleId: vehicleId2,
                    year: 2012,
                    make: "Honda",
                    model: "Jazz",
                    mileage: 1245
                }
            }
            stream2.addEvent(event11);
            await stream2.commitAsync();

            const event22 = {
                name: "VEHICLE_UPDATED",
                payload: {
                    vehicleId: vehicleId2,
                    year: 2012,
                    make: "Honda",
                    model: "Jazz",
                    mileage: 9999
                }
            }
            stream2.addEvent(event22);
            await stream2.commitAsync();

            let pollCounter = 0;
            let result = null;
            while (pollCounter < 10) {
                pollCounter += 1;
                debug('polling');

                const playbackList = clusteredEventstore.getPlaybackList('vehicle_list');
                const filteredResults = await playbackList.query(0, 1, [{
                    field: 'vehicleId',
                    operator: 'is',
                    value: vehicleId
                }], null);

                result = filteredResults.rows[0];

                if (result && _.isEqual(result.data, event2.payload)) {
                    break;
                } else {
                    debug(`projection has not processed yet. trying again in 1000ms`);
                    await sleep(retryInterval);
                }
            }
            expect(result.data).toEqual(event2.payload);
            expect(pollCounter).toBeLessThan(10);
        });

        it('should update the playbacklist data', async function() {
            let context = `vehicle${shortid.generate()}`
            const projectionConfig = {
                projectionId: context,
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
                partitionBy: 'stream',
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
            await clusteredEventstore.runProjectionAsync(projectionConfig.projectionId, false);
            await clusteredEventstore.startAllProjectionsAsync();

            let vehicleIdForChecking;
            let event2ForChecking;
            let stream;
            for(let i = 0; i <= 10; i++) {
                const vehicleId = shortid.generate();
                if(vehicleIdForChecking == undefined) {
                    vehicleIdForChecking = vehicleId;
                }
                stream = await clusteredEventstore.getLastEventAsStreamAsync({
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
                        make: "Honda",
                        model: "Jazz",
                        mileage: 1245
                    }
                }
                stream.addEvent(event);

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

                if(event2ForChecking == undefined) {
                    event2ForChecking = event2;
                }
                stream.addEvent(event2);
                await stream.commitAsync();
            }

            let pollCounter = 0;
            let result = null;
            while (pollCounter < 10) {
                pollCounter += 1;
                debug('polling');

                const playbackList = clusteredEventstore.getPlaybackList('vehicle_list');
                const filteredResults = await playbackList.query(0, 1, [{
                    field: 'vehicleId',
                    operator: 'is',
                    value: vehicleIdForChecking
                }], null);

                result = filteredResults.rows[0];

                if (result && _.isEqual(result.data, event2ForChecking.payload)) {
                    break;
                } else {
                    debug(`projection has not processed yet. trying again in 1000ms`);
                    await sleep(retryInterval);
                }
            }
            expect(result.data).toEqual(event2ForChecking.payload);
            expect(pollCounter).toBeLessThan(10);
        });

        it('should force run the projection when there is an error', async function() {
            let context = `vehicle${shortid.generate()}`
            const projectionConfig = {
                projectionId: context,
                projectionName: 'Vehicle Listing',
                playbackInterface: {
                    $init: function() {
                        return {
                            count: 0
                        }
                    },
                    VEHICLE_CREATED: async function(state, event, funcs) {
                        const eventPayload = event.payload.payload;
                        const data = {
                            vehicleId: eventPayload.vehicleId,
                            year: eventPayload.year,
                            make: eventPayload.make,
                            model: eventPayload.model,
                            mileage: eventPayload.mileage
                        };

                        const stateList = await funcs.getStateList('vehicle_force_run_state_list');
                        await stateList.push(data);
                    },
                    VEHICLE_UPDATED_ERROR: async function(state, event, funcs) {
                        throw new Error('your fault!');
                    },
                    VEHICLE_UPDATED: async function(state, event, funcs) {
                        const eventPayload = event.payload.payload;

                        const stateList = await funcs.getStateList('vehicle_force_run_state_list');

                        const filters = [{
                            field: 'vehicleId',
                            operator: 'is',
                            value: event.aggregateId
                        }];

                        const row = await stateList.find(filters);

                        if (!row || !row.value) {
                            throw new Error('statelist test error. vehicle not inserted to state list');
                        } else {
                            const updatedStateData = row.value;
                            updatedStateData.year = eventPayload.year;
                            updatedStateData.make = eventPayload.make;
                            updatedStateData.model = eventPayload.model;
                            updatedStateData.mileage = eventPayload.mileage;

                            await stateList.set(row.index, updatedStateData, row.meta);

                            const playbackList = await funcs.getPlaybackList('vehicle_force_run_playback_list');
                            await playbackList.add(event.aggregateId, event.streamRevision, updatedStateData, {});
                        }
                    }
                },
                query: {
                    context: context,
                    aggregate: 'vehicle'
                },
                partitionBy: 'stream',
                outputState: 'true',
                stateList: [{
                    name: 'vehicle_force_run_state_list',
                    fields: [{
                        name: 'vehicleId',
                        type: 'string'
                    }]
                }],
                playbackList: {
                    name: 'vehicle_force_run_playback_list',
                    fields: [{
                        name: 'vehicleId',
                        type: 'string'
                    }]
                }
            };

            await clusteredEventstore.projectAsync(projectionConfig);
            await clusteredEventstore.runProjectionAsync(projectionConfig.projectionId, false);
            await clusteredEventstore.startAllProjectionsAsync();

            const vehicleId = shortid.generate();
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
                    make: "Honda",
                    model: "Jazz",
                    mileage: 1245
                }
            }
            const event2 = {
                name: "VEHICLE_UPDATED_ERROR",
                payload: {
                    vehicleId: vehicleId,
                    year: 2012,
                    make: "Honda",
                    model: "Jazz",
                    mileage: 1245
                }
            }
            stream.addEvent(event);
            await stream.commitAsync();
            stream.addEvent(event2);
            await stream.commitAsync();

            let pollCounter = 0;
            let projection;
            let faultedProjectionTask;
            while (pollCounter < 10) {
                pollCounter += 1;
                let hasPassed = false;
                debug('polling');
                projection = await clusteredEventstore.getProjectionAsync(projectionConfig.projectionId);
                const projectionTasks = await clusteredEventstore.getProjectionTasksAsync(projectionConfig.projectionId);

                if (projection && projectionTasks.length > 0) {
                    if (projection.state == 'faulted') {
                        for (const pj of projectionTasks) {
                            if (pj.errorOffset) {
                                faultedProjectionTask = pj;
                                hasPassed = true;
                            }
                        }
                        console.log('projection.state is faulted. continuing with test');
                        break;
                    }
                    if (hasPassed) {
                        break;
                    } else {
                        console.log(`projection.state ${projection.state} is not faulted. trying again in 1000ms`);
                        await sleep(retryInterval);
                    }
                } else {
                    console.log(`projection has not processed yet. trying again in 1000ms`);
                    await sleep(retryInterval);
                }

                if (faultedProjectionTask != undefined) {
                    break;
                }
            }

            expect(pollCounter).toBeLessThan(10);
            expect(projection.state).toEqual('faulted');
            expect(faultedProjectionTask.error).toBeTruthy();
            expect(faultedProjectionTask.errorOffset).toBeTruthy();
            expect(faultedProjectionTask.errorEvent).toBeTruthy();
            const errorEvent = JSON.parse(faultedProjectionTask.errorEvent);
            expect(errorEvent.payload.name).toEqual(event2.name);
            expect(errorEvent.payload.payload).toEqual(event2.payload);

            await clusteredEventstore.runProjectionAsync(projectionConfig.projectionId, true);

            const event3 = {
                name: "VEHICLE_UPDATED",
                payload: {
                    vehicleId: vehicleId,
                    year: 2012,
                    make: "Honda",
                    model: "Jazz",
                    mileage: 1245
                }
            }
            stream.addEvent(event3);
            await stream.commitAsync();

            pollCounter = 0;
            let result = null;
            while (pollCounter < 10) {
                pollCounter += 1;
                debug('polling');

                const playbackList = clusteredEventstore.getPlaybackList('vehicle_force_run_playback_list');
                const filteredResults = await playbackList.query(0, 1, [{
                    field: 'vehicleId',
                    operator: 'is',
                    value: vehicleId
                }], null);

                result = filteredResults.rows[0];

                if (result && _.isEqual(result.data, event3.payload)) {
                    break;
                } else {
                    debug(`projection has not processed yet. trying again in 1000ms`);
                    await sleep(retryInterval);
                }
            }
            expect(result.data).toEqual(event3.payload);
            expect(pollCounter).toBeLessThan(10);
        });

        it('should delete the playbacklist data', async function() {
            let context = `vehicle${shortid.generate()}`
            const projectionConfig = {
                projectionId: context,
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
                    VEHICLE_DELETED: async function(state, event, funcs) {
                        const playbackList = await funcs.getPlaybackList('vehicle_list');
                        await playbackList.delete(event.aggregateId);
                    }
                },
                query: {
                    context: context,
                    aggregate: 'vehicle'
                },
                partitionBy: 'stream',
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
            await clusteredEventstore.runProjectionAsync(projectionConfig.projectionId, false);
            await clusteredEventstore.startAllProjectionsAsync();

            const vehicleId = shortid.generate();
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
                    make: "Honda",
                    model: "Jazz",
                    mileage: 1245
                }
            }
            stream.addEvent(event);
            await stream.commitAsync();

            let pollCounter = 0;
            let result = null;
            while (pollCounter < 10) {
                pollCounter += 1;
                debug('polling');

                const playbackList = clusteredEventstore.getPlaybackList('vehicle_list');
                const filteredResults = await playbackList.query(0, 1, [{
                    field: 'vehicleId',
                    operator: 'is',
                    value: vehicleId
                }], null);

                result = filteredResults.rows[0];

                if (result) {
                    break;
                } else {
                    debug(`projection has not processed yet. trying again in 1000ms`);
                    await sleep(retryInterval);
                }
            }
            expect(result).toBeDefined();
            expect(pollCounter).toBeLessThan(10);

            const event2 = {
                name: "VEHICLE_DELETED",
                payload: {
                    vehicleId: vehicleId
                }
            }
            stream.addEvent(event2);
            await stream.commitAsync();

            pollCounter = 0;
            while (pollCounter < 10) {
                pollCounter += 1;
                debug('polling');

                const playbackList = clusteredEventstore.getPlaybackList('vehicle_list');
                const filteredResults = await playbackList.query(0, 1, [{
                    field: 'vehicleId',
                    operator: 'is',
                    value: vehicleId
                }], null);

                result = filteredResults.rows[0];

                if (!result) {
                    break;
                } else {
                    debug(`projection has not processed yet. trying again in 1000ms`);
                    await sleep(retryInterval);
                }
            }
            expect(result).toBeFalsy();
            expect(pollCounter).toBeLessThan(10);
        });

        it('should get data from the playbacklistview', async function() {
            let context = `vehicle${shortid.generate()}`
            const projectionConfig = {
                projectionId: context,
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
                    }
                },
                query: {
                    context: context,
                    aggregate: 'vehicle'
                },
                partitionBy: 'stream',
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
            await clusteredEventstore.runProjectionAsync(projectionConfig.projectionId, false);
            await clusteredEventstore.startAllProjectionsAsync();

            await clusteredEventstore.registerPlaybackListViewAsync('vehicle-list',
                'SELECT list.row_id, list.row_revision, list.row_json, list.meta_json FROM vehicle_list list @@where @@order @@limit',
                'SELECT COUNT(1) as total_count FROM vehicle_list list @@where;', {
                    alias: {}
                });

            const vehicleId = shortid.generate();
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
                    make: "Honda",
                    model: "Jazz",
                    mileage: 1245
                }
            }
            stream.addEvent(event);
            await stream.commitAsync();

            let pollCounter = 0;
            let result = null;
            while (pollCounter < 10) {
                pollCounter += 1;
                debug('polling');

                const playbackListView = await clusteredEventstore.getPlaybackListViewAsync('vehicle-list');
                debug('playbackListView', playbackListView);
                Bluebird.promisifyAll(playbackListView);

                const filteredResults = await playbackListView.queryAsync(0, 1, [{
                    field: 'vehicleId',
                    operator: 'is',
                    value: vehicleId
                }], null);

                debug('filteredResults', filteredResults);
                result = filteredResults.rows[0];

                if (result && _.isEqual(result.data, event.payload)) {
                    break;
                } else {
                    debug(`projection has not processed yet. trying again in 1000ms`);
                    await sleep(retryInterval);
                }
            }
            expect(result.data).toEqual(event.payload);
            expect(pollCounter).toBeLessThan(10);
        });

        it('should emit playbackError on playback error', async (done) => {
            let context = `vehicle${shortid.generate()}`
            const errorMessage = 'test-error';
            const projectionConfig = {
                projectionId: context,
                projectionName: 'Vehicle Listing',
                playbackInterface: {
                    $init: function() {
                        return {
                            count: 0
                        }
                    },
                    VEHICLE_CREATED: async function(state, event, funcs) {
                        throw new Error('test-error');
                    }
                },
                query: {
                    context: context,
                    aggregate: 'vehicle'
                },
                partitionBy: 'stream',
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
            await clusteredEventstore.runProjectionAsync(projectionConfig.projectionId, false);
            await clusteredEventstore.startAllProjectionsAsync();

            const vehicleId = shortid.generate();
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
                    make: "Honda",
                    model: "Jazz",
                    mileage: 1245
                }
            }

            const listener = (error) => {
                expect(errorMessage).toEqual(error.message);
                clusteredEventstore.off('playbackError', listener);
                done();
            };

            clusteredEventstore.on('playbackError', listener);

            stream.addEvent(event);
            await stream.commitAsync();
        });

        it('should emit playbackSuccess on playback', async (done) => {
            let context = `vehicle${shortid.generate()}`
            const errorMessage = 'test-error';
            const projectionConfig = {
                projectionId: context,
                projectionName: 'Vehicle Listing',
                playbackInterface: {
                    $init: function() {
                        return {
                            count: 0
                        }
                    },
                    VEHICLE_CREATED: async function(state, event, funcs) {

                    }
                },
                query: {
                    context: context,
                    aggregate: 'vehicle'
                },
                partitionBy: 'stream',
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
            await clusteredEventstore.runProjectionAsync(projectionConfig.projectionId, false);
            await clusteredEventstore.startAllProjectionsAsync();

            const vehicleId = shortid.generate();
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
                    make: "Honda",
                    model: "Jazz",
                    mileage: 1245
                }
            }

            const listener = (data) => {
                //NOTE: playback success runs on all projections shard partition instance hence need to determine which did run an event
                if(data.eventsCount > 0) {
                    expect(data.projectionId).toContain(projectionConfig.projectionId);
                    expect(data.eventsCount).toEqual(1);
                    clusteredEventstore.off('playbackSuccess', listener);
                    done();
                }
            };

            clusteredEventstore.on('playbackSuccess', listener);

            stream.addEvent(event);
            await stream.commitAsync();
        });

        it('should be able to process and successfully playback multiple events', async (done) => {
            let context = `vehicle${shortid.generate()}`
            const errorMessage = 'test-error';
            const projectionConfig = {
                projectionId: context,
                projectionName: 'Vehicle Listing',
                playbackInterface: {
                    $init: function() {
                        return {
                            count: 0
                        }
                    },
                    VEHICLE_CREATED: async function(state, event, funcs) {

                    }
                },
                query: {
                    context: context,
                    aggregate: 'vehicle'
                },
                partitionBy: 'stream',
                outputState: 'true',
                playbackList: {
                    name: 'vehicle_list',
                    fields: [{
                        name: 'vehicleId',
                        type: 'string'
                    }]
                },
                pollingMaxRevisions: 10,
                concurrentEventNames: ['VEHICLE_CREATED', 'VEHICLE_UPDATED'],
                concurrencyCount: 5
            };

            await clusteredEventstore.projectAsync(projectionConfig);
            await clusteredEventstore.runProjectionAsync(projectionConfig.projectionId, false);
            await clusteredEventstore.startAllProjectionsAsync();

            let eventsCount = 0;
            const listener = (data) => {
                eventsCount += data.eventsCount;
                if (eventsCount == expectedEventsCount) {
                    clusteredEventstore.off('playbackSuccess', listener);
                    done();
                }
            };

            clusteredEventstore.on('playbackSuccess', listener);

            const expectedEventsCount = 10;
            for (let i = 0; i < expectedEventsCount; i++) {

                const vehicleId = shortid.generate();
                const stream = await clusteredEventstore.getLastEventAsStreamAsync({
                    context: context,
                    aggregate: 'vehicle',
                    aggregateId: vehicleId
                });

                Bluebird.promisifyAll(stream);

                const event = {
                    name: i == expectedEventsCount - 1 ? "VEHICLE_UPDATED" : "VEHICLE_CREATED",
                    payload: {
                        vehicleId: vehicleId,
                        year: 2012,
                        make: "Honda",
                        model: "Jazz",
                        mileage: 1245
                    }
                }

                stream.addEvent(event);
                await stream.commitAsync();
            }
        });

        it('should add a projection and process all relevant events', async function() {
            let context = `vehicle${shortid.generate()}`
            const initialProjectionConfig = {
                projectionId: context,
                projectionName: 'Vehicle Listing',
                playbackInterface: {
                    $init: function() {
                        return {
                            count: 0
                        }
                    },
                    VEHICLE_CREATED: async function(state, event, funcs) {
                        const playbackList = await funcs.getPlaybackList('vehicle_list_initial_off');
                        const eventPayload = event.payload.payload;
                        const data = {
                            vehicleId: eventPayload.vehicleId,
                            year: eventPayload.year,
                            make: eventPayload.make,
                            model: eventPayload.model,
                            mileage: eventPayload.mileage
                        };
                        await playbackList.add(event.aggregateId, event.streamRevision, data, {});
                    }
                },
                query: {
                    context: context,
                    aggregate: 'vehicle'
                },
                partitionBy: 'stream',
                outputState: 'true',
                playbackList: {
                    name: 'vehicle_list_initial_off',
                    fields: [{
                        name: 'vehicleId',
                        type: 'string'
                    },{
                        name: 'make',
                        type: 'string'
                    },{
                        name: 'year',
                        type: 'int'
                    }]
                }
            };

            await clusteredEventstore.projectAsync(initialProjectionConfig);
            await clusteredEventstore.runProjectionAsync(initialProjectionConfig.projectionId, false);
            await clusteredEventstore.startAllProjectionsAsync();

            const vehicleId1 = shortid.generate();
            const vehicleId2 = shortid.generate();
            const vehicleId3 = shortid.generate();
            const stream1 = await clusteredEventstore.getLastEventAsStreamAsync({
                context: context,
                aggregate: 'vehicle',
                aggregateId: vehicleId1
            });
            const stream2 = await clusteredEventstore.getLastEventAsStreamAsync({
                context: context,
                aggregate: 'vehicle',
                aggregateId: vehicleId2
            });
            const stream3 = await clusteredEventstore.getLastEventAsStreamAsync({
                context: context,
                aggregate: 'vehicle',
                aggregateId: vehicleId3
            });

            Bluebird.promisifyAll(stream1);
            Bluebird.promisifyAll(stream2);
            Bluebird.promisifyAll(stream3);

            const initialEvents = [{
                    name: "VEHICLE_CREATED",
                    payload: {
                        vehicleId: vehicleId1,
                        year: 2012,
                        make: "Honda",
                        model: "Jazz",
                        mileage: 1245
                    }
                },
                {
                    name: "VEHICLE_CREATED",
                    payload: {
                        vehicleId: vehicleId2,
                        year: 2014,
                        make: "Honda",
                        model: "Civic",
                        mileage: 1267
                    }
                },
                {
                    name: "VEHICLE_CREATED",
                    payload: {
                        vehicleId: vehicleId3,
                        year: 2020,
                        make: "Honda",
                        model: "Accord",
                        mileage: 3456
                    }
                }
            ];

            stream1.addEvent(initialEvents[0]);
            stream2.addEvent(initialEvents[1]);
            stream3.addEvent(initialEvents[2]);
            await stream1.commitAsync();
            await stream2.commitAsync();

            let pollCounter = 0;
            let result = null;
            while (pollCounter < 10) {
                pollCounter += 1;

                const playbackList = clusteredEventstore.getPlaybackList('vehicle_list_initial_off');
                const filteredResults = await playbackList.query(0, 5, [{
                    field: 'make',
                    operator: 'is',
                    value: 'Honda'
                }], null);

                result = filteredResults.rows;

                if (result && result.length === 2) {
                    break;
                } else {
                    debug(`projection has not processed yet. trying again in 1000ms`);
                    await sleep(retryInterval);
                }
            }
            expect(result.length).toEqual(2);
            expect(pollCounter).toBeLessThan(10);

            const additionalProjectionConfig = {
                projectionId: context + '-add',
                projectionName: 'Vehicle Listing Additional',
                playbackInterface: {
                    $init: function() {
                        return {
                            count: 0
                        }
                    },
                    VEHICLE_CREATED: async function(state, event, funcs) {
                        const playbackList = await funcs.getPlaybackList('vehicle_list_additional_off');
                        const eventPayload = event.payload.payload;
                        const data = {
                            vehicleId: eventPayload.vehicleId,
                            year: eventPayload.year,
                            make: eventPayload.make,
                            model: eventPayload.model,
                            mileage: eventPayload.mileage
                        };
                        await playbackList.add(event.aggregateId, event.streamRevision, data, {});
                    }
                },
                query: {
                    context: context,
                    aggregate: 'vehicle'
                },
                partitionBy: 'stream',
                outputState: 'true',
                playbackList: {
                    name: 'vehicle_list_additional_off',
                    fields: [{
                        name: 'vehicleId',
                        type: 'string'
                    },{
                        name: 'make',
                        type: 'string'
                    },{
                        name: 'year',
                        type: 'int'
                    }]
                }
            };

            await clusteredEventstore.projectAsync(additionalProjectionConfig);
            await clusteredEventstore.runProjectionAsync(additionalProjectionConfig.projectionId, false);

            pollCounter = 0;
            result = null;
            while (pollCounter < 10) {
                pollCounter += 1;

                const playbackList = clusteredEventstore.getPlaybackList('vehicle_list_additional_off');
                const filteredResults = await playbackList.query(0, 5, [{
                    field: 'make',
                    operator: 'is',
                    value: 'Honda'
                }], [{
                    field: 'year',
                    sortDirection: 'ASC'
                }]);

                result = filteredResults.rows;

                if (result && result.length === 2) {
                    break;
                } else {
                    debug(`projection has not processed yet. trying again in 1000ms`);
                    await sleep(retryInterval);
                }
            }
            expect(result.length).toEqual(2);
            expect(result[1].data).toEqual(initialEvents[1].payload);
            expect(pollCounter).toBeLessThan(10);

            await stream3.commitAsync();
            
            pollCounter = 0;
            let initialProjectionResult = null;
            let additionalProjectionResult = null;
            while (pollCounter < 10) {
                pollCounter += 1;

                const playbackListInitial = clusteredEventstore.getPlaybackList('vehicle_list_initial_off');
                const filteredInitialResults = await playbackListInitial.query(0, 5, [{
                    field: 'make',
                    operator: 'is',
                    value: 'Honda'
                }], [{
                    field: 'year',
                    sortDirection: 'ASC'
                }]);
                const playbackListAdditional = clusteredEventstore.getPlaybackList('vehicle_list_additional_off');
                const filteredAdditionalResults = await playbackListAdditional.query(0, 5, [{
                    field: 'make',
                    operator: 'is',
                    value: 'Honda'
                }], [{
                    field: 'year',
                    sortDirection: 'ASC'
                }]);

                initialProjectionResult = filteredInitialResults.rows;
                additionalProjectionResult = filteredAdditionalResults.rows;

                if (initialProjectionResult && initialProjectionResult.length === 3 && additionalProjectionResult && additionalProjectionResult.length === 3) {
                    break;
                } else {
                    debug(`both projections have not processed yet. trying again in 1000ms`);
                    await sleep(retryInterval);
                }
            }
            expect(initialProjectionResult.length).toEqual(3);
            expect(initialProjectionResult[2].data).toEqual(initialEvents[2].payload);
            expect(additionalProjectionResult.length).toEqual(3);
            expect(additionalProjectionResult[2].data).toEqual(initialEvents[2].payload);
            expect(pollCounter).toBeLessThan(10);
        });

        it('should add a projection and process latest events only if the configured fromOffset is set to latest', async function() {
            let context = `vehicle${shortid.generate()}`
            const initialProjectionConfig = {
                projectionId: context,
                projectionName: 'Vehicle Listing',
                playbackInterface: {
                    $init: function() {
                        return {
                            count: 0
                        }
                    },
                    VEHICLE_CREATED: async function(state, event, funcs) {
                        const playbackList = await funcs.getPlaybackList('vehicle_list_initial_off_2');
                        const eventPayload = event.payload.payload;
                        const data = {
                            vehicleId: eventPayload.vehicleId,
                            year: eventPayload.year,
                            make: eventPayload.make,
                            model: eventPayload.model,
                            mileage: eventPayload.mileage
                        };
                        await playbackList.add(event.aggregateId, event.streamRevision, data, {});
                    }
                },
                query: {
                    context: context,
                    aggregate: 'vehicle'
                },
                partitionBy: 'stream',
                outputState: 'true',
                playbackList: {
                    name: 'vehicle_list_initial_off_2',
                    fields: [{
                        name: 'vehicleId',
                        type: 'string'
                    },{
                        name: 'make',
                        type: 'string'
                    }]
                }
            };

            await clusteredEventstore.projectAsync(initialProjectionConfig);
            await clusteredEventstore.runProjectionAsync(initialProjectionConfig.projectionId, false);
            await clusteredEventstore.startAllProjectionsAsync();

            const vehicleId1 = shortid.generate();
            const vehicleId2 = shortid.generate();
            const vehicleId3 = shortid.generate();
            const stream1 = await clusteredEventstore.getLastEventAsStreamAsync({
                context: context,
                aggregate: 'vehicle',
                aggregateId: vehicleId1
            });
            const stream2 = await clusteredEventstore.getLastEventAsStreamAsync({
                context: context,
                aggregate: 'vehicle',
                aggregateId: vehicleId2
            });
            const stream3 = await clusteredEventstore.getLastEventAsStreamAsync({
                context: context,
                aggregate: 'vehicle',
                aggregateId: vehicleId3
            });

            Bluebird.promisifyAll(stream1);
            Bluebird.promisifyAll(stream2);
            Bluebird.promisifyAll(stream3);

            const initialEvents = [{
                    name: "VEHICLE_CREATED",
                    payload: {
                        vehicleId: vehicleId1,
                        year: 2012,
                        make: "Honda",
                        model: "Jazz",
                        mileage: 1245
                    }
                },
                {
                    name: "VEHICLE_CREATED",
                    payload: {
                        vehicleId: vehicleId2,
                        year: 2014,
                        make: "Honda",
                        model: "Civic",
                        mileage: 1267
                    }
                },
                {
                    name: "VEHICLE_CREATED",
                    payload: {
                        vehicleId: vehicleId3,
                        year: 2020,
                        make: "Honda",
                        model: "Accord",
                        mileage: 3456
                    }
                }
            ];

            stream1.addEvent(initialEvents[0]);
            stream2.addEvent(initialEvents[1]);
            stream3.addEvent(initialEvents[2]);
            await stream1.commitAsync();
            await stream2.commitAsync();

            let pollCounter = 0;
            let result = null;
            while (pollCounter < 10) {
                pollCounter += 1;

                const playbackList = clusteredEventstore.getPlaybackList('vehicle_list_initial_off_2');
                const filteredResults = await playbackList.query(0, 5, [{
                    field: 'make',
                    operator: 'is',
                    value: 'Honda'
                }], null);

                result = filteredResults.rows;

                if (result && result.length === 2) {
                    break;
                } else {
                    debug(`projection has not processed yet. trying again in 1000ms`);
                    await sleep(retryInterval);
                }
            }
            expect(result.length).toEqual(2);
            expect(pollCounter).toBeLessThan(10);

            const projectionWithLatestOffsetConfig = {
                projectionId: context + '-2',
                projectionName: 'Vehicle Listing 2',
                playbackInterface: {
                    $init: function() {
                        return {
                            count: 0
                        }
                    },
                    VEHICLE_CREATED: async function(state, event, funcs) {
                        const playbackList = await funcs.getPlaybackList('vehicle_list_latest_off_2');
                        const eventPayload = event.payload.payload;
                        const data = {
                            vehicleId: eventPayload.vehicleId,
                            year: eventPayload.year,
                            make: eventPayload.make,
                            model: eventPayload.model,
                            mileage: eventPayload.mileage
                        };
                        await playbackList.add(event.aggregateId, event.streamRevision, data, {});
                    }
                },
                query: {
                    context: context,
                    aggregate: 'vehicle'
                },
                partitionBy: 'stream',
                outputState: 'true',
                fromOffset: 'latest',
                playbackList: {
                    name: 'vehicle_list_latest_off_2',
                    fields: [{
                        name: 'vehicleId',
                        type: 'string'
                    },{
                        name: 'make',
                        type: 'string'
                    }]
                }
            };

            await clusteredEventstore.projectAsync(projectionWithLatestOffsetConfig);
            await clusteredEventstore.runProjectionAsync(projectionWithLatestOffsetConfig.projectionId, false);

            await stream3.commitAsync();
            
            pollCounter = 0;
            result = null;
            while (pollCounter < 10) {
                pollCounter += 1;

                const playbackList = clusteredEventstore.getPlaybackList('vehicle_list_latest_off_2');
                const filteredResults = await playbackList.query(0, 5, [{
                    field: 'make',
                    operator: 'is',
                    value: 'Honda'
                }], null);

                result = filteredResults.rows;

                if (result && result.length === 1) {
                    break;
                } else {
                    debug(`projection has not processed yet. trying again in 1000ms`);
                    await sleep(retryInterval);
                }
            }
            expect(result.length).toEqual(1);
            expect(result[0].data).toEqual(initialEvents[2].payload);
            expect(pollCounter).toBeLessThan(10);
        });

        describe('querying playbacklistviews with keyset pagination support', () => {
            it('should get data from the playbacklistview with keyset pagination support - querying the next page', async function() {
                const projectionConfig = {
                    projectionId: 'keyset-list-next',
                    projectionName: 'KeySet Pagination Listing',
                    playbackInterface: {
                        $init: function() {
                            return {
                                count: 0
                            }
                        },
                        KEYSET_LIST_NEXT_CREATED: async function(state, event, funcs) {
                            const playbackList = await funcs.getPlaybackList('keyset_list_next');
                            const eventPayload = event.payload.payload;
                            const data = {
                                keysetId: eventPayload.keysetId,
                                field1: eventPayload.field1,
                                field2: eventPayload.field2,
                                field3: eventPayload.field3
                            };
                            await playbackList.add(event.aggregateId, event.streamRevision, data, {});
                        }
                    },
                    query: {
                        context: 'keyset',
                        aggregate: 'keyset'
                    },
                    partitionBy: 'stream',
                    outputState: 'true',
                    playbackList: {
                        name: 'keyset_list_next',
                        fields: [{
                            name: 'keysetId',
                            type: 'string'
                        },{
                            name: 'field1',
                            type: 'int'
                        },{
                            name: 'field2',
                            type: 'string'
                        },{
                            name: 'field3',
                            type: 'int'
                        }]
                    }
                };

                await clusteredEventstore.projectAsync(projectionConfig);
                await clusteredEventstore.runProjectionAsync(projectionConfig.projectionId, false);
                await clusteredEventstore.startAllProjectionsAsync();

                await clusteredEventstore.registerPlaybackListViewAsync('keyset-list-next',
                    ('SELECT keyset_list_next.row_id, keyset_list_next.row_revision, keyset_list_next.row_json, keyset_list_next.keysetId, keyset_list_next.field1, keyset_list_next.field2, keyset_list_next.field3' + 
                    ' FROM (SELECT keyset_list_next.row_id as rowId FROM keyset_list_next @@where @@paginationOrder @@limit) LRLT JOIN keyset_list_next ON LRLT.rowId = keyset_list_next.row_id @@order;'),
                    'SELECT COUNT(1) as total_count FROM keyset_list_next list @@where;', {
                        alias: {},
                        paginationPrimaryKeys: [
                            `keysetId`
                        ],
                    });

                const payloads = [
                    { // #1
                        field1: 5,
                        field2: 'CCCC',
                        field3: 2
                    },
                    { // #3
                        field1: 4,
                        field2: 'BBBB',
                        field3: 5
                    },
                    { // #2
                        field1: 4,
                        field2: 'BBBB',
                        field3: 3
                    },
                    { // #5
                        field1: 4,
                        field2: 'DDDD',
                        field3: 4
                    },
                    { // #4
                        field1: 4,
                        field2: 'DDDD',
                        field3: 2
                    }
                ];
                let stream;
                for (let i = 0; i < 5; i++) {
                    const keysetId = shortid.generate();
                    stream = await clusteredEventstore.getLastEventAsStreamAsync({
                        context: 'keyset',
                        aggregate: 'keyset',
                        aggregateId: keysetId
                    });
                    Bluebird.promisifyAll(stream);

                    const payload = payloads[i];
                    payload.keysetId = keysetId;

                    const event = {
                        name: "KEYSET_LIST_NEXT_CREATED",
                        payload: payload
                    };
                    stream.addEvent(event);
                    await stream.commitAsync();
                }

                let pollCounter = 0;
                let filteredResults = null;
                while (pollCounter < 10) {
                    pollCounter += 1;
                    debug('polling');

                    const playbackListView = await clusteredEventstore.getPlaybackListViewAsync('keyset-list-next');
                    Bluebird.promisifyAll(playbackListView);

                    const nextKeyArray = [payloads[2].field1, payloads[2].field2, payloads[2].field3, payloads[2].keysetId];
                    const nextKey = Buffer.from(JSON.stringify(nextKeyArray)).toString('base64');
                    filteredResults = await playbackListView.queryAsync(0, 2, null, [
                        { 
                            field: 'field1', 
                            sortDirection: 'DESC'
                        },
                        { 
                            field: 'field2', 
                            sortDirection: 'ASC'
                        },
                        { 
                            field: 'field3', 
                            sortDirection: 'ASC'
                        }
                    ], null, nextKey);

                    if (filteredResults && _.isEqual(filteredResults.count, 5)) {
                        break;
                    } else {
                        debug(`projection has not processed yet. trying again in 1000ms`);
                        await sleep(retryInterval);
                    }
                }

                expect(filteredResults.count).toEqual(5);
                expect(filteredResults.rows.length).toEqual(2);
                expect(filteredResults.rows[0].data).toEqual(payloads[1]);
                expect(filteredResults.rows[1].data).toEqual(payloads[4]);
                expect(filteredResults.previousKey).toEqual(Buffer.from(JSON.stringify([payloads[1].field1, payloads[1].field2, payloads[1].field3, payloads[1].keysetId])).toString('base64'));
                expect(filteredResults.nextKey).toEqual(Buffer.from(JSON.stringify([payloads[4].field1, payloads[4].field2, payloads[4].field3, payloads[4].keysetId])).toString('base64'));
                expect(pollCounter).toBeLessThan(10);
            });

            it('should get data from the playbacklistview with keyset pagination support - querying the previous page', async function() {
                const projectionConfig = {
                    projectionId: 'keyset-list-prev',
                    projectionName: 'KeySet Pagination Listing',
                    playbackInterface: {
                        $init: function() {
                            return {
                                count: 0
                            }
                        },
                        KEYSET_LIST_PREV_CREATED: async function(state, event, funcs) {
                            const playbackList = await funcs.getPlaybackList('keyset_list_prev');
                            const eventPayload = event.payload.payload;
                            const data = {
                                keysetId: eventPayload.keysetId,
                                field1: eventPayload.field1,
                                field2: eventPayload.field2,
                                field3: eventPayload.field3
                            };
                            await playbackList.add(event.aggregateId, event.streamRevision, data, {});
                        }
                    },
                    query: {
                        context: 'keyset',
                        aggregate: 'keyset'
                    },
                    partitionBy: 'stream',
                    outputState: 'true',
                    playbackList: {
                        name: 'keyset_list_prev',
                        fields: [{
                            name: 'keysetId',
                            type: 'string'
                        },{
                            name: 'field1',
                            type: 'int'
                        },{
                            name: 'field2',
                            type: 'string'
                        },{
                            name: 'field3',
                            type: 'int'
                        }]
                    }
                };

                await clusteredEventstore.projectAsync(projectionConfig);
                await clusteredEventstore.startAllProjectionsAsync();
                await clusteredEventstore.runProjectionAsync(projectionConfig.projectionId, false);

                await clusteredEventstore.registerPlaybackListViewAsync('keyset-list-prev',
                    ('SELECT keyset_list_prev.row_id, keyset_list_prev.row_revision, keyset_list_prev.row_json, keyset_list_prev.keysetId, keyset_list_prev.field1, keyset_list_prev.field2, keyset_list_prev.field3' + 
                    ' FROM (SELECT keyset_list_prev.row_id as rowId FROM keyset_list_prev @@where @@paginationOrder @@limit) LRLT JOIN keyset_list_prev ON LRLT.rowId = keyset_list_prev.row_id @@order;'),
                    'SELECT COUNT(1) as total_count FROM keyset_list_prev list @@where;', {
                        alias: {},
                        paginationPrimaryKeys: [
                            `keysetId`
                        ],
                    });

                const payloads = [
                    { // #1
                        field1: 5,
                        field2: 'CCCC',
                        field3: 2
                    },
                    { // #3
                        field1: 4,
                        field2: 'BBBB',
                        field3: 5
                    },
                    { // #2
                        field1: 4,
                        field2: 'BBBB',
                        field3: 3
                    },
                    { // #5
                        field1: 4,
                        field2: 'DDDD',
                        field3: 4
                    },
                    { // #4
                        field1: 4,
                        field2: 'DDDD',
                        field3: 2
                    }
                ];
                let stream;
                for (let i = 0; i < 5; i++) {
                    const keysetId = shortid.generate();
                    stream = await clusteredEventstore.getLastEventAsStreamAsync({
                        context: 'keyset',
                        aggregate: 'keyset',
                        aggregateId: keysetId
                    });
                    Bluebird.promisifyAll(stream);

                    const payload = payloads[i];
                    payload.keysetId = keysetId;

                    const event = {
                        name: "KEYSET_LIST_PREV_CREATED",
                        payload: payload
                    };
                    stream.addEvent(event);
                    await stream.commitAsync();
                }

                let pollCounter = 0;
                let filteredResults = null;
                while (pollCounter < 10) {
                    pollCounter += 1;
                    debug('polling');

                    const playbackListView = await clusteredEventstore.getPlaybackListViewAsync('keyset-list-prev');
                    Bluebird.promisifyAll(playbackListView);

                    const prevKeyArray = [payloads[3].field1, payloads[3].field2, payloads[3].field3, payloads[3].keysetId];
                    const prevKey = Buffer.from(JSON.stringify(prevKeyArray)).toString('base64');
                    filteredResults = await playbackListView.queryAsync(0, 2, null, [
                        { 
                            field: 'field1', 
                            sortDirection: 'DESC'
                        },
                        { 
                            field: 'field2', 
                            sortDirection: 'ASC'
                        },
                        { 
                            field: 'field3', 
                            sortDirection: 'ASC'
                        }
                    ], prevKey, null);

                    if (filteredResults && _.isEqual(filteredResults.count, 5)) {
                        break;
                    } else {
                        debug(`projection has not processed yet. trying again in 1000ms`);
                        await sleep(retryInterval);
                    }
                }

                expect(filteredResults.count).toEqual(5);
                expect(filteredResults.rows.length).toEqual(2);
                expect(filteredResults.rows[0].data).toEqual(payloads[1]);
                expect(filteredResults.rows[1].data).toEqual(payloads[4]);
                expect(filteredResults.previousKey).toEqual(Buffer.from(JSON.stringify([payloads[1].field1, payloads[1].field2, payloads[1].field3, payloads[1].keysetId])).toString('base64'));
                expect(filteredResults.nextKey).toEqual(Buffer.from(JSON.stringify([payloads[4].field1, payloads[4].field2, payloads[4].field3, payloads[4].keysetId])).toString('base64'));
                expect(pollCounter).toBeLessThan(10);
            });

            it('should get data from the playbacklistview with keyset pagination support - querying the last page - 1', async function() {
                const projectionConfig = {
                    projectionId: 'keyset-list-last',
                    projectionName: 'KeySet Pagination Listing',
                    playbackInterface: {
                        $init: function() {
                            return {
                                count: 0
                            }
                        },
                        KEYSET_LIST_LAST_CREATED: async function(state, event, funcs) {
                            const playbackList = await funcs.getPlaybackList('keyset_list_last');
                            const eventPayload = event.payload.payload;
                            const data = {
                                keysetId: eventPayload.keysetId,
                                field1: eventPayload.field1,
                                field2: eventPayload.field2,
                                field3: eventPayload.field3
                            };
                            await playbackList.add(event.aggregateId, event.streamRevision, data, {});
                        }
                    },
                    query: {
                        context: 'keyset',
                        aggregate: 'keyset'
                    },
                    partitionBy: 'stream',
                    outputState: 'true',
                    playbackList: {
                        name: 'keyset_list_last',
                        fields: [{
                            name: 'keysetId',
                            type: 'string'
                        },{
                            name: 'field1',
                            type: 'int'
                        },{
                            name: 'field2',
                            type: 'string'
                        },{
                            name: 'field3',
                            type: 'int'
                        }]
                    }
                };

                await clusteredEventstore.projectAsync(projectionConfig);
                await clusteredEventstore.startAllProjectionsAsync();
                await clusteredEventstore.runProjectionAsync(projectionConfig.projectionId, false);

                await clusteredEventstore.registerPlaybackListViewAsync('keyset-list-last',
                    ('SELECT keyset_list_last.row_id, keyset_list_last.row_revision, keyset_list_last.row_json, keyset_list_last.keysetId, keyset_list_last.field1, keyset_list_last.field2, keyset_list_last.field3' + 
                    ' FROM (SELECT keyset_list_last.row_id as rowId FROM keyset_list_last @@where @@paginationOrder @@limit) LRLT JOIN keyset_list_last ON LRLT.rowId = keyset_list_last.row_id @@order;'),
                    'SELECT COUNT(1) as total_count FROM keyset_list_last list @@where;', {
                        alias: {},
                        paginationPrimaryKeys: [
                            `keysetId`
                        ],
                    });

                const payloads = [
                    { // #1
                        field1: 5,
                        field2: 'CCCC',
                        field3: 2
                    },
                    { // #3
                        field1: 4,
                        field2: 'BBBB',
                        field3: 5
                    },
                    { // #2
                        field1: 4,
                        field2: 'BBBB',
                        field3: 3
                    },
                    { // #5
                        field1: 4,
                        field2: 'DDDD',
                        field3: 4
                    },
                    { // #4
                        field1: 4,
                        field2: 'DDDD',
                        field3: 2
                    }
                ];

                let stream;
                for (let i = 0; i < 5; i++) {
                    const keysetId = shortid.generate();
                    stream = await clusteredEventstore.getLastEventAsStreamAsync({
                        context: 'keyset',
                        aggregate: 'keyset',
                        aggregateId: keysetId
                    });
                    Bluebird.promisifyAll(stream);

                    const payload = payloads[i];
                    payload.keysetId = keysetId;

                    const event = {
                        name: "KEYSET_LIST_LAST_CREATED",
                        payload: payload
                    };
                    stream.addEvent(event);
                    await stream.commitAsync();
                }

                let pollCounter = 0;
                let filteredResults = null;
                while (pollCounter < 10) {
                    pollCounter += 1;
                    debug('polling');

                    const playbackListView = await clusteredEventstore.getPlaybackListViewAsync('keyset-list-last');
                    Bluebird.promisifyAll(playbackListView);

                    filteredResults = await playbackListView.queryAsync(0, 2, null, [
                        { 
                            field: 'field1', 
                            sortDirection: 'DESC'
                        },
                        { 
                            field: 'field2', 
                            sortDirection: 'ASC'
                        },
                        { 
                            field: 'field3', 
                            sortDirection: 'ASC'
                        }
                    ], null, '__LAST');

                    if (filteredResults && _.isEqual(filteredResults.count, 5)) {
                        break;
                    } else {
                        debug(`projection has not processed yet. trying again in 1000ms`);
                        await sleep(retryInterval);
                    }
                }

                expect(filteredResults.count).toEqual(5);
                expect(filteredResults.rows.length).toEqual(1);
                expect(filteredResults.rows[0].data).toEqual(payloads[3]);
                expect(filteredResults.previousKey).toEqual(Buffer.from(JSON.stringify([payloads[3].field1, payloads[3].field2, payloads[3].field3, payloads[3].keysetId])).toString('base64'));
                expect(filteredResults.nextKey).toEqual(Buffer.from(JSON.stringify([payloads[3].field1, payloads[3].field2, payloads[3].field3, payloads[3].keysetId])).toString('base64'));
                expect(pollCounter).toBeLessThan(10);
            });

            it('should get data from the playbacklistview with keyset pagination support - querying the last page - 2', async function() {
                const projectionConfig = {
                    projectionId: 'keyset-list-last-2',
                    projectionName: 'KeySet Pagination Listing',
                    playbackInterface: {
                        $init: function() {
                            return {
                                count: 0
                            }
                        },
                        KEYSET_LIST_LAST_2_CREATED: async function(state, event, funcs) {
                            const playbackList = await funcs.getPlaybackList('keyset_list_last_2');
                            const eventPayload = event.payload.payload;
                            const data = {
                                keysetId: eventPayload.keysetId,
                                field1: eventPayload.field1,
                                field2: eventPayload.field2,
                                field3: eventPayload.field3
                            };
                            await playbackList.add(event.aggregateId, event.streamRevision, data, {});
                        }
                    },
                    query: {
                        context: 'keyset',
                        aggregate: 'keyset'
                    },
                    partitionBy: 'stream',
                    outputState: 'true',
                    playbackList: {
                        name: 'keyset_list_last_2',
                        fields: [{
                            name: 'keysetId',
                            type: 'string'
                        },{
                            name: 'field1',
                            type: 'int'
                        },{
                            name: 'field2',
                            type: 'string'
                        },{
                            name: 'field3',
                            type: 'int'
                        }]
                    }
                };

                await clusteredEventstore.projectAsync(projectionConfig);
                await clusteredEventstore.startAllProjectionsAsync();
                await clusteredEventstore.runProjectionAsync(projectionConfig.projectionId, false);

                await clusteredEventstore.registerPlaybackListViewAsync('keyset-list-last-2',
                    ('SELECT keyset_list_last_2.row_id, keyset_list_last_2.row_revision, keyset_list_last_2.row_json, keyset_list_last_2.keysetId, keyset_list_last_2.field1, keyset_list_last_2.field2, keyset_list_last_2.field3' + 
                    ' FROM (SELECT keyset_list_last_2.row_id as rowId FROM keyset_list_last_2 @@where @@paginationOrder @@limit) LRLT JOIN keyset_list_last_2 ON LRLT.rowId = keyset_list_last_2.row_id @@order;'),
                    'SELECT COUNT(1) as total_count FROM keyset_list_last_2 list @@where;', {
                        alias: {},
                        paginationPrimaryKeys: [
                            `keysetId`
                        ],
                    });

                const payloads = [
                    { // #1
                        field1: 5,
                        field2: 'CCCC',
                        field3: 2
                    },
                    { // #3
                        field1: 4,
                        field2: 'BBBB',
                        field3: 5
                    },
                    { // #2
                        field1: 4,
                        field2: 'BBBB',
                        field3: 3
                    },
                    { // #5
                        field1: 4,
                        field2: 'DDDD',
                        field3: 4
                    },
                    { // #4
                        field1: 4,
                        field2: 'DDDD',
                        field3: 2
                    },
                    { // #6
                        field1: 4,
                        field2: 'DDDD',
                        field3: 5
                    }
                ];

                let stream;
                for (let i = 0; i < 6; i++) {
                    const keysetId = shortid.generate();
                    stream = await clusteredEventstore.getLastEventAsStreamAsync({
                        context: 'keyset',
                        aggregate: 'keyset',
                        aggregateId: keysetId
                    });
                    Bluebird.promisifyAll(stream);

                    const payload = payloads[i];
                    payload.keysetId = keysetId;

                    const event = {
                        name: "KEYSET_LIST_LAST_2_CREATED",
                        payload: payload
                    };
                    stream.addEvent(event);
                    await stream.commitAsync();
                }

                let pollCounter = 0;
                let filteredResults = null;
                while (pollCounter < 10) {
                    pollCounter += 1;
                    debug('polling');

                    const playbackListView = await clusteredEventstore.getPlaybackListViewAsync('keyset-list-last-2');
                    Bluebird.promisifyAll(playbackListView);

                    filteredResults = await playbackListView.queryAsync(0, 2, null, [
                        { 
                            field: 'field1', 
                            sortDirection: 'DESC'
                        },
                        { 
                            field: 'field2', 
                            sortDirection: 'ASC'
                        },
                        { 
                            field: 'field3', 
                            sortDirection: 'ASC'
                        }
                    ], null, '__LAST');

                    if (filteredResults && _.isEqual(filteredResults.count, 6)) {
                        break;
                    } else {
                        debug(`projection has not processed yet. trying again in 1000ms`);
                        await sleep(retryInterval);
                    }
                }

                expect(filteredResults.count).toEqual(6);
                expect(filteredResults.rows.length).toEqual(2);
                expect(filteredResults.rows[0].data).toEqual(payloads[3]);
                expect(filteredResults.rows[1].data).toEqual(payloads[5]);
                expect(filteredResults.previousKey).toEqual(Buffer.from(JSON.stringify([payloads[3].field1, payloads[3].field2, payloads[3].field3, payloads[3].keysetId])).toString('base64'));
                expect(filteredResults.nextKey).toEqual(Buffer.from(JSON.stringify([payloads[5].field1, payloads[5].field2, payloads[5].field3, payloads[5].keysetId])).toString('base64'));
                expect(pollCounter).toBeLessThan(10);
            });

            it('should get data from the playbacklistview with keyset pagination support - querying with null values - 1', async function() {
                const projectionConfig = {
                    projectionId: 'keyset-list-null',
                    projectionName: 'KeySet Pagination Listing - Null',
                    playbackInterface: {
                        $init: function() {
                            return {
                                count: 0
                            }
                        },
                        KEYSET_LIST_NULL_CREATED: async function(state, event, funcs) {
                            const playbackList = await funcs.getPlaybackList('keyset_list_null');
                            const eventPayload = event.payload.payload;
                            const data = {
                                keysetId: eventPayload.keysetId,
                                field1: eventPayload.field1,
                                field2: eventPayload.field2,
                                field3: eventPayload.field3
                            };
                            await playbackList.add(event.aggregateId, event.streamRevision, data, {});
                        }
                    },
                    query: {
                        context: 'keyset',
                        aggregate: 'keyset'
                    },
                    partitionBy: 'stream',
                    outputState: 'true',
                    playbackList: {
                        name: 'keyset_list_null',
                        fields: [{
                            name: 'keysetId',
                            type: 'string'
                        },{
                            name: 'field1',
                            type: 'int'
                        },{
                            name: 'field2',
                            type: 'string'
                        },{
                            name: 'field3',
                            type: 'int'
                        }]
                    }
                };

                await clusteredEventstore.projectAsync(projectionConfig);
                await clusteredEventstore.startAllProjectionsAsync();
                await clusteredEventstore.runProjectionAsync(projectionConfig.projectionId, false);

                await clusteredEventstore.registerPlaybackListViewAsync('keyset-list-null',
                    ('SELECT keyset_list_null.row_id, keyset_list_null.row_revision, keyset_list_null.row_json, keyset_list_null.keysetId, keyset_list_null.field1, keyset_list_null.field2, keyset_list_null.field3' + 
                    ' FROM (SELECT keyset_list_null.row_id as rowId FROM keyset_list_null @@where @@paginationOrder @@limit) LRLT JOIN keyset_list_null ON LRLT.rowId = keyset_list_null.row_id @@order;'),
                    'SELECT COUNT(1) as total_count FROM keyset_list_null list @@where;', {
                        alias: {},
                        paginationPrimaryKeys: [
                            `keysetId`
                        ],
                    });

                const payloads = [
                    { // #1
                        field1: 5,
                        field2: 'CCCC',
                        field3: 2
                    },
                    { // #3
                        field1: null,
                        field2: 'BBBB',
                        field3: 5
                    },
                    { // #2
                        field1: null,
                        field2: 'BBBB',
                        field3: null
                    },
                    { // #5
                        field1: null,
                        field2: 'DDDD',
                        field3: 4
                    },
                    { // #4
                        field1: null,
                        field2: 'DDDD',
                        field3: 2
                    }
                ];

                let stream;
                for (let i = 0; i < 5; i++) {
                    const keysetId = shortid.generate();
                    stream = await clusteredEventstore.getLastEventAsStreamAsync({
                        context: 'keyset',
                        aggregate: 'keyset',
                        aggregateId: keysetId
                    });
                    Bluebird.promisifyAll(stream);

                    const payload = payloads[i];
                    payload.keysetId = keysetId;

                    const event = {
                        name: "KEYSET_LIST_NULL_CREATED",
                        payload: payload
                    };
                    stream.addEvent(event);
                    await stream.commitAsync();
                }

                const sanitizedPayloads = [
                    { // #1
                        field1: 5,
                        field2: 'CCCC',
                        field3: 2,
                        keysetId: payloads[0].keysetId
                    },
                    { // #3
                        field2: 'BBBB',
                        field3: 5,
                        keysetId: payloads[1].keysetId
                    },
                    { // #2
                        field2: 'BBBB',
                        keysetId: payloads[2].keysetId
                    },
                    { // #5
                        field2: 'DDDD',
                        field3: 4,
                        keysetId: payloads[3].keysetId
                    },
                    { // #4
                        field2: 'DDDD',
                        field3: 2,
                        keysetId: payloads[4].keysetId
                    }
                ];

                let pollCounter = 0;
                let filteredResults = null;
                while (pollCounter < 10) {
                    pollCounter += 1;
                    debug('polling');

                    const playbackListView = await clusteredEventstore.getPlaybackListViewAsync('keyset-list-null');
                    Bluebird.promisifyAll(playbackListView);

                    const nextKeyArray = [payloads[2].field1, payloads[2].field2, payloads[2].field3, payloads[2].keysetId];
                    const nextKey = Buffer.from(JSON.stringify(nextKeyArray)).toString('base64');
                    filteredResults = await playbackListView.queryAsync(0, 2, null, [
                        { 
                            field: 'field1', 
                            sortDirection: 'DESC'
                        },
                        { 
                            field: 'field2', 
                            sortDirection: 'ASC'
                        },
                        { 
                            field: 'field3', 
                            sortDirection: 'ASC'
                        }
                    ], null, nextKey);

                    if (filteredResults && _.isEqual(filteredResults.count, 5)) {
                        break;
                    } else {
                        debug(`projection has not processed yet. trying again in 1000ms`);
                        await sleep(retryInterval);
                    }
                }

                expect(filteredResults.count).toEqual(5);
                expect(filteredResults.rows.length).toEqual(2);
                expect(filteredResults.rows[0].data).toEqual(sanitizedPayloads[1]);
                expect(filteredResults.rows[1].data).toEqual(sanitizedPayloads[4]);
                expect(filteredResults.previousKey).toEqual(Buffer.from(JSON.stringify([payloads[1].field1, payloads[1].field2, payloads[1].field3, payloads[1].keysetId])).toString('base64'));
                expect(filteredResults.nextKey).toEqual(Buffer.from(JSON.stringify([payloads[4].field1, payloads[4].field2, payloads[4].field3, payloads[4].keysetId])).toString('base64'));
                expect(pollCounter).toBeLessThan(10);
            });

            it('should get data from the playbacklistview with keyset pagination support - querying with null values - 2', async function() {
                const projectionConfig = {
                    projectionId: 'keyset-list-null-2',
                    projectionName: 'KeySet Pagination Listing - Null',
                    playbackInterface: {
                        $init: function() {
                            return {
                                count: 0
                            }
                        },
                        KEYSET_LIST_NULL_2_CREATED: async function(state, event, funcs) {
                            const playbackList = await funcs.getPlaybackList('keyset_list_null_2');
                            const eventPayload = event.payload.payload;
                            const data = {
                                keysetId: eventPayload.keysetId,
                                field1: eventPayload.field1,
                                field2: eventPayload.field2,
                                field3: eventPayload.field3
                            };
                            await playbackList.add(event.aggregateId, event.streamRevision, data, {});
                        }
                    },
                    query: {
                        context: 'keyset',
                        aggregate: 'keyset'
                    },
                    partitionBy: 'stream',
                    outputState: 'true',
                    playbackList: {
                        name: 'keyset_list_null_2',
                        fields: [{
                            name: 'keysetId',
                            type: 'string'
                        },{
                            name: 'field1',
                            type: 'int'
                        },{
                            name: 'field2',
                            type: 'string'
                        },{
                            name: 'field3',
                            type: 'int'
                        }]
                    }
                };

                await clusteredEventstore.projectAsync(projectionConfig);
                await clusteredEventstore.startAllProjectionsAsync();
                await clusteredEventstore.runProjectionAsync(projectionConfig.projectionId, false);

                await clusteredEventstore.registerPlaybackListViewAsync('keyset-list-null-2',
                    ('SELECT keyset_list_null_2.row_id, keyset_list_null_2.row_revision, keyset_list_null_2.row_json, keyset_list_null_2.keysetId, keyset_list_null_2.field1, keyset_list_null_2.field2, keyset_list_null_2.field3' + 
                    ' FROM (SELECT keyset_list_null_2.row_id as rowId FROM keyset_list_null_2 @@where @@paginationOrder @@limit) LRLT JOIN keyset_list_null_2 ON LRLT.rowId = keyset_list_null_2.row_id @@order;'),
                    'SELECT COUNT(1) as total_count FROM keyset_list_null_2 list @@where;', {
                        alias: {},
                        paginationPrimaryKeys: [
                            `keysetId`
                        ],
                    });

                const payloads = [
                    { // #1
                        field1: 5,
                        field2: 'CCCC',
                        field3: 2
                    },
                    { // #3
                        field1: null,
                        field2: 'BBBB',
                        field3: 5
                    },
                    { // #2
                        field1: 4,
                        field2: 'BBBB',
                        field3: null
                    },
                    { // #5
                        field1: null,
                        field2: 'DDDD',
                        field3: 4
                    },
                    { // #4
                        field1: null,
                        field2: 'DDDD',
                        field3: 2
                    }
                ];

                let stream;
                for (let i = 0; i < 5; i++) {
                    const keysetId = shortid.generate();
                    stream = await clusteredEventstore.getLastEventAsStreamAsync({
                        context: 'keyset',
                        aggregate: 'keyset',
                        aggregateId: keysetId
                    });
                    Bluebird.promisifyAll(stream);

                    const payload = payloads[i];
                    payload.keysetId = keysetId;

                    const event = {
                        name: "KEYSET_LIST_NULL_2_CREATED",
                        payload: payload
                    };
                    stream.addEvent(event);
                    await stream.commitAsync();
                }

                const sanitizedPayloads = [
                    { // #1
                        field1: 5,
                        field2: 'CCCC',
                        field3: 2,
                        keysetId: payloads[0].keysetId
                    },
                    { // #3
                        field2: 'BBBB',
                        field3: 5,
                        keysetId: payloads[1].keysetId
                    },
                    { // #2
                        field1: 4,
                        field2: 'BBBB',
                        keysetId: payloads[2].keysetId
                    },
                    { // #5
                        field2: 'DDDD',
                        field3: 4,
                        keysetId: payloads[3].keysetId
                    },
                    { // #4
                        field2: 'DDDD',
                        field3: 2,
                        keysetId: payloads[4].keysetId
                    }
                ];

                let pollCounter = 0;
                let filteredResults = null;
                while (pollCounter < 10) {
                    pollCounter += 1;
                    debug('polling');

                    const playbackListView = await clusteredEventstore.getPlaybackListViewAsync('keyset-list-null-2');
                    Bluebird.promisifyAll(playbackListView);

                    const nextKeyArray = [payloads[2].field1, payloads[2].field2, payloads[2].field3, payloads[2].keysetId];
                    const nextKey = Buffer.from(JSON.stringify(nextKeyArray)).toString('base64');
                    filteredResults = await playbackListView.queryAsync(0, 2, null, [
                        { 
                            field: 'field1', 
                            sortDirection: 'DESC'
                        },
                        { 
                            field: 'field2', 
                            sortDirection: 'ASC'
                        },
                        { 
                            field: 'field3', 
                            sortDirection: 'ASC'
                        }
                    ], null, nextKey);

                    if (filteredResults && _.isEqual(filteredResults.count, 5)) {
                        break;
                    } else {
                        debug(`projection has not processed yet. trying again in 1000ms`);
                        await sleep(retryInterval);
                    }
                }

                expect(filteredResults.count).toEqual(5);
                expect(filteredResults.rows.length).toEqual(2);
                expect(filteredResults.rows[0].data).toEqual(sanitizedPayloads[1]);
                expect(filteredResults.rows[1].data).toEqual(sanitizedPayloads[4]);
                expect(filteredResults.previousKey).toEqual(Buffer.from(JSON.stringify([payloads[1].field1, payloads[1].field2, payloads[1].field3, payloads[1].keysetId])).toString('base64'));
                expect(filteredResults.nextKey).toEqual(Buffer.from(JSON.stringify([payloads[4].field1, payloads[4].field2, payloads[4].field3, payloads[4].keysetId])).toString('base64'));
                expect(pollCounter).toBeLessThan(10);
            });
        });
    });

    describe('projections - stream buffer on', () => {
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
                    {
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
                    enableProjectionEventStreamBuffer: true,
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

        it('should run the projection', async function() {
            let context = `vehicle${shortid.generate()}`
            const projectionConfig = {
                projectionId: context,
                projectionName: 'Vehicle Listing',
                playbackInterface: {
                    $init: function() {
                        return {
                            count: 0
                        }
                    },
                    VEHICLE_CREATED: async function(state, event, funcs) {
                    }
                },
                query: [{
                    context: context
                }],
                partitionBy: 'stream',
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
            await clusteredEventstore.runProjectionAsync(projectionConfig.projectionId, false);
            await clusteredEventstore.startAllProjectionsAsync();

            const vehicleId = shortid.generate();
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
                    make: "Honda",
                    model: "Jazz",
                    mileage: 1245
                }
            }
            stream.addEvent(event);
            await stream.commitAsync();

            let pollCounter = 0;
            let projection;
            let projectionTasks;
            while (pollCounter < 10) {
                pollCounter += 1;
                projection = await clusteredEventstore.getProjectionAsync(projectionConfig.projectionId);
                projectionTasks = await clusteredEventstore.getProjectionTasksAsync(projectionConfig.projectionId);

                if (projection && projectionTasks.length > 0) {
                    let hasPassed = false;
                    for (const pj of projectionTasks) {
                        if (pj.processedDate && projection.state == 'running') {
                            hasPassed = true;
                        }
                    }
                    if (hasPassed) {
                        break;
                    } else {
                        debug(`projection has not processed yet. trying again in 1000ms`);
                        await sleep(retryInterval);
                    }
                } else {
                    debug(`projection has not processed yet. trying again in 1000ms`);
                    await sleep(retryInterval);
                }
            }

            expect(pollCounter).toBeLessThan(10);

            expect(projection.state).toEqual('running');
            expect(projectionTasks).toBeTruthy();
            expect(projectionTasks.length).toEqual(4);
            expect(projectionTasks[0].processedDate).toBeTruthy();
        });

        it('should add a projection and process all relevant events', async function() {
            let context = `vehicle${shortid.generate()}`
            const initialProjectionConfig = {
                projectionId: context,
                projectionName: 'Vehicle Listing',
                playbackInterface: {
                    $init: function() {
                        return {
                            count: 0
                        }
                    },
                    VEHICLE_CREATED: async function(state, event, funcs) {
                        const playbackList = await funcs.getPlaybackList('vehicle_list_initial_on');
                        const eventPayload = event.payload.payload;
                        const data = {
                            vehicleId: eventPayload.vehicleId,
                            year: eventPayload.year,
                            make: eventPayload.make,
                            model: eventPayload.model,
                            mileage: eventPayload.mileage
                        };
                        await playbackList.add(event.aggregateId, event.streamRevision, data, {});
                    }
                },
                query: {
                    context: context,
                    aggregate: 'vehicle'
                },
                partitionBy: 'stream',
                outputState: 'true',
                playbackList: {
                    name: 'vehicle_list_initial_on',
                    fields: [{
                        name: 'vehicleId',
                        type: 'string'
                    },{
                        name: 'make',
                        type: 'string'
                    },{
                        name: 'year',
                        type: 'int'
                    }]
                }
            };

            await clusteredEventstore.projectAsync(initialProjectionConfig);
            await clusteredEventstore.runProjectionAsync(initialProjectionConfig.projectionId, false);
            await clusteredEventstore.startAllProjectionsAsync();

            const vehicleId1 = shortid.generate();
            const vehicleId2 = shortid.generate();
            const vehicleId3 = shortid.generate();
            const stream1 = await clusteredEventstore.getLastEventAsStreamAsync({
                context: context,
                aggregate: 'vehicle',
                aggregateId: vehicleId1
            });
            const stream2 = await clusteredEventstore.getLastEventAsStreamAsync({
                context: context,
                aggregate: 'vehicle',
                aggregateId: vehicleId2
            });
            const stream3 = await clusteredEventstore.getLastEventAsStreamAsync({
                context: context,
                aggregate: 'vehicle',
                aggregateId: vehicleId3
            });

            Bluebird.promisifyAll(stream1);
            Bluebird.promisifyAll(stream2);
            Bluebird.promisifyAll(stream3);

            const initialEvents = [{
                    name: "VEHICLE_CREATED",
                    payload: {
                        vehicleId: vehicleId1,
                        year: 2012,
                        make: "Honda",
                        model: "Jazz",
                        mileage: 1245
                    }
                },
                {
                    name: "VEHICLE_CREATED",
                    payload: {
                        vehicleId: vehicleId2,
                        year: 2014,
                        make: "Honda",
                        model: "Civic",
                        mileage: 1267
                    }
                },
                {
                    name: "VEHICLE_CREATED",
                    payload: {
                        vehicleId: vehicleId3,
                        year: 2020,
                        make: "Honda",
                        model: "Accord",
                        mileage: 3456
                    }
                }
            ];

            stream1.addEvent(initialEvents[0]);
            stream2.addEvent(initialEvents[1]);
            stream3.addEvent(initialEvents[2]);
            await stream1.commitAsync();
            await stream2.commitAsync();

            let pollCounter = 0;
            let result = null;
            while (pollCounter < 10) {
                pollCounter += 1;

                const playbackList = clusteredEventstore.getPlaybackList('vehicle_list_initial_on');
                const filteredResults = await playbackList.query(0, 5, [{
                    field: 'make',
                    operator: 'is',
                    value: 'Honda'
                }], null);

                result = filteredResults.rows;

                if (result && result.length === 2) {
                    break;
                } else {
                    debug(`projection has not processed yet. trying again in 1000ms`);
                    await sleep(retryInterval);
                }
            }
            expect(result.length).toEqual(2);
            expect(pollCounter).toBeLessThan(10);

            const additionalProjectionConfig = {
                projectionId: context + '-add',
                projectionName: 'Vehicle Listing Additional',
                playbackInterface: {
                    $init: function() {
                        return {
                            count: 0
                        }
                    },
                    VEHICLE_CREATED: async function(state, event, funcs) {
                        const playbackList = await funcs.getPlaybackList('vehicle_list_additional_on');
                        const eventPayload = event.payload.payload;
                        const data = {
                            vehicleId: eventPayload.vehicleId,
                            year: eventPayload.year,
                            make: eventPayload.make,
                            model: eventPayload.model,
                            mileage: eventPayload.mileage
                        };
                        await playbackList.add(event.aggregateId, event.streamRevision, data, {});
                    }
                },
                query: {
                    context: context,
                    aggregate: 'vehicle'
                },
                partitionBy: 'stream',
                outputState: 'true',
                playbackList: {
                    name: 'vehicle_list_additional_on',
                    fields: [{
                        name: 'vehicleId',
                        type: 'string'
                    },{
                        name: 'make',
                        type: 'string'
                    },{
                        name: 'year',
                        type: 'int'
                    }]
                }
            };

            await clusteredEventstore.projectAsync(additionalProjectionConfig);
            await clusteredEventstore.runProjectionAsync(additionalProjectionConfig.projectionId, false);

            pollCounter = 0;
            result = null;
            while (pollCounter < 10) {
                pollCounter += 1;

                const playbackList = clusteredEventstore.getPlaybackList('vehicle_list_additional_on');
                const filteredResults = await playbackList.query(0, 5, [{
                    field: 'make',
                    operator: 'is',
                    value: 'Honda'
                }], [{
                    field: 'year',
                    sortDirection: 'ASC'
                }]);

                result = filteredResults.rows;

                if (result && result.length === 2) {
                    break;
                } else {
                    debug(`projection has not processed yet. trying again in 1000ms`);
                    await sleep(retryInterval);
                }
            }
            expect(result.length).toEqual(2);
            expect(result[1].data).toEqual(initialEvents[1].payload);
            expect(pollCounter).toBeLessThan(10);

            await stream3.commitAsync();
            
            pollCounter = 0;
            let initialProjectionResult = null;
            let additionalProjectionResult = null;
            while (pollCounter < 10) {
                pollCounter += 1;

                const playbackListInitial = clusteredEventstore.getPlaybackList('vehicle_list_initial_on');
                const filteredInitialResults = await playbackListInitial.query(0, 5, [{
                    field: 'make',
                    operator: 'is',
                    value: 'Honda'
                }], [{
                    field: 'year',
                    sortDirection: 'ASC'
                }]);
                const playbackListAdditional = clusteredEventstore.getPlaybackList('vehicle_list_additional_on');
                const filteredAdditionalResults = await playbackListAdditional.query(0, 5, [{
                    field: 'make',
                    operator: 'is',
                    value: 'Honda'
                }], [{
                    field: 'year',
                    sortDirection: 'ASC'
                }]);

                initialProjectionResult = filteredInitialResults.rows;
                additionalProjectionResult = filteredAdditionalResults.rows;

                if (initialProjectionResult && initialProjectionResult.length === 3 && additionalProjectionResult && additionalProjectionResult.length === 3) {
                    break;
                } else {
                    debug(`both projections have not processed yet. trying again in 1000ms`);
                    await sleep(retryInterval);
                }
            }
            expect(initialProjectionResult.length).toEqual(3);
            expect(initialProjectionResult[2].data).toEqual(initialEvents[2].payload);
            expect(additionalProjectionResult.length).toEqual(3);
            expect(additionalProjectionResult[2].data).toEqual(initialEvents[2].payload);
            expect(pollCounter).toBeLessThan(10);
        });

        it('should add a projection and process latest events only if the configured fromOffset is set to latest', async function() {
            let context = `vehicle${shortid.generate()}`
            const initialProjectionConfig = {
                projectionId: context,
                projectionName: 'Vehicle Listing',
                playbackInterface: {
                    $init: function() {
                        return {
                            count: 0
                        }
                    },
                    VEHICLE_CREATED: async function(state, event, funcs) {
                        const playbackList = await funcs.getPlaybackList('vehicle_list_initial_on_2');
                        const eventPayload = event.payload.payload;
                        const data = {
                            vehicleId: eventPayload.vehicleId,
                            year: eventPayload.year,
                            make: eventPayload.make,
                            model: eventPayload.model,
                            mileage: eventPayload.mileage
                        };
                        await playbackList.add(event.aggregateId, event.streamRevision, data, {});
                    }
                },
                query: {
                    context: context,
                    aggregate: 'vehicle'
                },
                partitionBy: 'stream',
                outputState: 'true',
                playbackList: {
                    name: 'vehicle_list_initial_on_2',
                    fields: [{
                        name: 'vehicleId',
                        type: 'string'
                    },{
                        name: 'make',
                        type: 'string'
                    }]
                }
            };

            await clusteredEventstore.projectAsync(initialProjectionConfig);
            await clusteredEventstore.runProjectionAsync(initialProjectionConfig.projectionId, false);
            await clusteredEventstore.startAllProjectionsAsync();

            const vehicleId1 = shortid.generate();
            const vehicleId2 = shortid.generate();
            const vehicleId3 = shortid.generate();
            const stream1 = await clusteredEventstore.getLastEventAsStreamAsync({
                context: context,
                aggregate: 'vehicle',
                aggregateId: vehicleId1
            });
            const stream2 = await clusteredEventstore.getLastEventAsStreamAsync({
                context: context,
                aggregate: 'vehicle',
                aggregateId: vehicleId2
            });
            const stream3 = await clusteredEventstore.getLastEventAsStreamAsync({
                context: context,
                aggregate: 'vehicle',
                aggregateId: vehicleId3
            });

            Bluebird.promisifyAll(stream1);
            Bluebird.promisifyAll(stream2);
            Bluebird.promisifyAll(stream3);

            const initialEvents = [{
                    name: "VEHICLE_CREATED",
                    payload: {
                        vehicleId: vehicleId1,
                        year: 2012,
                        make: "Honda",
                        model: "Jazz",
                        mileage: 1245
                    }
                },
                {
                    name: "VEHICLE_CREATED",
                    payload: {
                        vehicleId: vehicleId2,
                        year: 2014,
                        make: "Honda",
                        model: "Civic",
                        mileage: 1267
                    }
                },
                {
                    name: "VEHICLE_CREATED",
                    payload: {
                        vehicleId: vehicleId3,
                        year: 2020,
                        make: "Honda",
                        model: "Accord",
                        mileage: 3456
                    }
                }
            ];

            stream1.addEvent(initialEvents[0]);
            stream2.addEvent(initialEvents[1]);
            stream3.addEvent(initialEvents[2]);
            await stream1.commitAsync();
            await stream2.commitAsync();

            let pollCounter = 0;
            let result = null;
            while (pollCounter < 10) {
                pollCounter += 1;

                const playbackList = clusteredEventstore.getPlaybackList('vehicle_list_initial_on_2');
                const filteredResults = await playbackList.query(0, 5, [{
                    field: 'make',
                    operator: 'is',
                    value: 'Honda'
                }], null);

                result = filteredResults.rows;

                if (result && result.length === 2) {
                    break;
                } else {
                    debug(`projection has not processed yet. trying again in 1000ms`);
                    await sleep(retryInterval);
                }
            }
            expect(result.length).toEqual(2);
            expect(pollCounter).toBeLessThan(10);

            const projectionWithLatestOffsetConfig = {
                projectionId: context + '-2',
                projectionName: 'Vehicle Listing 2',
                playbackInterface: {
                    $init: function() {
                        return {
                            count: 0
                        }
                    },
                    VEHICLE_CREATED: async function(state, event, funcs) {
                        const playbackList = await funcs.getPlaybackList('vehicle_list_latest_on_2');
                        const eventPayload = event.payload.payload;
                        const data = {
                            vehicleId: eventPayload.vehicleId,
                            year: eventPayload.year,
                            make: eventPayload.make,
                            model: eventPayload.model,
                            mileage: eventPayload.mileage
                        };
                        await playbackList.add(event.aggregateId, event.streamRevision, data, {});
                    }
                },
                query: {
                    context: context,
                    aggregate: 'vehicle'
                },
                partitionBy: 'stream',
                outputState: 'true',
                fromOffset: 'latest',
                playbackList: {
                    name: 'vehicle_list_latest_on_2',
                    fields: [{
                        name: 'vehicleId',
                        type: 'string'
                    },{
                        name: 'make',
                        type: 'string'
                    }]
                }
            };

            await clusteredEventstore.projectAsync(projectionWithLatestOffsetConfig);
            await clusteredEventstore.runProjectionAsync(projectionWithLatestOffsetConfig.projectionId, false);

            await stream3.commitAsync();
            
            pollCounter = 0;
            result = null;
            while (pollCounter < 10) {
                pollCounter += 1;

                const playbackList = clusteredEventstore.getPlaybackList('vehicle_list_latest_on_2');
                const filteredResults = await playbackList.query(0, 5, [{
                    field: 'make',
                    operator: 'is',
                    value: 'Honda'
                }], null);

                result = filteredResults.rows;

                if (result && result.length === 1) {
                    break;
                } else {
                    debug(`projection has not processed yet. trying again in 1000ms`);
                    await sleep(retryInterval);
                }
            }
            expect(result.length).toEqual(1);
            expect(result[0].data).toEqual(initialEvents[2].payload);
            expect(pollCounter).toBeLessThan(10);
        });
    });
});
