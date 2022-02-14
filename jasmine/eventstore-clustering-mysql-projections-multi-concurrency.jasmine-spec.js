const compose = require('docker-compose');
const path = require('path');
const debug = require('debug')('eventstore:clustering:mysql');
const mysql2 = require('mysql2/promise');
const clusteredEs = require('../lib/eventstore-projections/clustered-eventstore');
const Bluebird = require('bluebird');
const shortid = require('shortid');
const Redis = require('ioredis');
const {
    isNumber
} = require('lodash');

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

const _deserializeProjectionOffset = function(serializedProjectionOffset) {
    return JSON.parse(Buffer.from(serializedProjectionOffset, 'base64').toString('utf8'));
}

const _serializeProjectionOffset = function(projectionOffset) {
    return Buffer.from(JSON.stringify(projectionOffset)).toString('base64');
}

const retryInterval = 1000;
// TODO: fix cleanup of eventstores after each test
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

    describe('projections', () => {
        let clusteredEventstore;
        beforeAll(async function() {
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
                await clusteredEventstore.initAsync();
            } catch (error) {
                console.error('error in beforeAll', error);
            }
        }, 60000);

        afterEach(async function() {
            // console.log('AFTER EACH');
            await clusteredEventstore.closeProjectionEventStreamBuffersAsync();

            const projections = await clusteredEventstore.getProjectionsAsync();
            for (const projection of projections) {
                const projectionId = projection.projectionId;
                await clusteredEventstore.resetProjectionAsync(projectionId);
                await clusteredEventstore.deleteProjectionAsync(projectionId);
            }

            for (const eventstore of clusteredEventstore._eventstores) {
                Bluebird.promisifyAll(eventstore.store);
                await eventstore.store.clearAsync();
            }

            clusteredEventstore.removeAllListeners('rebalance');
            // console.log('AFTER EACH DONE');
        }, 60000);

        it('should run the projection with sharding', async function() {
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
                        // console.log('TEST PROJECTION GOT EVENT', event);
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

            pollCounter = 0;
            let projection;
            let projectionOffset;
            while (pollCounter < 10) {
                pollCounter += 1;
                debug('polling');
                projection = await clusteredEventstore.getProjectionAsync(projectionConfig.projectionId);
                const projectionTasks = await clusteredEventstore.getProjectionTasksAsync(projectionConfig.projectionId);

                if (projection && projectionTasks.length > 0) {
                    let hasPassed = false;
                    for (const pj of projectionTasks) {
                        if (pj.processedDate && projection.state == 'running') {
                            hasPassed = true;
                        }
                        if (isNumber(_deserializeProjectionOffset(pj.offset))) {
                            projectionOffset = _deserializeProjectionOffset(pj.offset);
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

            console.log(pollCounter, projectionOffset);
            expect(pollCounter).toBeLessThan(10);
            expect(projectionOffset).toBeLessThanOrEqual(1);
        });

        it('should run the projection on same projectionId:shard:partition if same aggregateId', async function() {
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

            const vehicleId = shortid.generate();
            const stream = await clusteredEventstore.getLastEventAsStreamAsync({
                context: context,
                aggregate: 'vehicle',
                aggregateId: vehicleId
            });

            Bluebird.promisifyAll(stream);

            const event1 = {
                name: "VEHICLE_CREATED",
                payload: {
                    vehicleId: vehicleId,
                    year: 2012,
                    make: "Honda",
                    model: "Jazz",
                    mileage: 1245
                }
            }

            stream.addEvent(event1);

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

            stream.addEvent(event2);

            await stream.commitAsync();

            pollCounter = 0;
            let projection;
            let projectionOffset;
            while (pollCounter < 10) {
                pollCounter += 1;
                debug('polling');
                projection = await clusteredEventstore.getProjectionAsync(projectionConfig.projectionId);
                const projectionTasks = await clusteredEventstore.getProjectionTasksAsync(projectionConfig.projectionId);

                if (projection && projectionTasks.length > 0) {
                    let hasPassed = false;
                    for (const pj of projectionTasks) {
                        if (pj.processedDate && projection.state == 'running') {
                            hasPassed = true;
                        }

                        if (isNumber(_deserializeProjectionOffset(pj.offset))) {
                            projectionOffset = _deserializeProjectionOffset(pj.offset);
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
            expect(projectionOffset).toBeLessThanOrEqual(2);
        });

        xit('should reset the projection', async function() {
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
                        expect(_deserializeProjectionOffset(pj.offset)).toEqual(0);
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

        it('should set the projection to faulted if there is an event handler error', async function() {
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
                debug('polling');
                projection = await clusteredEventstore.getProjectionAsync(projectionConfig.projectionId);
                const projectionTasks = await clusteredEventstore.getProjectionTasksAsync(projectionConfig.projectionId);

                if (projection && projectionTasks.length > 0) {
                    let hasPassed = false;
                    for (const pj of projectionTasks) {
                        // console.log('pj: ', pj.projectionTaskId, pj.offset, projection.state);
                        if (pj.offset && _deserializeProjectionOffset(pj.offset) > 0 && projection.state == 'faulted') {
                            hasPassed = true;
                            console.log('projection.state is faulted. continuing with test');
                            faultedProjectionTask = pj;
                            break;
                        }
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

            expect(pollCounter).toBeLessThan(20);
            expect(faultedProjectionTask.error).toBeTruthy();
            expect(faultedProjectionTask.errorEvent).toBeTruthy();
            expect(_deserializeProjectionOffset(faultedProjectionTask.errorOffset)).toBeGreaterThan(0);
            expect(_deserializeProjectionOffset(faultedProjectionTask.offset)).toEqual(1);
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
                        console.log('PROCESSING EVENT VEHICLE_CREATED');
                    },
                    VEHICLE_UPDATED: async function(state, event, funcs) {
                        console.log('PROCESSING EVENT VEHICLE_UPDATED');
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

            pollCounter = 0;
            let projection;
            let faultedProjectionTask;
            while (pollCounter < 10) {
                pollCounter += 1;
                debug('polling');
                projection = await clusteredEventstore.getProjectionAsync(projectionConfig.projectionId);
                const projectionTasks = await clusteredEventstore.getProjectionTasksAsync(projectionConfig.projectionId);

                if (projection && projectionTasks.length > 0) {
                    let hasPassed = false;
                    for (const pj of projectionTasks) {
                        // console.log('pj: ', pj.projectionTaskId, pj.offset, projection.state);
                        if (pj.offset && _deserializeProjectionOffset(pj.offset) > 0 && projection.state == 'faulted') {
                            hasPassed = true;
                            console.log('projection.state is faulted. continuing with test');
                            faultedProjectionTask = pj;
                            break;
                        }
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

            expect(pollCounter).toBeLessThan(20);
            expect(faultedProjectionTask.error).toBeTruthy();
            expect(faultedProjectionTask.errorEvent).toBeTruthy();
            expect(_deserializeProjectionOffset(faultedProjectionTask.errorOffset)).toBeGreaterThan(0);
            expect(_deserializeProjectionOffset(faultedProjectionTask.offset)).toEqual(1);

            let lastOffset = _deserializeProjectionOffset(faultedProjectionTask.offset);
            faultedProjectionTask = null;
            pollCounter = 0;

            await clusteredEventstore.runProjectionAsync(projectionConfig.projectionId, true);

            while (pollCounter < 10) {
                pollCounter += 1;
                debug('polling 2');
                projection = await clusteredEventstore.getProjectionAsync(projectionConfig.projectionId);
                const projectionTasks = await clusteredEventstore.getProjectionTasksAsync(projectionConfig.projectionId);

                if (projection && projectionTasks.length > 0) {
                    let hasPassed = false;
                    for (const pj of projectionTasks) {
                        // console.log('pj', lastOffset, pj.projectionTaskId, pj.offset, projection.state);
                        if (!lastOffset) {
                            lastOffset = isNumber(pj.offset) ? _deserializeProjectionOffset(pj.offset) : null;
                        }
                        if (pj.offset && _deserializeProjectionOffset(pj.offset) > lastOffset && projection.state === 'running') {
                            hasPassed = true;
                            // console.log('offset changed', lastOffset, pj.offset);
                            faultedProjectionTask = pj;
                            break;
                        }
                    }
                    if (hasPassed) {
                        break;
                    } else {
                        debug(`projection has not processed yet. trying again in 1000ms`, lastOffset);
                        await sleep(retryInterval);
                    }
                } else {
                    debug(`projection has not processed yet. trying again in 1000ms`);
                    await sleep(retryInterval);
                }

                if (faultedProjectionTask != undefined) {
                    break;
                }
            }

            expect(_deserializeProjectionOffset(faultedProjectionTask.offset)).toEqual(2);
            expect(projection.state).toEqual('running');
            expect(faultedProjectionTask.isIdle).toEqual(0);
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

                        // console.log('event.aggregateId', event.aggregateId);
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

            pollCounter = 0;
            while (pollCounter < 10) {
                pollCounter += 1;
                debug('polling');
                const projection = await clusteredEventstore.getProjectionAsync(projectionConfig.projectionId);
                const projectionTasks = await clusteredEventstore.getProjectionTasksAsync(projectionConfig.projectionId);

                if (projection && projectionTasks.length > 0) {
                    let hasPassed = false;

                    let totalOffset = 0;
                    let offsetsPerShard = {};
                    for(const pj of projectionTasks) {
                        debug('pj', pj);

                        if (pj.processedDate && projection.state == 'running') {
                            offsetsPerShard[pj.shard] = Math.max(offsetsPerShard[pj.shard] || 0, _deserializeProjectionOffset(pj.offset));
                        }
                    }
                    const offsets = Object.values(offsetsPerShard);
                    offsets.forEach((offset) => {
                        totalOffset += offset;
                    });
                    if (totalOffset >= 4) {
                        hasPassed = true;
                        break;
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

            const playbackList = clusteredEventstore.getPlaybackList('vehicle_list');
            const filteredResults = await playbackList.query(0, 1, [{
                field: 'vehicleId',
                operator: 'is',
                value: vehicleId
            }], null);

            const result = filteredResults.rows[0];
            expect(result.data).toEqual(event2.payload);
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

            let vehicleIdForChecking;
            let event2ForChecking;
            for(let i = 0; i <= 10; i++) {
                const vehicleId = shortid.generate();
                if(vehicleIdForChecking == undefined) {
                    vehicleIdForChecking = vehicleId;
                }
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

            pollCounter = 0;
            while (pollCounter < 10) {
                pollCounter += 1;
                debug('polling');
                const projection = await clusteredEventstore.getProjectionAsync(projectionConfig.projectionId);
                const projectionTasks = await clusteredEventstore.getProjectionTasksAsync(projectionConfig.projectionId);

                if (projection && projectionTasks.length > 0) {
                    let hasPassed = false;

                    let totalOffset = 0;
                    let offsetsPerShard = {};
                    for(const pj of projectionTasks) {
                        debug('pj', pj);

                        if (pj.processedDate && projection.state == 'running') {
                            offsetsPerShard[pj.shard] = Math.max(offsetsPerShard[pj.shard] || 0, _deserializeProjectionOffset(pj.offset));
                        }
                    }
                    const offsets = Object.values(offsetsPerShard);
                    offsets.forEach((offset) => {
                        totalOffset += offset;
                    });
                    if (totalOffset >= 22) {
                        hasPassed = true;
                        break;
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

            const playbackList = clusteredEventstore.getPlaybackList('vehicle_list');
            const filteredResults = await playbackList.query(0, 1, [{
                field: 'vehicleId',
                operator: 'is',
                value: vehicleIdForChecking
            }], null);

            const result = filteredResults.rows[0];
            expect(result.data).toEqual(event2ForChecking.payload);
        });

        // FOR REVIEW: This test executes an ostrich pattern
        xit('should batch update the playbacklist data', async function() {
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
                        const playbackList = await funcs.getPlaybackList('vehicle_batch_list');
                        const eventPayload = event.payload.payload;
                        const data = {
                            vehicleId: eventPayload.vehicleId,
                            year: eventPayload.year,
                            make: eventPayload.make,
                            model: eventPayload.model,
                            mileage: eventPayload.mileage,
                            dealershipId: eventPayload.dealershipId,
                            dealershipName: eventPayload.dealershipName
                        };
                        await playbackList.add(event.aggregateId, event.streamRevision, data, {});
                        debug('VEHICLE_CREATED');
                    },
                    DEALERSHIP_UPDATED: async function(state, event, funcs) {
                        const eventPayload = event.payload.payload;
                        const playbackList = await funcs.getPlaybackList('vehicle_batch_list');

                        const filters = [{
                            field: 'dealershipId',
                            operator: 'is',
                            value: eventPayload.dealershipId
                        }];

                        await playbackList.batchUpdate(filters, {
                            dealershipName: eventPayload.dealershipName
                        });
                        debug('DEALERSHIP_UPDATED');
                    }
                },
                query: [{
                    context: context,
                    aggregate: 'vehicle'
                }, {
                    context: context,
                    aggregate: 'dealership'
                }],
                partitionBy: 'stream',
                outputState: 'true',
                playbackList: {
                    name: 'vehicle_batch_list',
                    fields: [{
                        name: 'vehicleId',
                        type: 'string'
                    }, {
                        name: 'dealershipId',
                        type: 'string'
                    }]
                }
            };

            await clusteredEventstore.projectAsync(projectionConfig);
            await clusteredEventstore.runProjectionAsync(projectionConfig.projectionId, false);
            await clusteredEventstore.startAllProjectionsAsync();

            const dealershipId = 'CARWAVE';
            const newDealershipName = 'NEW Carwave Auctions';
            for (let i = 0; i < 2; i++) {
                const vehicleId = shortid.generate();

                const stream = await clusteredEventstore.getLastEventAsStreamAsync({
                    context: context,
                    aggregate: 'vehicle',
                    aggregateId: vehicleId
                });

                Bluebird.promisifyAll(stream);

                const vehicleEvent = {
                    name: "VEHICLE_CREATED",
                    payload: {
                        vehicleId: vehicleId,
                        year: 2012,
                        make: "Honda",
                        model: "Jazz",
                        mileage: 1245,
                        dealershipId: 'CARWAVE',
                        dealershipName: 'Carwave Auctions'
                    }
                }
                stream.addEvent(vehicleEvent);
                await stream.commitAsync();
            }

            const dealershipStream = await clusteredEventstore.getLastEventAsStreamAsync({
                context: context,
                aggregate: 'dealership',
                aggregateId: dealershipId
            });

            Bluebird.promisifyAll(dealershipStream);

            const dealershipEvent = {
                name: "DEALERSHIP_UPDATED",
                payload: {
                    dealershipId: dealershipId,
                    dealershipName: newDealershipName
                }
            }
            dealershipStream.addEvent(dealershipEvent);
            await dealershipStream.commitAsync();

            let pollCounter = 0;
            let projectionRunned;
            while (pollCounter < 10) {
                pollCounter += 1;
                debug('polling');
                const projection = await clusteredEventstore.getProjectionAsync(projectionConfig.projectionId);

                let projections = [];
                if (Array.isArray(projection)) {
                    projections = projections.concat(projection);
                } else {
                    projections.push(projection);
                }

                if (projections.length > 0) {
                    for (const pj of projections) {
                        debug('pj', pj);
                        if (pj.processedDate) {
                            projectionRunned = pj;
                            break;
                        } else {
                            debug(`projection has not processed yet. trying again in 1000ms`);
                            await sleep(retryInterval);
                        }
                    }
                } else {
                    debug(`projection has not processed yet. trying again in 1000ms`);
                    await sleep(retryInterval);
                }

                if (projectionRunned != undefined) {
                    break;
                }
            }

            expect(pollCounter).toBeLessThan(10);

            // const dealershipEvent = {
            //     name: "DEALERSHIP_UPDATED",
            //     payload: {
            //         dealershipId: dealershipId,
            //         dealershipName: newDealershipName
            //     }
            // }
            // dealershipStream.addEvent(dealershipEvent);
            // await dealershipStream.commitAsync();
            // await sleep(2000);

            // pollCounter = 0;
            // projectionRunned = undefined;
            // while (pollCounter < 10) {
            //     pollCounter += 1;
            //     debug('polling2');
            //     projection = await clusteredEventstore.getProjectionAsync(projectionConfig.projectionId);

            //     let projections = [];
            //     if(Array.isArray(projection)) {
            //         projections = projections.concat(projection);
            //     } else {
            //         projections.push(projection);
            //     }

            //     if(projections.length > 0) {
            //         for(const pj of projections) {
            //             debug('pj', pj);
            //             if (pj.processedDate) {
            //                 projectionRunned = pj;
            //                 break;
            //             } else {
            //                 debug(`projection has not processed yet. trying again in 1000ms`);
            //                 await sleep(retryInterval);
            //             }
            //         }
            //     } else {
            //         debug(`projection has not processed yet. trying again in 1000ms`);
            //         await sleep(retryInterval);
            //     }

            //     if(projectionRunned != undefined) {
            //         break;
            //     }
            // }

            const playbackList = clusteredEventstore.getPlaybackList('vehicle_batch_list');
            const filteredResults = await playbackList.query(0, 2, [], null);
            expect(filteredResults.rows[0].data.dealershipName).toEqual(newDealershipName);
            expect(filteredResults.rows[1].data.dealershipName).toEqual(newDealershipName);
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
                const projection = await clusteredEventstore.getProjectionAsync(projectionConfig.projectionId);
                const projectionTasks = await clusteredEventstore.getProjectionTasksAsync(projectionConfig.projectionId);

                if (projection && projectionTasks.length > 0) {
                    let hasPassed = false;

                    let totalOffset = 0;
                    let offsetsPerShard = {};
                    for(const pj of projectionTasks) {
                        debug('pj', pj);

                        if (pj.processedDate && projection.state == 'running') {
                            offsetsPerShard[pj.shard] = Math.max(offsetsPerShard[pj.shard] || 0, _deserializeProjectionOffset(pj.offset));
                        }
                    }
                    const offsets = Object.values(offsetsPerShard);
                    offsets.forEach((offset) => {
                        totalOffset += offset;
                    });
                    if (totalOffset >= 2) {
                        hasPassed = true;
                        break;
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

            const playbackList = clusteredEventstore.getPlaybackList('vehicle_list');
            const filteredResults = await playbackList.query(0, 1, [{
                field: 'vehicleId',
                operator: 'is',
                value: vehicleId
            }], null);

            const result = filteredResults.rows[0];
            expect(result).toBeFalsy();
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
            while (pollCounter < 10) {
                pollCounter += 1;
                debug('polling');
                const projection = await clusteredEventstore.getProjectionAsync(projectionConfig.projectionId);
                const projectionTasks = await clusteredEventstore.getProjectionTasksAsync(projectionConfig.projectionId);

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

            await sleep(retryInterval);

            const playbackListView = await clusteredEventstore.getPlaybackListViewAsync('vehicle-list');
            debug('playbackListView', playbackListView);
            Bluebird.promisifyAll(playbackListView);

            const filteredResults = await playbackListView.queryAsync(0, 1, [{
                field: 'vehicleId',
                operator: 'is',
                value: vehicleId
            }], null);

            debug('filteredResults', filteredResults);
            const result = filteredResults.rows[0];
            expect(result.data).toEqual(event.payload);
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

        it('should handle batch events', async (done) => {
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

        it('should add a projection with a proper offset if the configured fromOffset is set to latest', async function() {
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
    
            let hasRebalanced = false;
            clusteredEventstore.on('rebalance', function(updatedAssignments) {
                for(const projectionTaskId of updatedAssignments) {
                    if (projectionTaskId.startsWith(initialProjectionConfig.projectionId)) {
                        hasRebalanced = true;
                    }
                }
            });
            await clusteredEventstore.projectAsync(initialProjectionConfig);
            await clusteredEventstore.runProjectionAsync(initialProjectionConfig.projectionId, false);
            await clusteredEventstore.startAllProjectionsAsync();

            let pollCounter = 0;
            while (pollCounter < 5) {
                pollCounter += 1;
                if (hasRebalanced) {
                    // console.log(`clustered-es has rebalanced with the proper projection id. continuing with test`, initialProjectionConfig.projectionId);
                    break;
                } else {
                    // console.log(`clustered-es has not yet rebalanced with the proper projection id. trying again in 1000ms`, initialProjectionConfig.projectionId);
                    await sleep(retryInterval);
                }
            }
            expect(pollCounter).toBeLessThan(5);
            
            const vehicleId = shortid.generate();
            const stream = await clusteredEventstore.getLastEventAsStreamAsync({
                context: context,
                aggregate: 'vehicle',
                aggregateId: vehicleId
            });
    
            Bluebird.promisifyAll(stream);
    
            const initialEvents = [{
                    name: "VEHICLE_CREATED",
                    payload: {
                        vehicleId: vehicleId,
                        year: 2012,
                        make: "Honda",
                        model: "Jazz",
                        mileage: 1245
                    }
                },
                {
                    name: "VEHICLE_CREATED",
                    payload: {
                        vehicleId: vehicleId,
                        year: 2014,
                        make: "Honda",
                        model: "Jazz",
                        mileage: 1265
                    }
                }
            ]
            stream.addEvent(initialEvents[0]);
            stream.addEvent(initialEvents[1]);
            await stream.commitAsync();
    
            // let pollCounter = 0;
            // while (pollCounter < 10) {
            //     const initialProjection = await clusteredEventstore.getProjectionAsync(initialProjectionConfig.projectionId);
            //     if (initialProjection.processedDate) {
            //         break;
            //     } else {
            //         debug(`projection has not processed yet. trying again in 1000ms`);
            //         await sleep(1000);
            //     }
            // }

            pollCounter = 0;
            while (pollCounter < 10) {
                pollCounter += 1;
                debug('polling');
                const projection = await clusteredEventstore.getProjectionAsync(initialProjectionConfig.projectionId);
                const projectionTasks = await clusteredEventstore.getProjectionTasksAsync(initialProjectionConfig.projectionId);

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
    
            const projectionWithLatestOffsetConfig = {
                projectionId: 'auction-list',
                projectionName: 'Auction Listing',
                playbackInterface: {
                    $init: function() {
                        return {
                            count: 0
                        }
                    }
                },
                query: {
                    context: context,
                    aggregate: 'auction-listing'
                },
                partitionBy: 'stream',
                outputState: 'true',
                fromOffset: 'latest',
                playbackList: {
                    name: 'auction_list',
                    fields: [{
                        name: 'auctionId',
                        type: 'string'
                    }]
                }
            };
    
            await clusteredEventstore.projectAsync(projectionWithLatestOffsetConfig);
            await clusteredEventstore.runProjectionAsync(projectionWithLatestOffsetConfig.projectionId, false);
        
            pollCounter = 0;
            let maxOffset;
            while (pollCounter < 10) {
                pollCounter += 1;
                debug('polling');
                const projection = await clusteredEventstore.getProjectionAsync(projectionWithLatestOffsetConfig.projectionId);
                const projectionTasks = await clusteredEventstore.getProjectionTasksAsync(projectionWithLatestOffsetConfig.projectionId);

                if (projection && projectionTasks.length > 0) {
                    let hasPassed = false;
                    for (const pj of projectionTasks) {
                        if (pj.processedDate && projection.state == 'running') {
                            hasPassed = true;
                        }
                    }
                    if (hasPassed) {
                        for (const pj of projectionTasks) {
                            if (!maxOffset) {
                                maxOffset = _deserializeProjectionOffset(pj.offset);
                            } else {
                                if (_deserializeProjectionOffset(pj.offset) > maxOffset) {
                                    maxOffset = pj;
                                }
                            }
                        }
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

            expect(maxOffset).toEqual(2);
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
            
                await clusteredEventstore.registerPlaybackListViewAsync('keyset-list-next',
                    ('SELECT keyset_list_next.row_id, keyset_list_next.row_revision, keyset_list_next.row_json, keyset_list_next.keysetId, keyset_list_next.field1, keyset_list_next.field2, keyset_list_next.field3' + 
                        ' FROM (SELECT keyset_list_next.row_id as rowId FROM keyset_list_next @@where @@paginationOrder @@limit) LRLT JOIN keyset_list_next ON LRLT.rowId = keyset_list_next.row_id @@order;'),
                    'SELECT COUNT(1) as total_count FROM keyset_list_next list @@where;', {
                        alias: {},
                        paginationPrimaryKeys: [
                            `keysetId`
                        ],
                    });

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
            
                for (let i = 0; i < 5; i++) {
                    const keysetId = shortid.generate();
                    const stream = await clusteredEventstore.getLastEventAsStreamAsync({
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
            
                pollCounter = 0;
                while (pollCounter < 10) {
                    pollCounter += 1;
                    debug('polling');
                    const projection = await clusteredEventstore.getProjectionAsync(projectionConfig.projectionId);
                    const projectionTasks = await clusteredEventstore.getProjectionTasksAsync(projectionConfig.projectionId);
    
                    if (projection && projectionTasks.length > 0) {
                        let hasPassed = false;

                        let totalOffset = 0;
                        let offsetsPerShard = {};
                        for(const pj of projectionTasks) {
                            debug('pj', pj);

                            if (pj.processedDate && projection.state == 'running') {
                                offsetsPerShard[pj.shard] = Math.max(offsetsPerShard[pj.shard] || 0, _deserializeProjectionOffset(pj.offset));
                            }
                        }
                        const offsets = Object.values(offsetsPerShard);
                        offsets.forEach((offset) => {
                            totalOffset += offset;
                        });
                        if (totalOffset >= 5) {
                            hasPassed = true;
                            break;
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
            
                const playbackListView = await clusteredEventstore.getPlaybackListViewAsync('keyset-list-next');
                Bluebird.promisifyAll(playbackListView);
            
                const nextKeyArray = [payloads[2].field1, payloads[2].field2, payloads[2].field3, payloads[2].keysetId];
                const nextKey = Buffer.from(JSON.stringify(nextKeyArray)).toString('base64');
                const filteredResults = await playbackListView.queryAsync(0, 2, null, [
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
            
                expect(filteredResults.count).toEqual(5);
                expect(filteredResults.rows.length).toEqual(2);
                expect(filteredResults.rows[0].data).toEqual(payloads[1]);
                expect(filteredResults.rows[1].data).toEqual(payloads[4]);
                expect(filteredResults.previousKey).toEqual(Buffer.from(JSON.stringify([payloads[1].field1, payloads[1].field2, payloads[1].field3, payloads[1].keysetId])).toString('base64'));
                expect(filteredResults.nextKey).toEqual(Buffer.from(JSON.stringify([payloads[4].field1, payloads[4].field2, payloads[4].field3, payloads[4].keysetId])).toString('base64'));
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
            
                let hasRebalanced = false;
                clusteredEventstore.on('rebalance', function(updatedAssignments) {
                    for(const projectionTaskId of updatedAssignments) {
                        if (projectionTaskId.startsWith(projectionConfig.projectionId)) {
                            hasRebalanced = true;
                        }
                    }
                });
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
            
                for (let i = 0; i < 5; i++) {
                    const keysetId = shortid.generate();
                    const stream = await clusteredEventstore.getLastEventAsStreamAsync({
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

                pollCounter = 0;
                while (pollCounter < 10) {
                    pollCounter += 1;
                    debug('polling');
                    const projection = await clusteredEventstore.getProjectionAsync(projectionConfig.projectionId);
                    const projectionTasks = await clusteredEventstore.getProjectionTasksAsync(projectionConfig.projectionId);
    
                    if (projection && projectionTasks.length > 0) {
                        let hasPassed = false;

                        let totalOffset = 0;
                        let offsetsPerShard = {};
                        for(const pj of projectionTasks) {
                            debug('pj', pj);

                            if (pj.processedDate && projection.state == 'running') {
                                offsetsPerShard[pj.shard] = Math.max(offsetsPerShard[pj.shard] || 0, _deserializeProjectionOffset(pj.offset));
                            }
                        }
                        const offsets = Object.values(offsetsPerShard);
                        offsets.forEach((offset) => {
                            totalOffset += offset;
                        });
                        if (totalOffset >= 5) {
                            hasPassed = true;
                            break;
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
            
                const playbackListView = await clusteredEventstore.getPlaybackListViewAsync('keyset-list-prev');
                Bluebird.promisifyAll(playbackListView);
            
                const prevKeyArray = [payloads[3].field1, payloads[3].field2, payloads[3].field3, payloads[3].keysetId];
                const prevKey = Buffer.from(JSON.stringify(prevKeyArray)).toString('base64');
                const filteredResults = await playbackListView.queryAsync(0, 2, null, [
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
            
                expect(filteredResults.count).toEqual(5);
                expect(filteredResults.rows.length).toEqual(2);
                expect(filteredResults.rows[0].data).toEqual(payloads[1]);
                expect(filteredResults.rows[1].data).toEqual(payloads[4]);
                expect(filteredResults.previousKey).toEqual(Buffer.from(JSON.stringify([payloads[1].field1, payloads[1].field2, payloads[1].field3, payloads[1].keysetId])).toString('base64'));
                expect(filteredResults.nextKey).toEqual(Buffer.from(JSON.stringify([payloads[4].field1, payloads[4].field2, payloads[4].field3, payloads[4].keysetId])).toString('base64'));
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
        
                let hasRebalanced = false;
                clusteredEventstore.on('rebalance', function(updatedAssignments) {
                    for(const projectionTaskId of updatedAssignments) {
                        if (projectionTaskId.startsWith(projectionConfig.projectionId)) {
                            hasRebalanced = true;
                        }
                    }
                });
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
        
                for (let i = 0; i < 5; i++) {
                    const keysetId = shortid.generate();
                    const stream = await clusteredEventstore.getLastEventAsStreamAsync({
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

                pollCounter = 0;
                while (pollCounter < 10) {
                    pollCounter += 1;
                    debug('polling');
                    const projection = await clusteredEventstore.getProjectionAsync(projectionConfig.projectionId);
                    const projectionTasks = await clusteredEventstore.getProjectionTasksAsync(projectionConfig.projectionId);
    
                    if (projection && projectionTasks.length > 0) {
                        let hasPassed = false;

                        let totalOffset = 0;
                        let offsetsPerShard = {};
                        for(const pj of projectionTasks) {
                            debug('pj', pj);

                            if (pj.processedDate && projection.state == 'running') {
                                offsetsPerShard[pj.shard] = Math.max(offsetsPerShard[pj.shard] || 0, _deserializeProjectionOffset(pj.offset));
                            }
                        }
                        const offsets = Object.values(offsetsPerShard);
                        offsets.forEach((offset) => {
                            totalOffset += offset;
                        });
                        if (totalOffset >= 5) {
                            hasPassed = true;
                            break;
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
        
                const playbackListView = await clusteredEventstore.getPlaybackListViewAsync('keyset-list-last');
                Bluebird.promisifyAll(playbackListView);
        
                const filteredResults = await playbackListView.queryAsync(0, 2, null, [
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
        
                expect(filteredResults.count).toEqual(5);
                expect(filteredResults.rows.length).toEqual(1);
                expect(filteredResults.rows[0].data).toEqual(payloads[3]);
                expect(filteredResults.previousKey).toEqual(Buffer.from(JSON.stringify([payloads[3].field1, payloads[3].field2, payloads[3].field3, payloads[3].keysetId])).toString('base64'));
                expect(filteredResults.nextKey).toEqual(Buffer.from(JSON.stringify([payloads[3].field1, payloads[3].field2, payloads[3].field3, payloads[3].keysetId])).toString('base64'));
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
        
                let hasRebalanced = false;
                clusteredEventstore.on('rebalance', function(updatedAssignments) {
                    for(const projectionTaskId of updatedAssignments) {
                        if (projectionTaskId.startsWith(projectionConfig.projectionId)) {
                            hasRebalanced = true;
                        }
                    }
                });
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
        
                for (let i = 0; i < 6; i++) {
                    const keysetId = shortid.generate();
                    const stream = await clusteredEventstore.getLastEventAsStreamAsync({
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

                pollCounter = 0;
                while (pollCounter < 10) {
                    pollCounter += 1;
                    debug('polling');
                    const projection = await clusteredEventstore.getProjectionAsync(projectionConfig.projectionId);
                    const projectionTasks = await clusteredEventstore.getProjectionTasksAsync(projectionConfig.projectionId);
    
                    if (projection && projectionTasks.length > 0) {
                        let hasPassed = false;

                        let totalOffset = 0;
                        let offsetsPerShard = {};
                        for(const pj of projectionTasks) {
                            debug('pj', pj);

                            if (pj.processedDate && projection.state == 'running') {
                                offsetsPerShard[pj.shard] = Math.max(offsetsPerShard[pj.shard] || 0, _deserializeProjectionOffset(pj.offset));
                            }
                        }
                        const offsets = Object.values(offsetsPerShard);
                        offsets.forEach((offset) => {
                            totalOffset += offset;
                        });
                        if (totalOffset >= 6) {
                            hasPassed = true;
                            break;
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
        
                const playbackListView = await clusteredEventstore.getPlaybackListViewAsync('keyset-list-last-2');
                Bluebird.promisifyAll(playbackListView);
        
                const filteredResults = await playbackListView.queryAsync(0, 2, null, [
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
        
                expect(filteredResults.count).toEqual(6);
                expect(filteredResults.rows.length).toEqual(2);
                expect(filteredResults.rows[0].data).toEqual(payloads[3]);
                expect(filteredResults.rows[1].data).toEqual(payloads[5]);
                expect(filteredResults.previousKey).toEqual(Buffer.from(JSON.stringify([payloads[3].field1, payloads[3].field2, payloads[3].field3, payloads[3].keysetId])).toString('base64'));
                expect(filteredResults.nextKey).toEqual(Buffer.from(JSON.stringify([payloads[5].field1, payloads[5].field2, payloads[5].field3, payloads[5].keysetId])).toString('base64'));
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
            
                let hasRebalanced = false;
                clusteredEventstore.on('rebalance', function(updatedAssignments) {
                    for(const projectionTaskId of updatedAssignments) {
                        if (projectionTaskId.startsWith(projectionConfig.projectionId)) {
                            hasRebalanced = true;
                        }
                    }
                });
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
            
                for (let i = 0; i < 5; i++) {
                    const keysetId = shortid.generate();
                    const stream = await clusteredEventstore.getLastEventAsStreamAsync({
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

                pollCounter = 0;
                while (pollCounter < 10) {
                    pollCounter += 1;
                    debug('polling');
                    const projection = await clusteredEventstore.getProjectionAsync(projectionConfig.projectionId);
                    const projectionTasks = await clusteredEventstore.getProjectionTasksAsync(projectionConfig.projectionId);
    
                    if (projection && projectionTasks.length > 0) {
                        let hasPassed = false;

                        let totalOffset = 0;
                        let offsetsPerShard = {};
                        for(const pj of projectionTasks) {
                            debug('pj', pj);

                            if (pj.processedDate && projection.state == 'running') {
                                offsetsPerShard[pj.shard] = Math.max(offsetsPerShard[pj.shard] || 0, _deserializeProjectionOffset(pj.offset));
                            }
                        }
                        const offsets = Object.values(offsetsPerShard);
                        offsets.forEach((offset) => {
                            totalOffset += offset;
                        });
                        if (totalOffset >= 5) {
                            hasPassed = true;
                            break;
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
            
                const playbackListView = await clusteredEventstore.getPlaybackListViewAsync('keyset-list-null');
                Bluebird.promisifyAll(playbackListView);
            
                const nextKeyArray = [payloads[2].field1, payloads[2].field2, payloads[2].field3, payloads[2].keysetId];
                const nextKey = Buffer.from(JSON.stringify(nextKeyArray)).toString('base64');
                const filteredResults = await playbackListView.queryAsync(0, 2, null, [
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
            
                expect(filteredResults.count).toEqual(5);
                expect(filteredResults.rows.length).toEqual(2);
                expect(filteredResults.rows[0].data).toEqual(sanitizedPayloads[1]);
                expect(filteredResults.rows[1].data).toEqual(sanitizedPayloads[4]);
                expect(filteredResults.previousKey).toEqual(Buffer.from(JSON.stringify([payloads[1].field1, payloads[1].field2, payloads[1].field3, payloads[1].keysetId])).toString('base64'));
                expect(filteredResults.nextKey).toEqual(Buffer.from(JSON.stringify([payloads[4].field1, payloads[4].field2, payloads[4].field3, payloads[4].keysetId])).toString('base64'));
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
            
                let hasRebalanced = false;
                clusteredEventstore.on('rebalance', function(updatedAssignments) {
                    for(const projectionTaskId of updatedAssignments) {
                        if (projectionTaskId.startsWith(projectionConfig.projectionId)) {
                            hasRebalanced = true;
                        }
                    }
                });
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
            
                for (let i = 0; i < 5; i++) {
                    const keysetId = shortid.generate();
                    const stream = await clusteredEventstore.getLastEventAsStreamAsync({
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

                pollCounter = 0;
                while (pollCounter < 10) {
                    pollCounter += 1;
                    debug('polling');
                    const projection = await clusteredEventstore.getProjectionAsync(projectionConfig.projectionId);
                    const projectionTasks = await clusteredEventstore.getProjectionTasksAsync(projectionConfig.projectionId);
    
                    if (projection && projectionTasks.length > 0) {
                        let hasPassed = false;

                        let totalOffset = 0;
                        let offsetsPerShard = {};
                        for(const pj of projectionTasks) {
                            debug('pj', pj);

                            if (pj.processedDate && projection.state == 'running') {
                                offsetsPerShard[pj.shard] = Math.max(offsetsPerShard[pj.shard] || 0, _deserializeProjectionOffset(pj.offset));
                            }
                        }
                        const offsets = Object.values(offsetsPerShard);
                        offsets.forEach((offset) => {
                            totalOffset += offset;
                        });
                        if (totalOffset >= 5) {
                            hasPassed = true;
                            break;
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
            
                const playbackListView = await clusteredEventstore.getPlaybackListViewAsync('keyset-list-null-2');
                Bluebird.promisifyAll(playbackListView);
            
                const nextKeyArray = [payloads[2].field1, payloads[2].field2, payloads[2].field3, payloads[2].keysetId];
                const nextKey = Buffer.from(JSON.stringify(nextKeyArray)).toString('base64');
                const filteredResults = await playbackListView.queryAsync(0, 2, null, [
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
            
                expect(filteredResults.count).toEqual(5);
                expect(filteredResults.rows.length).toEqual(2);
                expect(filteredResults.rows[0].data).toEqual(sanitizedPayloads[1]);
                expect(filteredResults.rows[1].data).toEqual(sanitizedPayloads[4]);
                expect(filteredResults.previousKey).toEqual(Buffer.from(JSON.stringify([payloads[1].field1, payloads[1].field2, payloads[1].field3, payloads[1].keysetId])).toString('base64'));
                expect(filteredResults.nextKey).toEqual(Buffer.from(JSON.stringify([payloads[4].field1, payloads[4].field2, payloads[4].field3, payloads[4].keysetId])).toString('base64'));
            });
        });
    });
});
