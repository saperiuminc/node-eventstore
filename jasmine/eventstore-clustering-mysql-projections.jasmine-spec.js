const compose = require('docker-compose');
const path = require('path');
const debug = require('debug')('eventstore:clustering:mysql');
const mysql2 = require('mysql2/promise');
const clusteredEs = require('../clustered/index');
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

const retryInterval = 1000;
fdescribe('eventstore clustering mysql projection tests', () => {
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
        // await compose.down({
        //     cwd: path.join(__dirname)
        // });
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
                    projectionGroup: 'default',
                    membershipPollingTimeout: 10000
                };
                clusteredEventstore = clusteredEs(config);
                Bluebird.promisifyAll(clusteredEventstore);
                await clusteredEventstore.initAsync();
            } catch (error) {
                console.error('error in beforeAll', error);
            }

        }, 60000);

        beforeEach(async function() {
            for (const eventstore of clusteredEventstore._eventstores) {
                Bluebird.promisifyAll(eventstore.store);
                await eventstore.store.clearAsync();
            }

            const projections = await clusteredEventstore.getProjectionsAsync();
            for (const projection of projections[0]) {
                const splitProjectionId = projection.projectionId.split(':');
                await clusteredEventstore.resetProjectionAsync(splitProjectionId[0]);
                await clusteredEventstore.deleteProjectionAsync(splitProjectionId[0]);
            }
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

                    }
                },
                query: [{
                    context: context
                }],
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

            debug('projectAsync');
            await clusteredEventstore.projectAsync(projectionConfig);

            debug('runProjectionAsync');
            await clusteredEventstore.runProjectionAsync(projectionConfig.projectionId, false);


            debug('startAllProjectionsAsync');
            await clusteredEventstore.startAllProjectionsAsync();


            const vehicleId = shortid.generate();
            // const vehicleId = 'QPvvZubzuMg';
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
            let projectionOffset;
            while (pollCounter < 10) {
                pollCounter += 1;
                debug('polling');
                projection = await clusteredEventstore.getProjectionAsync(projectionConfig.projectionId);

                let projections = [];
                if (Array.isArray(projection)) {
                    projections = projections.concat(projection);
                } else {
                    projections.push(projection);
                }

                if (projections.length > 0) {
                    let hasPassed = false;
                    for (const pj of projections) {
                        if (pj.processedDate && pj.state == 'running') {
                            hasPassed = true;
                        }
                        if (isNumber(pj.offset)) {
                            projectionOffset = pj.offset
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

            debug('projectAsync');
            await clusteredEventstore.projectAsync(projectionConfig);

            debug('runProjectionAsync');
            await clusteredEventstore.runProjectionAsync(projectionConfig.projectionId, false);


            debug('startAllProjectionsAsync');
            await clusteredEventstore.startAllProjectionsAsync();


            const vehicleId = shortid.generate();
            // const vehicleId = 'QPvvZubzuMg';
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

            let pollCounter = 0;
            let projection;
            let projectionOffset;
            while (pollCounter < 10) {
                pollCounter += 1;
                debug('polling');
                projection = await clusteredEventstore.getProjectionAsync(projectionConfig.projectionId);

                let projections = [];
                if (Array.isArray(projection)) {
                    projections = projections.concat(projection);
                } else {
                    projections.push(projection);
                }

                if (projections.length > 0) {
                    let hasPassed = false;
                    for (const pj of projections) {
                        if (pj.processedDate && pj.state == 'running') {
                            hasPassed = true;
                        }
                        if (isNumber(pj.offset)) {
                            projectionOffset = pj.offset
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
            await clusteredEventstore.resetProjectionAsync(projectionConfig.projectionId);

            const projection = await clusteredEventstore.getProjectionAsync(projectionConfig.projectionId);

            let projections = [];
            if (Array.isArray(projection)) {
                projections = projections.concat(projection);
            } else {
                projections.push(projection);
            }

            if (projections.length > 0) {
                for (const pj of projections) {
                    if (pj) {
                        expect(pj.offset).toEqual(0);
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
            await clusteredEventstore.deleteProjectionAsync(projectionConfig.projectionId);

            const storedProjection = await clusteredEventstore.getProjectionAsync(projectionConfig.projectionId);

            let projections = [];
            if (Array.isArray(storedProjection)) {
                projections = projections.concat(storedProjection);
            } else {
                projections.push(storedProjection);
            }

            if (projections.length > 0) {
                for (const pj of projections) {
                    if (pj) {
                        expect(pj).toBeNull();
                    }
                }
            }
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
            await clusteredEventstore.pauseProjectionAsync(projectionConfig.projectionId);

            const storedProjection = await clusteredEventstore.getProjectionAsync(projectionConfig.projectionId);

            let projections = [];
            if (Array.isArray(storedProjection)) {
                projections = projections.concat(storedProjection);
            } else {
                projections.push(storedProjection);
            }

            if (projections.length > 0) {
                for (const pj of projections) {
                    if (pj) {
                        expect(pj.state).toEqual('paused');
                    }
                }
            }
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
            let faultedProjection;
            while (pollCounter < 10) {
                pollCounter += 1;
                debug('polling');
                projection = await clusteredEventstore.getProjectionAsync(projectionConfig.projectionId);

                let projections = [];
                if (Array.isArray(projection)) {
                    projections = projections.concat(projection);
                } else {
                    projections.push(projection);
                }

                if (projections.length > 0) {
                    for (const pj of projections) {
                        debug('pj', pj);
                        if (pj.state == 'faulted' && pj.offset > 0) {
                            debug('projection.state is faulted. continuing with test');
                            faultedProjection = pj;
                            break;
                        } else {
                            debug(`projection.state ${pj.state} is not faulted. trying again in 1000ms`);
                            await sleep(retryInterval);
                        }
                    }
                } else {
                    debug(`projection has not processed yet. trying again in 1000ms`);
                    await sleep(retryInterval);
                }

                if (faultedProjection != undefined) {
                    break;
                }
            }

            expect(pollCounter).toBeLessThan(20);
            expect(faultedProjection.error).toBeTruthy();
            expect(faultedProjection.errorEvent).toBeTruthy();
            expect(faultedProjection.errorOffset).toBeGreaterThan(0);
            expect(faultedProjection.offset).toEqual(1);
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

                    },
                    VEHICLE_UPDATED: async function(state, event, funcs) {
                        throw new Error('your fault!');
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
            let faultedProjection;
            while (pollCounter < 10) {
                pollCounter += 1;
                debug('polling');
                projection = await clusteredEventstore.getProjectionAsync(projectionConfig.projectionId);

                let projections = [];
                if (Array.isArray(projection)) {
                    projections = projections.concat(projection);
                } else {
                    projections.push(projection);
                }

                if (projections.length > 0) {
                    for (const pj of projections) {
                        debug('pj', pj);
                        if (pj.state == 'faulted') {
                            debug('projection.state is faulted. continuing with test');
                            faultedProjection = pj;
                            break;
                        } else {
                            debug(`projection.state ${pj.state} is not faulted. trying again in 1000ms`);
                            await sleep(retryInterval);
                        }
                    }
                } else {
                    debug(`projection has not processed yet. trying again in 1000ms`);
                    await sleep(retryInterval);
                }

                if (faultedProjection != undefined) {
                    break;
                }
            }

            expect(pollCounter).toBeLessThan(20);
            expect(faultedProjection.error).toBeTruthy();
            expect(faultedProjection.errorEvent).toBeTruthy();
            expect(faultedProjection.errorOffset).toBeGreaterThan(0);
            expect(faultedProjection.offset).toEqual(1);

            let lastOffset = faultedProjection.offset;
            faultedProjection = null;
            pollCounter = 0;

            await clusteredEventstore.runProjectionAsync(projectionConfig.projectionId, true);

            while (pollCounter < 10) {
                pollCounter += 1;
                debug('polling 2');
                projection = await clusteredEventstore.getProjectionAsync(projectionConfig.projectionId);

                let projections = [];
                if (Array.isArray(projection)) {
                    projections = projections.concat(projection);
                } else {
                    projections.push(projection);
                }

                if (projections.length > 0) {
                    for (const pj of projections) {
                        debug('pj', pj);
                        if (!lastOffset) {
                            lastOffset = pj.offset;
                        }
                        if (pj.offset > lastOffset) {
                            debug('offset changed', lastOffset, pj.offset);
                            faultedProjection = pj;
                            break;
                        } else {
                            debug(`projection has not processed yet. trying again in 1000ms`, lastOffset, pj.offset);
                            await sleep(retryInterval);
                        }
                    }
                } else {
                    debug(`projection has not processed yet. trying again in 1000ms`);
                    await sleep(retryInterval);
                }

                if (faultedProjection != undefined) {
                    break;
                }
            }

            expect(faultedProjection.offset).toEqual(2);
            expect(faultedProjection.state).toEqual('running');
            expect(faultedProjection.isIdle).toEqual(0);
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

                        console.log('event.aggregateId', event.aggregateId);
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
                partitionBy: '',
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
            let projectionRunned;
            while (pollCounter < 10) {
                pollCounter += 1;
                debug('polling');
                projection = await clusteredEventstore.getProjectionAsync(projectionConfig.projectionId);

                let projections = [];
                if (Array.isArray(projection)) {
                    projections = projections.concat(projection);
                } else {
                    projections.push(projection);
                }

                if (projections.length > 0) {
                    for (const pj of projections) {
                        debug('pj', pj);
                        if (pj.processedDate && pj.state == 'running') {
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
            await clusteredEventstore.runProjectionAsync(projectionConfig.projectionId, false);
            await clusteredEventstore.startAllProjectionsAsync();

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

            let pollCounter = 0;
            let projectionRunned;
            while (pollCounter < 10) {
                pollCounter += 1;
                debug('polling');
                projection = await clusteredEventstore.getProjectionAsync(projectionConfig.projectionId);

                let projections = [];
                if (Array.isArray(projection)) {
                    projections = projections.concat(projection);
                } else {
                    projections.push(projection);
                }

                if (projections.length > 0) {
                    for (const pj of projections) {
                        debug('pj', pj);
                        if (pj.processedDate && pj.offset > 0) {
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
                partitionBy: '',
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
                projection = await clusteredEventstore.getProjectionAsync(projectionConfig.projectionId);

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

            const event2 = {
                name: "VEHICLE_DELETED",
                payload: {
                    vehicleId: vehicleId
                }
            }
            stream.addEvent(event2);
            await stream.commitAsync();

            let pollCounter = 0;
            let projectionRunned;
            while (pollCounter < 10) {
                pollCounter += 1;
                debug('polling');
                projection = await clusteredEventstore.getProjectionAsync(projectionConfig.projectionId);

                let projections = [];
                if (Array.isArray(projection)) {
                    projections = projections.concat(projection);
                } else {
                    projections.push(projection);
                }

                if (projections.length > 0) {
                    for (const pj of projections) {
                        debug('pj', pj);
                        if (pj.processedDate && pj.offset > 0) {
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
            let projectionRunned;
            while (pollCounter < 10) {
                pollCounter += 1;
                debug('polling');
                projection = await clusteredEventstore.getProjectionAsync(projectionConfig.projectionId);

                let projections = [];
                if (Array.isArray(projection)) {
                    projections = projections.concat(projection);
                } else {
                    projections.push(projection);
                }

                if (projections.length > 0) {
                    for (const pj of projections) {
                        debug('pj', pj);
                        if (pj.processedDate && pj.offset > 0) {
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
                partitionBy: '',
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
    
            
            await clusteredEventstore.projectAsync(projectionConfig);
            await clusteredEventstore.runProjectionAsync(projectionConfig.projectionId, false);
            await clusteredEventstore.startAllProjectionsAsync();
    
            const waitForRebalance = function() {
                return new Promise((resolve) => {
                    clusteredEventstore.on('rebalance', resolve);
                });
            }
    
            let eventsCount = 0;
            const listener = (data) => {
                eventsCount += data.eventsCount;
                if (eventsCount == expectedEventsCount) {
                    done();
                }
            };
    
            clusteredEventstore.on('playbackSuccess', listener);
            await waitForRebalance();
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
    
            await clusteredEventstore.projectAsync(initialProjectionConfig);
            await clusteredEventstore.runProjectionAsync(initialProjectionConfig.projectionId, false);
            await clusteredEventstore.startAllProjectionsAsync();
            
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

            let pollCounter = 0;
            let projectionRunned;
            while (pollCounter < 10) {
                pollCounter += 1;
                debug('polling');
                let projection = await clusteredEventstore.getProjectionAsync(initialProjectionConfig.projectionId);
    
                let projections = [];
                if(Array.isArray(projection)) {
                    projections = projections.concat(projection);
                } else {
                    projections.push(projection);
                }
    
                if(projections.length > 0) {
                    for(const pj of projections) {
                        debug('pj', pj);
                        if (pj.processedDate && pj.offset > 0) {
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

                if(projectionRunned != undefined) {
                    break;
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
                partitionBy: '',
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
        

            pollCounter = 0;
            projectionRunned;
            while (pollCounter < 10) {
                pollCounter += 1;
                debug('polling 2');
                const projection2 = await clusteredEventstore.getProjectionAsync(projectionWithLatestOffsetConfig.projectionId);
    
                let projections = [];
                if(Array.isArray(projection2)) {
                    projections = projections.concat(projection2);
                } else {
                    projections.push(projection2);
                }
    
                if(projections.length > 0) {
                    for(const pj of projections) {
                        debug('pj', pj);
                        if (pj.processedDate && pj.offset > 0) {
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

                if(projectionRunned != undefined) {
                    break;
                }
            }

            expect(projectionRunned.offset).toEqual(2);
        });
    });
})