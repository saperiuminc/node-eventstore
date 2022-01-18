const compose = require('docker-compose');
const path = require('path');
const debug = require('debug')('eventstore:clustering:mysql');
const mysql2 = require('mysql2/promise');
const clusteredEs = require('../clustered/index');
const Bluebird = require('bluebird');
const shortId = require('shortid');
const Redis = require('ioredis');
const { isNumber } = require('lodash');

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
    pollingTimeout: 1000
}

describe('eventstore clustering mysql projection tests', () => {
    const sleep = function(timeout) {
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

        const retryInterval = 1000;
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
        })
        debug('docker compose down finished');
    });

    describe('projections', () => {
        let clusteredEventstore;
        beforeAll(async function() {
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
        }, 60000);

        beforeEach(async function() {
            for(const eventstore of clusteredEventstore._eventstores) {
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
            let context = `vehicle${shortId.generate()}`

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
    
    
            const vehicleId = shortId.generate();
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
                if(Array.isArray(projection)) {
                    projections = projections.concat(projection);
                } else {
                    projections.push(projection);
                }
    
                if(projections.length > 0) {
                    let hasPassed = false;
                    for(const pj of projections) {
                        if (pj.processedDate && pj.state == 'running') {
                            hasPassed = true;
                        }
                        if(isNumber(pj.offset)) {
                            projectionOffset = pj.offset
                        }
                    }
                    if (hasPassed) {
                        break;
                    } else {
                        debug(`projection has not processed yet. trying again in 1000ms`);
                        await sleep(1000);
                    }
                } else {
                    debug(`projection has not processed yet. trying again in 1000ms`);
                    await sleep(1000);
                }
                
            }
    
            expect(pollCounter).toBeLessThan(10);
            expect(projectionOffset).toBeLessThanOrEqual(1);
        });

        it('should run the projection on same projectionId:shard:partition if same aggregateId', async function() {
            let context = `vehicle${shortId.generate()}`

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
    
    
            const vehicleId = shortId.generate();
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
                if(Array.isArray(projection)) {
                    projections = projections.concat(projection);
                } else {
                    projections.push(projection);
                }
    
                if(projections.length > 0) {
                    let hasPassed = false;
                    for(const pj of projections) {
                        if (pj.processedDate && pj.state == 'running') {
                            hasPassed = true;
                        }
                        if(isNumber(pj.offset)) {
                            projectionOffset = pj.offset
                        }
                    }
                    if (hasPassed) {
                        break;
                    } else {
                        debug(`projection has not processed yet. trying again in 1000ms`);
                        await sleep(1000);
                    }
                } else {
                    debug(`projection has not processed yet. trying again in 1000ms`);
                    await sleep(1000);
                }
                
            }
    
            expect(pollCounter).toBeLessThan(10);
            expect(projectionOffset).toBeLessThanOrEqual(2);
        });

        it('should reset the projection', async function() {
            let context = `vehicle${shortId.generate()}`

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
            if(Array.isArray(projection)) {
                projections = projections.concat(projection);
            } else {
                projections.push(projection);
            }

            if(projections.length > 0) {
                for(const pj of projections) {
                    if(pj) {
                        expect(pj.offset).toEqual(0);
                    }
                }
            }
        });

        it('should delete the projection', async function() {
            let context = `vehicle${shortId.generate()}`

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
            if(Array.isArray(storedProjection)) {
                projections = projections.concat(storedProjection);
            } else {
                projections.push(storedProjection);
            }

            if(projections.length > 0) {
                for(const pj of projections) {
                    if(pj) {
                        expect(pj).toBeNull();
                    }
                }
            }
        });

        it('should pause the projection', async function() {
            let context = `vehicle${shortId.generate()}`

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
            if(Array.isArray(storedProjection)) {
                projections = projections.concat(storedProjection);
            } else {
                projections.push(storedProjection);
            }

            if(projections.length > 0) {
                for(const pj of projections) {
                    if(pj) {
                        expect(pj.state).toEqual('paused');
                    }
                }
            }
        });

        it('should set the projection to faulted if there is an event handler error', async function() {
            let context = `vehicle${shortId.generate()}`
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
    
            const vehicleId = shortId.generate();
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
                if(Array.isArray(projection)) {
                    projections = projections.concat(projection);
                } else {
                    projections.push(projection);
                }
    
                if(projections.length > 0) {
                    for(const pj of projections) {
                        debug('pj', pj);
                        if (pj.state == 'faulted') {
                            debug('projection.state is faulted. continuing with test');
                            faultedProjection = pj;
                            break;
                        } else {
                            debug(`projection.state ${pj.state} is not faulted. trying again in 1000ms`);
                            await sleep(1000);
                        }
                    }
                } else {
                    debug(`projection has not processed yet. trying again in 1000ms`);
                    await sleep(1000);
                }

                if(faultedProjection != undefined) {
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
            let context = `vehicle${shortId.generate()}`
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
    
            const vehicleId = shortId.generate();
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
                if(Array.isArray(projection)) {
                    projections = projections.concat(projection);
                } else {
                    projections.push(projection);
                }
    
                if(projections.length > 0) {
                    for(const pj of projections) {
                        debug('pj', pj);
                        if (pj.state == 'faulted') {
                            debug('projection.state is faulted. continuing with test');
                            faultedProjection = pj;
                            break;
                        } else {
                            debug(`projection.state ${pj.state} is not faulted. trying again in 1000ms`);
                            await sleep(1000);
                        }
                    }
                } else {
                    debug(`projection has not processed yet. trying again in 1000ms`);
                    await sleep(1000);
                }

                if(faultedProjection != undefined) {
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
                if(Array.isArray(projection)) {
                    projections = projections.concat(projection);
                } else {
                    projections.push(projection);
                }
    
                if(projections.length > 0) {
                    for(const pj of projections) {
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
                            await sleep(1000);
                        }
                    }
                } else {
                    debug(`projection has not processed yet. trying again in 1000ms`);
                    await sleep(1000);
                }

                if(faultedProjection != undefined) {
                    break;
                }
            }
    
            expect(faultedProjection.offset).toEqual(2);
            expect(faultedProjection.state).toEqual('running');
            expect(faultedProjection.isIdle).toEqual(0);
        });

        fit('should add and update data to the stateList', async function() {
            try {
                let context = `vehicle${shortId.generate()}`
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
            const vehicleId = shortId.generate();
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
             const vehicleId2 = shortId.generate();
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
                if(Array.isArray(projection)) {
                    projections = projections.concat(projection);
                } else {
                    projections.push(projection);
                }
    
                if(projections.length > 0) {
                    for(const pj of projections) {
                        debug('pj', pj);
                        if (pj.processedDate && pj.state == 'running') {
                            projectionRunned = pj;
                            break;
                        } else {
                            debug(`projection has not processed yet. trying again in 1000ms`);
                            await sleep(1000);
                        }
                    }
                } else {
                    debug(`projection has not processed yet. trying again in 1000ms`);
                    await sleep(1000);
                }

                if(projectionRunned != undefined) {
                    break;
                }
            }

            expect(pollCounter).toBeLessThan(10);

            const playbackList = clusteredEventstore.getPlaybackList('vehicle_list');
            debug('playbackList', playbackList);
            const filteredResults = await playbackList.query(0, 1, [{
                field: 'vehicleId',
                operator: 'is',
                value: vehicleId
            }], null);
    
            const result = filteredResults.rows[0];
            expect(result.data).toEqual(event2.payload);
            } catch (error) {
                console.error('got error', error);
                throw error;
            }
            
        });
    });
})  