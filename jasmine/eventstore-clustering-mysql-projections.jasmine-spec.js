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

fdescribe('eventstore clustering mysql projection tests', () => {
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
        while (connectCounter < 30) {
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

        if (connectCounter == 10) {
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
        let clustedEventstore;
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
                membershipPollingTimeout: 1000
            };
            clustedEventstore = clusteredEs(config);
            Bluebird.promisifyAll(clustedEventstore);
            await clustedEventstore.initAsync();
        }, 60000);

        // afterEach(async function() {
        //     for(const eventstore of clustedEventstore._eventstores) {
        //         Bluebird.promisifyAll(eventstore.store);
        //         await eventstore.store.clearAsync();
        //     }

        //     const projections = await clustedEventstore.getProjectionsAsync();
        //     for (const projection of projections[0]) {
        //         const splitProjectionId = projection.projectionId.split(':');
        //         await clustedEventstore.deleteProjectionAsync(splitProjectionId[0]);
        //     }
        // }, 60000);

        fit('should run the projection with sharding', async function() {
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
            await clustedEventstore.projectAsync(projectionConfig);
            
            debug('runProjectionAsync');
            await clustedEventstore.runProjectionAsync(projectionConfig.projectionId, false);
    
            
            debug('startAllProjectionsAsync');
            await clustedEventstore.startAllProjectionsAsync();
    
    
            const vehicleId = shortId.generate();
            // const vehicleId = 'QPvvZubzuMg';
            const stream = await clustedEventstore.getLastEventAsStreamAsync({
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
                projection = await clustedEventstore.getProjectionAsync(projectionConfig.projectionId);
    
                debug(projection);

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
            await clustedEventstore.projectAsync(projectionConfig);
            
            debug('runProjectionAsync');
            await clustedEventstore.runProjectionAsync(projectionConfig.projectionId, false);
    
            
            debug('startAllProjectionsAsync');
            await clustedEventstore.startAllProjectionsAsync();
    
    
            const vehicleId = shortId.generate();
            // const vehicleId = 'QPvvZubzuMg';
            const stream = await clustedEventstore.getLastEventAsStreamAsync({
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
                projection = await clustedEventstore.getProjectionAsync(projectionConfig.projectionId);
    
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

        it('should not run projections when deleted', async function() {
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
            await clustedEventstore.projectAsync(projectionConfig);
            
            debug('runProjectionAsync');
            await clustedEventstore.runProjectionAsync(projectionConfig.projectionId, false);
    
            debug('deleteProjectionAsync');
            await clustedEventstore.deleteProjectionAsync(projectionConfig.projectionId);

            debug('startAllProjectionsAsync');
            await clustedEventstore.startAllProjectionsAsync();
    
    
            const vehicleId = shortId.generate();
            // const vehicleId = 'QPvvZubzuMg';
            const stream = await clustedEventstore.getLastEventAsStreamAsync({
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
            while (pollCounter < 5) {
                pollCounter += 1;
                debug('polling');
                projection = await clustedEventstore.getProjectionAsync(projectionConfig.projectionId);
    
                let projections = [];
                if(Array.isArray(projection)) {
                    projections = projections.concat(projection);
                } else {
                    projections.push(projection);
                }
    
                if(projections.length > 0) {
                    let hasPassed = false;
                    for(const pj of projections) {
                        if(pj) {
                            if (pj.processedDate && pj.state == 'running') {
                                hasPassed = true;
                            }
                            if(isNumber(pj.offset)) {
                                projectionOffset = pj.offset
                            }
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
    
            // NOTE: expected not to process since deleted projection
            expect(pollCounter).toEqual(5);
            expect(projectionOffset).toEqual(undefined);
        });

        it('should not run projections when paused', async function() {
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
            await clustedEventstore.projectAsync(projectionConfig);
            
            debug('runProjectionAsync');
            await clustedEventstore.runProjectionAsync(projectionConfig.projectionId, false);
    
            debug('pauseProjectionAsync');
            await clustedEventstore.pauseProjectionAsync(projectionConfig.projectionId);

            debug('startAllProjectionsAsync');
            await clustedEventstore.startAllProjectionsAsync();
    
    
            const vehicleId = shortId.generate();
            // const vehicleId = 'QPvvZubzuMg';
            const stream = await clustedEventstore.getLastEventAsStreamAsync({
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
            while (pollCounter < 5) {
                pollCounter += 1;
                debug('polling');
                projection = await clustedEventstore.getProjectionAsync(projectionConfig.projectionId);
    
                let projections = [];
                if(Array.isArray(projection)) {
                    projections = projections.concat(projection);
                } else {
                    projections.push(projection);
                }
    
                if(projections.length > 0) {
                    let hasPassed = false;
                    for(const pj of projections) {
                        if(pj) {
                            if (pj.processedDate && pj.state == 'running') {
                                hasPassed = true;
                            }
                            if(isNumber(pj.offset)) {
                                projectionOffset = pj.offset
                            }
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
    
            // NOTE: expected not to process since deleted projection
            expect(pollCounter).toEqual(5);
            expect(projectionOffset).toEqual(undefined);
        });
    });
})