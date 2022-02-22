const compose = require('docker-compose');
const path = require('path');
const debug = require('debug')('eventstore:clustering:mysql');
const mysql2 = require('mysql2/promise');
const clusteredEs = require('../clustered');
const Bluebird = require('bluebird');
const shortid = require('shortid');
const Redis = require('ioredis');
const helpers = require('../lib/helpers');
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
    return helpers.deserializeProjectionOffset(serializedProjectionOffset);
}

const _serializeProjectionOffset = function(projectionOffset) {
    return helpers.serializeProjectionOffset(projectionOffset);
}

const retryInterval = 1000;
describe('eventstore clustering mysql projection tests', () => {
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
    
    describe('useEventPublisher on enableProjection is true', () => {
        describe('when calling useEventPublisher before init', () => {
            let clusteredEventstore;
            it('should be able to call useEventPublisher', async () => {
                const useEventPublisherConfig = {
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
                    pollingTimeout: eventstoreConfig.pollingTimeout,
                    pollingMaxRevisions: 100,
                    errorMaxRetryCount: 2,
                    errorRetryExponent: 2,
                    playbackEventJobCount: 10,
                    context: 'vehicle',
                    projectionGroup: shortid.generate(),
                    membershipPollingTimeout: 10000
                };
                clusteredEventstore = clusteredEs(useEventPublisherConfig);
                Bluebird.promisifyAll(clusteredEventstore);
                Bluebird.promisifyAll(clusteredEventstore.store);
                let hasProcessed = false;
                const useEventPublisherCallback = function(evt, callback) {
                    console.log('CALLBACK GOT EVENT', evt);
                    hasProcessed = true;
                    callback();
                };
                clusteredEventstore.useEventPublisher(useEventPublisherCallback);
                
                await clusteredEventstore.initAsync();
                for (const eventstore of clusteredEventstore._eventstores) {
                    Bluebird.promisifyAll(eventstore.store);
                    await eventstore.store.clearAsync();
                }
                
                await clusteredEventstore.closeProjectionEventStreamBuffersAsync();
                
                let projections = await clusteredEventstore.getProjectionsAsync();
                for (const projection of projections) {
                    await clusteredEventstore.resetProjectionAsync(projection.projectionId);
                    await clusteredEventstore.deleteProjectionAsync(projection.projectionId);
                }
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
                while (pollCounter < 5) {
                    if (hasProcessed) {
                        break;
                    } else {
                        debug(`events have not yet been played back. trying again in 1000ms`);
                        pollCounter++;
                        await sleep(1000);
                    }
                }
                expect(pollCounter).toBeLessThan(5);
                
                await clusteredEventstore.closeProjectionEventStreamBuffersAsync();
                
                projections = await clusteredEventstore.getProjectionsAsync();
                for (const projection of projections) {
                    const projectionId = projection.projectionId;
                    await clusteredEventstore.resetProjectionAsync(projectionId);
                    await clusteredEventstore.deleteProjectionAsync(projectionId);
                }
                await clusteredEventstore.store.clearAsync();
                
                clusteredEventstore.removeAllListeners('rebalance');
                await clusteredEventstore.closeAsync();
            });
        });
        
        describe('without invoking useEventPublisher', () => {
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
                        projectionGroup: shortid.generate(), // task assignment group should be deleted for every test
                        membershipPollingTimeout: 10000
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
                // console.log('AFTER EACH');
                await clusteredEventstore.closeProjectionEventStreamBuffersAsync();
                
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
            }, 60000);
            
            it('should be able to process projections', async () => {
                let context = `vehicle${shortid.generate()}`;
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
                const timeStampBeforeEventAdd = new Date().getTime();
                stream.addEvent(event);
                await stream.commitAsync();
                
                pollCounter = 0;
                let projection;
                let projectionOffset;
                let maxCommitStamp;
                // let maxStreamRevision;
                let maxEventId;
                while (pollCounter < 10) {
                    pollCounter += 1;
                    debug('polling');
                    projection = await clusteredEventstore.getProjectionAsync(projectionConfig.projectionId);
                    const projectionTasks = await clusteredEventstore.getProjectionTasksAsync(projectionConfig.projectionId);
                    
                    if (projection && projectionTasks.length > 0) {
                        let hasPassed = false;
                        for (const pj of projectionTasks) {
                            const deserializedOffset = _deserializeProjectionOffset(pj.offset);
                            projectionOffset = deserializedOffset.map((bookmark) => {
                                const deserializedBookmark = _deserializeProjectionOffset(bookmark);
                                return {
                                    eventId: deserializedBookmark.eventId,
                                    commitStamp: deserializedBookmark.commitStamp,
                                    streamRevision: deserializedBookmark.streamRevision
                                }
                            })
                            maxCommitStamp = Math.max(...projectionOffset.map(bookmark => bookmark.commitStamp));
                            // maxStreamRevision = Math.max(...projectionOffset.map(bookmark => bookmark.streamRevision));
                            projectionOffset.forEach((bookmark) => {
                                if (!maxEventId || bookmark.eventId > maxEventId) {
                                    maxEventId = bookmark.eventId
                                }
                            });
                            if (pj.processedDate && projection.state == 'running' && maxCommitStamp) {
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
                expect(projectionOffset.length).toBeGreaterThan(0);
                expect(maxCommitStamp).toBeGreaterThanOrEqual(timeStampBeforeEventAdd);
                expect(maxEventId != '').toBeTruthy();
            });
        });
    });
    
    describe('event subscription', () => {
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
                    membershipPollingTimeout: 250
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
            // console.log('AFTER EACH');
            await clusteredEventstore.closeProjectionEventStreamBuffersAsync();
            
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
        }, 60000);
        
        it('should be able to call subscribe and receive new events', async () => {
            const aggregateId = shortid.generate();
            
            const query = {
                aggregateId: aggregateId,
                aggregate: 'vehicle',
                context: 'vehicle'
            };
            
            let counter = 0;
            const subscriptionToken = clusteredEventstore.subscribe(query, 0, (err, event, callback) => {
                counter++;
                callback();
            }, (err) => {
                debug(`playback error`, err);
            });
            
            const stream = await clusteredEventstore.getLastEventAsStreamAsync(query);
            
            Bluebird.promisifyAll(stream);
            
            const event = {
                name: "VEHICLE_CREATED",
                payload: {
                    vehicleId: aggregateId,
                    year: 2012,
                    make: "Honda",
                    model: "Jazz",
                    mileage: 1245
                }
            }
            
            stream.addEvent(event);
            await stream.commitAsync();
            
            let pollCounter = 0;
            while (pollCounter < 3) {
                if (counter > 0) {
                    break;
                } else {
                    debug(`events have not yet been played back. trying again in 1000ms`);
                    pollCounter++;
                    await sleep(1000);
                }
            }
            expect(pollCounter).toBeLessThan(3);
            expect(counter).toEqual(1);
        });
        
        it('should be able to call unsubscribe and stop receiving new events', async () => {
            const aggregateId = shortid.generate();
            
            const query = {
                aggregateId: aggregateId,
                aggregate: 'vehicle',
                context: 'vehicle'
            };
            
            let playbackCounter = 0;
            let counter = 0;
            const subscriptionToken = clusteredEventstore.subscribe(query, 0, (err, event, callback) => {
                playbackCounter++;
                console.log('SUB SUBSCRIBE GOT EVENT', subscriptionToken);
                callback();
            }, (err) => {
                debug(`playback error`, err);
            });
            const assertSubscriptionToken = clusteredEventstore.subscribe(query, 0, (err, event, callback) => {
                counter++;
                console.log('ASSERT SUBSCRIBE GOT EVENT', counter);
                callback();
            }, (err) => {
                debug(`playback error`, err);
            });
            
            const stream = await clusteredEventstore.getLastEventAsStreamAsync(query);
            
            Bluebird.promisifyAll(stream);
            
            const event = {
                name: "VEHICLE_CREATED",
                payload: {
                    vehicleId: aggregateId,
                    year: 2012,
                    make: "Honda",
                    model: "Jazz",
                    mileage: 1245
                }
            }
            
            stream.addEvent(event);
            await stream.commitAsync();
            
            let pollCounter = 0;
            while (pollCounter < 5) {
                if (playbackCounter > 0) {
                    break;
                } else {
                    debug(`events have not yet been played back. trying again in 1000ms`);
                    pollCounter++;
                    await sleep(1000);
                }
            }
            expect(pollCounter).toBeLessThan(5);
            clusteredEventstore.unsubscribe(assertSubscriptionToken);
            
            const event2 = {
                name: "VEHICLE_UPDATED",
                payload: {
                    vehicleId: aggregateId,
                    year: 2012,
                    make: "Honda",
                    model: "Jazz",
                    mileage: 1500
                }
            }
            stream.addEvent(event2);
            await stream.commitAsync();
            
            pollCounter = 0;
            while (pollCounter < 10) {
                console.log('CURRENT PLAYBACK COUNTER', playbackCounter);
                if (playbackCounter > 1) {
                    break;
                } else {
                    debug(`events have not yet been played back. trying again in 1000ms`);
                    pollCounter++;
                    await sleep(1000);
                }
            }
            expect(pollCounter).toBeLessThan(10);
            expect(counter).toEqual(1);
        });
    });
});
