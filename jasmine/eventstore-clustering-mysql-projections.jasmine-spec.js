const compose = require('docker-compose');
const path = require('path');
const debug = require('debug')('eventstore:clustering:mysql');
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
            
            // for streambuffer on
            stream.addEvent(event);
            // add 4 more events


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

            // subscribe new token
            // will catchup
            // expect 2nd token to receive 5 callbacks
            // add event
            // expect both subscribers to receive event
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

        describe('stream buffer on', () => {
            let clusteredEventstoreStreamBufferOn;
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
                        enableSubscriptionEventStreamBuffer: true,
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
                    clusteredEventstoreStreamBufferOn = clusteredEs(config);
                    Bluebird.promisifyAll(clusteredEventstoreStreamBufferOn);
                    Bluebird.promisifyAll(clusteredEventstoreStreamBufferOn.store);
                    await clusteredEventstoreStreamBufferOn.initAsync();
                } catch (error) {
                    console.error('error in beforeAll', error);
                }
                
            }, 60000);
            
            afterEach(async function() {
                // console.log('AFTER EACH');
                const projections = await clusteredEventstoreStreamBufferOn.getProjectionsAsync();
                for (const projection of projections) {
                    const projectionId = projection.projectionId;
                    await clusteredEventstoreStreamBufferOn.resetProjectionAsync(projectionId);
                    await clusteredEventstoreStreamBufferOn.deleteProjectionAsync(projectionId);
                }
                
                await clusteredEventstoreStreamBufferOn.store.clearAsync();
                
                clusteredEventstoreStreamBufferOn.removeAllListeners('rebalance');
                await clusteredEventstoreStreamBufferOn.closeAsync();
                // console.log('AFTER EACH DONE');
            }, 60000);

            it('should be able to call subscribe and receive new events', async () => {
                const aggregateId = shortid.generate();
                
                const query = {
                    aggregateId: aggregateId,
                    aggregate: 'vehicle',
                    context: 'vehicle'
                };
                
                let counter1 = 0;
                const subscriptionToken1 = clusteredEventstoreStreamBufferOn.subscribe(query, 0, (err, event, callback) => {
                    counter1++;
                    callback();
                }, (err) => {
                    debug(`playback error`, err);
                });
                
                const stream = await clusteredEventstoreStreamBufferOn.getLastEventAsStreamAsync(query);
                Bluebird.promisifyAll(stream);
                
                const events = [{
                    name: "VEHICLE_CREATED",
                    payload: {
                        vehicleId: aggregateId,
                        year: 2012,
                        make: "Honda",
                        model: "Jazz",
                        mileage: 1245
                    }
                }, {
                    name: "VEHICLE_UPDATED",
                    payload: {
                        vehicleId: aggregateId,
                        mileage: 1246
                    }
                }, {
                    name: "VEHICLE_UPDATED",
                    payload: {
                        vehicleId: aggregateId,
                        mileage: 1247
                    }
                }, {
                    name: "VEHICLE_UPDATED",
                    payload: {
                        vehicleId: aggregateId,
                        mileage: 1248
                    }
                }, {
                    name: "VEHICLE_UPDATED",
                    payload: {
                        vehicleId: aggregateId,
                        mileage: 1249
                    }
                }];

                events.forEach((event) => {
                    stream.addEvent(event);
                });
                await stream.commitAsync();
                
                let pollCounter = 0;
                while (pollCounter < 3) {
                    if (counter1 > 0) {
                        break;
                    } else {
                        debug(`events have not yet been played back. trying again in 1000ms`);
                        pollCounter++;
                        await sleep(1000);
                    }
                }
                expect(pollCounter).toBeLessThan(3);
                expect(counter1).toEqual(5);
    
                // subscribe new token
                let counter2 = 0
                const subscriptionToken2 = clusteredEventstoreStreamBufferOn.subscribe(query, 0, (err, event, callback) => {
                    counter2++;
                    callback();
                }, (err) => {
                    debug(`playback error`, err);
                });

                // will catchup
                pollCounter = 0;
                while (pollCounter < 3) {
                    if (counter2 > 0) {
                        break;
                    } else {
                        debug(`events have not yet been played back. trying again in 1000ms`);
                        pollCounter++;
                        await sleep(1000);
                    }
                }
                // expect 2nd token to receive 5 callbacks
                expect(pollCounter).toBeLessThan(3);
                expect(counter2).toEqual(5);
                
                // add event
                const event6 = {
                    name: "VEHICLE_CREATED",
                    payload: {
                        vehicleId: aggregateId,
                        year: 2012,
                        make: "Honda",
                        model: "Jazz",
                        mileage: 1245
                    }
                }
                
                stream.addEvent(event6);
                await stream.commitAsync();

                pollCounter = 0;
                counter1 = 0;
                counter2 = 0;
                while (pollCounter < 3) {
                    if (counter1 > 0) {
                        break;
                    } else {
                        debug(`events have not yet been played back. trying again in 1000ms`);
                        pollCounter++;
                        await sleep(1000);
                    }
                }

                // expect both subscribers to receive event
                expect(pollCounter).toBeLessThan(3);
                expect(counter1).toEqual(1);
                expect(counter2).toEqual(1);
            });
        });

        describe('stream buffer off', () => {
            let clusteredEventstoreStreamBufferOff;
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
                        enableSubscriptionEventStreamBuffer: false,
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
                    clusteredEventstoreStreamBufferOff = clusteredEs(config);
                    Bluebird.promisifyAll(clusteredEventstoreStreamBufferOff);
                    Bluebird.promisifyAll(clusteredEventstoreStreamBufferOff.store);
                    await clusteredEventstoreStreamBufferOff.initAsync();
                } catch (error) {
                    console.error('error in beforeAll', error);
                }
                
            }, 60000);
            
            afterEach(async function() {
                // console.log('AFTER EACH');
                const projections = await clusteredEventstoreStreamBufferOff.getProjectionsAsync();
                for (const projection of projections) {
                    const projectionId = projection.projectionId;
                    await clusteredEventstoreStreamBufferOff.resetProjectionAsync(projectionId);
                    await clusteredEventstoreStreamBufferOff.deleteProjectionAsync(projectionId);
                }
                
                await clusteredEventstoreStreamBufferOff.store.clearAsync();
                
                clusteredEventstoreStreamBufferOff.removeAllListeners('rebalance');
                await clusteredEventstoreStreamBufferOff.closeAsync();
                // console.log('AFTER EACH DONE');
            }, 60000);

            it('should be able to call subscribe and receive new events', async () => {
                const aggregateId = shortid.generate();
                
                const query = {
                    aggregateId: aggregateId,
                    aggregate: 'vehicle',
                    context: 'vehicle'
                };
                
                let counter1 = 0;
                const subscriptionToken1 = clusteredEventstoreStreamBufferOff.subscribe(query, 0, (err, event, callback) => {
                    counter1++;
                    callback();
                }, (err) => {
                    debug(`playback error`, err);
                });
                
                const stream = await clusteredEventstoreStreamBufferOff.getLastEventAsStreamAsync(query);
                
                Bluebird.promisifyAll(stream);
                
                const events = [{
                    name: "VEHICLE_CREATED",
                    payload: {
                        vehicleId: aggregateId,
                        year: 2012,
                        make: "Honda",
                        model: "Jazz",
                        mileage: 1245
                    }
                }, {
                    name: "VEHICLE_UPDATED",
                    payload: {
                        vehicleId: aggregateId,
                        mileage: 1246
                    }
                }, {
                    name: "VEHICLE_UPDATED",
                    payload: {
                        vehicleId: aggregateId,
                        mileage: 1247
                    }
                }, {
                    name: "VEHICLE_UPDATED",
                    payload: {
                        vehicleId: aggregateId,
                        mileage: 1248
                    }
                }, {
                    name: "VEHICLE_UPDATED",
                    payload: {
                        vehicleId: aggregateId,
                        mileage: 1249
                    }
                }];

                events.forEach((event) => {
                    stream.addEvent(event);
                });
                await stream.commitAsync();
                
                let pollCounter = 0;
                while (pollCounter < 3) {
                    if (counter1 > 0) {
                        break;
                    } else {
                        debug(`events have not yet been played back. trying again in 1000ms`);
                        pollCounter++;
                        await sleep(1000);
                    }
                }
                expect(pollCounter).toBeLessThan(3);
                expect(counter1).toEqual(5);
    
                // subscribe new token
                let counter2 = 0
                const subscriptionToken2 = clusteredEventstoreStreamBufferOff.subscribe(query, 0, (err, event, callback) => {
                    counter2++;
                    callback();
                }, (err) => {
                    debug(`playback error`, err);
                });

                // will catchup
                pollCounter = 0;
                while (pollCounter < 3) {
                    if (counter2 > 0) {
                        break;
                    } else {
                        debug(`events have not yet been played back. trying again in 1000ms`);
                        pollCounter++;
                        await sleep(1000);
                    }
                }
                // expect 2nd token to receive 5 callbacks
                expect(counter2).toEqual(5);
                
                // add event
                const event6 = {
                    name: "VEHICLE_CREATED",
                    payload: {
                        vehicleId: aggregateId,
                        year: 2012,
                        make: "Honda",
                        model: "Jazz",
                        mileage: 1245
                    }
                }
                
                stream.addEvent(event6);
                await stream.commitAsync();

                pollCounter = 0;
                counter1 = 0;
                counter2 = 0;
                while (pollCounter < 3) {
                    if (counter1 > 0) {
                        break;
                    } else {
                        debug(`events have not yet been played back. trying again in 1000ms`);
                        pollCounter++;
                        await sleep(1000);
                    }
                }

                // expect both subscribers to receive event
                expect(counter1).toEqual(1);
                expect(counter2).toEqual(1);
            });
        });
    });
});
