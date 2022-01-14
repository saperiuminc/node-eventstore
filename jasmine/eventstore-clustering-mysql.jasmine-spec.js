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

fdescribe('eventstore clustering mysql tests', () => {
    const sleep = function(timeout) {
        return new Promise((resolve) => {
            setTimeout(resolve, timeout);
        })
    }

    const redisFactory = function() {
        const options = redisConfig;
        const redisClient = new Redis(options);
        const redisSubscriber = new Redis(options);
    
        return {
            createClient: function(type) {
                switch (type) {
                    case 'client':
                        return redisClient;
                    case 'bclient':
                        return new Redis(options); // always create a new one
                    case 'subscriber':
                        return redisSubscriber;
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
        // await compose.down({
        //     cwd: path.join(__dirname)
        // })
        debug('docker compose down finished');
    });

    it('should implement init', async () => {
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
            }]
        };

        const clustedEventstore = clusteredEs(config);

        Bluebird.promisifyAll(clustedEventstore);
        await clustedEventstore.initAsync();
    })

    it('should be able to call getEventStream', async () => {
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
            }]
        };
        const clustedEventstore = clusteredEs(config);

        Bluebird.promisifyAll(clustedEventstore);
        await clustedEventstore.initAsync();

        const aggregateId = shortId.generate();

        const stream = await clustedEventstore.getEventStreamAsync({
            aggregateId: aggregateId,
            aggregate: 'vehicle',
            context: 'vehicle'
        });

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

        const savedStream = await clustedEventstore.getEventStreamAsync({
            aggregateId: aggregateId,
            aggregate: 'vehicle',
            context: 'vehicle'
        });

        expect(savedStream.events[0].payload).toEqual(event);
    })

    it('should be able to call getLastEventAsStream', async () => {
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
            }]
        };
        const clustedEventstore = clusteredEs(config);

        Bluebird.promisifyAll(clustedEventstore);
        await clustedEventstore.initAsync();

        const aggregateId = shortId.generate();

        const stream = await clustedEventstore.getLastEventAsStreamAsync({
            aggregateId: aggregateId,
            aggregate: 'vehicle',
            context: 'vehicle'
        });

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

        const savedStream = await clustedEventstore.getLastEventAsStreamAsync({
            aggregateId: aggregateId,
            aggregate: 'vehicle',
            context: 'vehicle'
        });

        expect(savedStream.events[0].payload).toEqual(event);
    })

    it('should be able to call getLastEvent', async () => {
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
            }]
        };
        const clustedEventstore = clusteredEs(config);

        Bluebird.promisifyAll(clustedEventstore);
        await clustedEventstore.initAsync();

        const aggregateId = shortId.generate();

        const stream = await clustedEventstore.getLastEventAsStreamAsync({
            aggregateId: aggregateId,
            aggregate: 'vehicle',
            context: 'vehicle'
        });

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

        const lastEvent = await clustedEventstore.getLastEventAsync({
            aggregateId: aggregateId,
            aggregate: 'vehicle',
            context: 'vehicle'
        });

        expect(lastEvent.payload).toEqual(event);
    })

    it('should be able to call useEventPublisher', async (done) => {
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
            }]
        };
        const clustedEventstore = clusteredEs(config);

        Bluebird.promisifyAll(clustedEventstore);

        clustedEventstore.useEventPublisher(function(event, callback) {
            callback();
            done();
        });

        await clustedEventstore.initAsync();

        const aggregateId = shortId.generate();

        const stream = await clustedEventstore.getLastEventAsStreamAsync({
            aggregateId: aggregateId,
            aggregate: 'vehicle',
            context: 'vehicle'
        });

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
    })

    it('should be able to call defineEventMappings', async () => {
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
            }]
        };
        const clustedEventstore = clusteredEs(config);

        Bluebird.promisifyAll(clustedEventstore);

        clustedEventstore.defineEventMappings({
            id: 'id',
            commitId: 'commitId',
            commitSequence: 'commitSequence',
            commitStamp: 'commitStamp',
            streamRevision: 'streamRevision'
        });
    })

    it('should be able to call createSnapshot', async () => {
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
            }]
        };
        const clustedEventstore = clusteredEs(config);

        Bluebird.promisifyAll(clustedEventstore);

        await clustedEventstore.initAsync();

        await clustedEventstore.createSnapshotAsync({
            aggregateId: 'myAggregateId',
            aggregate: 'person',
            context: 'hr',
            data: {
                name: 'ryan'
            },
            revision: 1,
            version: 1 // optional
        });

    })

    it('should be able to call getSnapshot', async () => {
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
            }]
        };
        const clustedEventstore = clusteredEs(config);

        Bluebird.promisifyAll(clustedEventstore);

        await clustedEventstore.initAsync();

        const newSnapshot = {
            name: 'ryan'
        };
        await clustedEventstore.createSnapshotAsync({
            aggregateId: 'myAggregateId',
            aggregate: 'person',
            context: 'hr',
            data: newSnapshot,
            revision: 1,
            version: 1
        });

        const savedSnapshot = await clustedEventstore.getFromSnapshotAsync({
            aggregateId: 'myAggregateId',
            aggregate: 'person',
            context: 'hr'
        });

        expect(savedSnapshot.data).toEqual(newSnapshot);
    })

    it('should be able to call getUndispatchedEvents', async () => {
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
            }]
        };
        const clustedEventstore = clusteredEs(config);

        Bluebird.promisifyAll(clustedEventstore);

        await clustedEventstore.initAsync();

        // TODO: implement getUndispatchedEvents back
        await clustedEventstore.getUndispatchedEventsAsync({
            aggregateId: 'test'
        });
    })

    it('should be able to call setEventToDispatched', async () => {
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
            }]
        };
        const clustedEventstore = clusteredEs(config);

        Bluebird.promisifyAll(clustedEventstore);

        await clustedEventstore.initAsync();

        const aggregateId = shortId.generate();

        const stream = await clustedEventstore.getLastEventAsStreamAsync({
            aggregateId: aggregateId,
            aggregate: 'vehicle',
            context: 'vehicle'
        });

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

        const lastEvent = await clustedEventstore.getLastEventAsync({
            aggregateId: aggregateId,
            aggregate: 'vehicle',
            context: 'vehicle'
        })

        await clustedEventstore.setEventToDispatchedAsync(lastEvent);
    })

    fit('should run the projection with multiple contexts', async function() {
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
             projectionGroup: 'default'
        };
        const clustedEventstore = clusteredEs(config);
        Bluebird.promisifyAll(clustedEventstore);
        await clustedEventstore.initAsync();
        
        const projectionConfig = {
            projectionId: 'vehicle-list',
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
                context: 'vehicle'
            }, {
                context: 'vehicle'
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
        const stream = await clustedEventstore.getLastEventAsStreamAsync({
            context: 'vehicle',
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
        expect(projectionOffset).toBeGreaterThan(0);
    })
})