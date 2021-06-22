const Bluebird = require('bluebird');
const Docker = require('dockerode');
var docker = new Docker();
const knex = require('mysql');
const debug = require('debug')('DEBUG');
const Redis = require('ioredis');
const shortid = require('shortid');

jasmine.DEFAULT_TIMEOUT_INTERVAL = 20000;

const mySqlImageName = 'mysql:5.7.32';
const redisImageName = 'redis:latest';

const redisConfig = {
    host: 'localhost',
    port: 6379
}

const mysqlConfig = {
    host: 'localhost',
    port: 3306,
    user: 'root',
    password: 'root',
    database: 'eventstore'
}

const eventstoreConfig = {
    pollingTimeout: 1000
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

const sleep = function(timeout) {
    return new Promise((resolve) => {
        setTimeout(resolve, timeout);
    })
}

describe('evenstore classicist tests', function() {
    /**
     * @type {Docker.Container}
     */
    let mysqlContainer;

    /**
     * @type {Docker.Container}
     */
    let redisContainer;

    let eventstore;

    afterAll(async function() {
        try {
            debug('removing all containers');
            await Promise.all([mysqlContainer.stop(), redisContainer.stop()]);
            await Promise.all([mysqlContainer.remove(), redisContainer.remove()]);
            debug('all containers removed');
        } catch (error) {
            console.error('error in removing containers', error);
            throw error;
        }
    })

    beforeAll(async function() {

        debug(`pulling ${mySqlImageName}`);
        await docker.pull(mySqlImageName);
        debug(`pulling ${redisImageName}`);
        await docker.pull(redisImageName);

        debug('pulling images done');

        debug('creating and starting mysql container');

        mysqlContainer = await docker.createContainer({
            Image: mySqlImageName,
            Tty: true,
            Cmd: '--default-authentication-plugin=mysql_native_password',
            Env: [
                'MYSQL_USER=user',
                'MYSQL_PASSWORD=password',
                'MYSQL_ROOT_PASSWORD=root',
                'MYSQL_DATABASE=eventstore'
            ],
            HostConfig: {
                PortBindings: {
                    '3306/tcp': [{
                        HostPort: `${mysqlConfig.port}`
                    }]
                }
            }
        });

        await mysqlContainer.start();

        debug('creating and starting redis container');

        redisContainer = await docker.createContainer({
            Image: redisImageName,
            Tty: true,
            HostConfig: {
                PortBindings: {
                    '6379/tcp': [{
                        HostPort: `${redisConfig.port}`
                    }]
                }
            }
        });

        await redisContainer.start();

        debug('creating and starting containers done');

        const retryInterval = 1000;
        let connectCounter = 0;
        while (connectCounter < 10) {
            try {
                const mysqlConnection = knex.createConnection({
                    user: 'root',
                    password: 'root'
                });
                Bluebird.promisifyAll(mysqlConnection);
                await mysqlConnection.connectAsync();
                mysqlConnection.destroy();
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

        eventstore = require('../index')({
            type: 'mysql',
            host: mysqlConfig.host,
            port: mysqlConfig.port,
            user: mysqlConfig.user,
            password: mysqlConfig.password,
            database: mysqlConfig.database,
            connectionPoolLimit: 10,
            // projections-specific configuration below
            redisCreateClient: redisFactory().createClient,
            listStore: {
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
            eventCallbackTimeout: 1000,
            lockTimeToLive: 1000,
            pollingTimeout: eventstoreConfig.pollingTimeout, // optional,
            pollingMaxRevisions: 100,
            errorMaxRetryCount: 2,
            errorRetryExponent: 2,
            playbackEventJobCount: 10,
            context: 'vehicle'
        });

        Bluebird.promisifyAll(eventstore);
        await eventstore.initAsync();
    });

    beforeEach(async function() {
        Bluebird.promisifyAll(eventstore.store);
        await eventstore.store.clearAsync();

        const projections = await eventstore.getProjectionsAsync();
        for (let index = 0; index < projections.length; index++) {
            const projection = projections[index];
            await eventstore.deleteProjectionAsync(projection.projectionId);
        }
    });

    it('should create the projection', async function() {
        const projectionConfig = {
            projectionId: 'vehicle-list',
            projectionName: 'Vehicle Listing',
            playbackInterface: {
                $init: function() {
                    return {
                        count: 0
                    }
                }
            },
            query: {
                context: 'vehicle',
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

        await eventstore.projectAsync(projectionConfig);
        const storedProjection = await eventstore.getProjectionAsync(projectionConfig.projectionId);

        expect(storedProjection).toBeTruthy();
    });

    it('should get projections list', async function() {
        const projectionConfig = {
            projectionId: 'vehicle-list',
            projectionName: 'Vehicle Listing',
            playbackInterface: {
                $init: function() {
                    return {
                        count: 0
                    }
                }
            },
            query: {
                context: 'vehicle',
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
            context: 'vehicle'
        };

        await eventstore.projectAsync(projectionConfig);
        const projections = await eventstore.getProjectionsAsync();
        expect(projections[0].projectionId).toEqual(projectionConfig.projectionId); 
        expect(projections[0].context).toEqual(projectionConfig.context); 
        expect(projections.length).toEqual(1);
    });
    
    it('should delete the projection', async function() {
        const projectionConfig = {
            projectionId: 'vehicle-list',
            projectionName: 'Vehicle Listing',
            playbackInterface: {
                $init: function() {
                    return {
                        count: 0
                    }
                }
            },
            query: {
                context: 'vehicle',
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

        await eventstore.projectAsync(projectionConfig);
        await eventstore.deleteProjectionAsync(projectionConfig.projectionId);
        const storedProjection = await eventstore.getProjectionAsync(projectionConfig.projectionId);

        expect(storedProjection).toBeNull();
    });

    it('should reset the projection', async function() {
        const projectionConfig = {
            projectionId: 'vehicle-list',
            projectionName: 'Vehicle Listing',
            playbackInterface: {
                $init: function() {
                    return {
                        count: 0
                    }
                }
            },
            query: {
                context: 'vehicle',
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

        await eventstore.projectAsync(projectionConfig);
        await eventstore.resetProjectionAsync(projectionConfig.projectionId);
        const projection = await eventstore.getProjectionAsync(projectionConfig.projectionId);

        expect(projection.offset).toEqual(0);
    });

    it('should run the projection', async function() {
        const projectionConfig = {
            projectionId: 'vehicle-list',
            projectionName: 'Vehicle Listing',
            playbackInterface: {
                $init: function() {
                    return {
                        count: 0
                    }
                }
            },
            query: {
                context: 'vehicle',
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

        await eventstore.projectAsync(projectionConfig);
        await eventstore.runProjectionAsync(projectionConfig.projectionId, false);
        const projection = await eventstore.getProjectionAsync(projectionConfig.projectionId);

        expect(projection.state).toEqual('running');
    });

    

    it('should pause the projection', async function() {
        const projectionConfig = {
            projectionId: 'vehicle-list',
            projectionName: 'Vehicle Listing',
            playbackInterface: {
                $init: function() {
                    return {
                        count: 0
                    }
                }
            },
            query: {
                context: 'vehicle',
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

        await eventstore.projectAsync(projectionConfig);
        await eventstore.pauseProjectionAsync(projectionConfig.projectionId);
        const projection = await eventstore.getProjectionAsync(projectionConfig.projectionId);

        expect(projection.state).toEqual('paused');
    });

    it('should set the projection to faulted if there is an event handler error', async function() {
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
                    
                },
                VEHICLE_UPDATED: async function(state, event, funcs) {
                    throw new Error('your fault!');
                }
            },
            query: {
                context: 'vehicle',
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

        await eventstore.projectAsync(projectionConfig);
        await eventstore.startAllProjectionsAsync();

        await eventstore.runProjectionAsync(projectionConfig.projectionId, false);

        const vehicleId = shortid.generate();
        const stream = await eventstore.getLastEventAsStreamAsync({
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
        while (pollCounter < 10) {
            projection = await eventstore.getProjectionAsync(projectionConfig.projectionId);
            if (projection.state == 'faulted') {
                break;
            } else {
                debug(`projection.state ${projection.state} is not faulted. trying again in 1000ms`);
                await sleep(1000);
            }
        }

        expect(pollCounter).toBeLessThan(10);
        expect(projection.error).toBeTruthy();
        expect(projection.errorEvent).toBeTruthy();
        expect(projection.errorOffset).toBeGreaterThan(0);
        expect(projection.offset).toEqual(1);
    });

    it('should force run the projection when there is an error', async function() {
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
                    throw new Error('your fault!');
                },
                VEHICLE_UPDATED: async function(state, event, funcs) {
                    
                }
            },
            query: {
                context: 'vehicle',
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

        await eventstore.projectAsync(projectionConfig);
        await eventstore.startAllProjectionsAsync();

        await eventstore.runProjectionAsync(projectionConfig.projectionId, false);

        const vehicleId = shortid.generate();
        const stream = await eventstore.getLastEventAsStreamAsync({
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
        while (pollCounter < 10) {
            projection = await eventstore.getProjectionAsync(projectionConfig.projectionId);
            if (projection.state == 'faulted') {
                break;
            } else {
                debug(`projection.state ${projection.state} is not faulted. trying again in 1000ms`);
                await sleep(1000);
            }
        }

        expect(pollCounter).toBeLessThan(10);

        projection = null;
        pollCounter = 0;

        await eventstore.runProjectionAsync(projectionConfig.projectionId, true);

        let lastProcessedDate;
        while (pollCounter < 10) {
            projection = await eventstore.getProjectionAsync(projectionConfig.projectionId);
            if (!lastProcessedDate) {
                lastProcessedDate = projection.processedDate;
            }

            debug('projection got', projection);
            if (projection.processedDate > lastProcessedDate) {
                break;
            } else {
                debug(`projection has not processed yet. trying again in 1000ms`);
                await sleep(1000);
            }
        }

        expect(projection.offset).toEqual(2);
        expect(projection.state).toEqual('running');
        expect(projection.isIdle).toEqual(0);
    });

    it('should add data to the playbacklist', async function() {
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
                    const playbackList = await funcs.getPlaybackList('vehicle_list');
                    const eventPayload = event.payload.payload;
                    const data = {
                        vehicleId: eventPayload.vehicleId,
                        year: eventPayload.year,
                        make: eventPayload.make,
                        model: eventPayload.model,
                        mileage: eventPayload.mileage
                    };
                    console.log('adding', event.aggregateId);
                    await playbackList.add(event.aggregateId, event.streamRevision, data, {});
                }
            },
            query: {
                context: 'vehicle',
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

        await eventstore.projectAsync(projectionConfig);
        await eventstore.startAllProjectionsAsync();

        await eventstore.runProjectionAsync(projectionConfig.projectionId, false);


        const vehicleId = shortid.generate();
        const stream = await eventstore.getLastEventAsStreamAsync({
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
        while (pollCounter < 10) {
            const projection = await eventstore.getProjectionAsync(projectionConfig.projectionId);
            if (projection.processedDate) {
                break;
            } else {
                debug(`projection has not processed yet. trying again in 1000ms`);
                await sleep(1000);
            }
        }

        expect(pollCounter).toBeLessThan(10);

        const playbackList = eventstore.getPlaybackList('vehicle_list');
        const result = await playbackList.get(vehicleId);
        expect(result.data).toEqual(event.payload);
    });

    it('should update the playbacklist data', async function() {
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
                context: 'vehicle',
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

        await eventstore.projectAsync(projectionConfig);
        await eventstore.startAllProjectionsAsync();

        await eventstore.runProjectionAsync(projectionConfig.projectionId, false);

        const vehicleId = shortid.generate();
        const stream = await eventstore.getLastEventAsStreamAsync({
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

        let pollCounter = 0;
        while (pollCounter < 10) {
            const projection = await eventstore.getProjectionAsync(projectionConfig.projectionId);
            if (projection.processedDate) {
                break;
            } else {
                debug(`projection has not processed yet. trying again in 1000ms`);
                await sleep(1000);
            }
        }

        expect(pollCounter).toBeLessThan(10);

        const playbackList = eventstore.getPlaybackList('vehicle_list');
        const result = await playbackList.get(vehicleId);
        expect(result.data).toEqual(event2.payload);
    });

    it('should delete the playbacklist data', async function() {
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
                context: 'vehicle',
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

        await eventstore.projectAsync(projectionConfig);
        await eventstore.startAllProjectionsAsync();

        await eventstore.runProjectionAsync(projectionConfig.projectionId, false);

        const vehicleId = shortid.generate();
        const stream = await eventstore.getLastEventAsStreamAsync({
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

        const event2 = {
            name: "VEHICLE_DELETED",
            payload: {
                vehicleId: vehicleId
            }
        }
        stream.addEvent(event2);
        await stream.commitAsync();

        let pollCounter = 0;
        while (pollCounter < 10) {
            const projection = await eventstore.getProjectionAsync(projectionConfig.projectionId);
            if (projection.processedDate) {
                break;
            } else {
                debug(`projection has not processed yet. trying again in 1000ms`);
                await sleep(1000);
            }
        }

        expect(pollCounter).toBeLessThan(10);

        const playbackList = eventstore.getPlaybackList('vehicle_list');
        const result = await playbackList.get(vehicleId);
        expect(result).toBeNull();
    });

    it('should emit playbackError on playback error', async (done) => {
        const errorMessage = 'test-error';
        const projectionConfig = {
            projectionId: 'vehicle-list-error',
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
                context: 'vehicle',
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
        
        await eventstore.projectAsync(projectionConfig);
        await eventstore.startAllProjectionsAsync();

        await eventstore.runProjectionAsync(projectionConfig.projectionId, false);

        const vehicleId = shortid.generate();
        const stream = await eventstore.getLastEventAsStreamAsync({
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

        const listener = (error) => {
            expect(errorMessage).toEqual(error.message);
            eventstore.off('playbackError', listener);
            done();
        };

        eventstore.on('playbackError', listener);

        stream.addEvent(event);
        await stream.commitAsync();
    });

    it('should emit playbackSuccess on playback', async (done) => {
        const errorMessage = 'test-error';
        const projectionConfig = {
            projectionId: 'vehicle-list-error',
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
                context: 'vehicle',
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
        
        await eventstore.projectAsync(projectionConfig);
        await eventstore.startAllProjectionsAsync();

        await eventstore.runProjectionAsync(projectionConfig.projectionId, false);

        const vehicleId = shortid.generate();
        const stream = await eventstore.getLastEventAsStreamAsync({
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

        const listener = (data) => {
            expect(data.projectionId).toEqual(projectionConfig.projectionId);
            expect(data.eventsCount).toEqual(1);
            eventstore.off('playbackSuccess', listener);
            done();
        };

        eventstore.on('playbackSuccess', listener);

        stream.addEvent(event);
        await stream.commitAsync();
    });

    xit('should close the eventstore projection', async (done) => {
        const eventstore2 = require('../index')({
            type: 'mysql',
            host: mysqlConfig.host,
            port: mysqlConfig.port,
            user: mysqlConfig.user,
            password: mysqlConfig.password,
            database: mysqlConfig.database,
            connectionPoolLimit: 10,
            // projections-specific configuration below
            redisCreateClient: redisFactory().createClient,
            listStore: {
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
            eventCallbackTimeout: 1000,
            lockTimeToLive: 1000,
            pollingTimeout: eventstoreConfig.pollingTimeout, // optional,
            pollingMaxRevisions: 100,
            errorMaxRetryCount: 2,
            errorRetryExponent: 2,
            playbackEventJobCount: 10,
            context: 'vehicle'
        });

        Bluebird.promisifyAll(eventstore2);
        await eventstore2.initAsync();

        const projectionConfig = {
            projectionId: 'vehicle-list-close',
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
                context: 'vehicle',
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
        
        await eventstore2.projectAsync(projectionConfig);
        await eventstore2.startAllProjectionsAsync();

        await eventstore2.runProjectionAsync(projectionConfig.projectionId, false);

        const vehicleId = shortid.generate();
        const stream = await eventstore2.getLastEventAsStreamAsync({
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

        const lastProjectionCheck = await eventstore2.getProjectionAsync(projectionConfig.projectionId);
        setTimeout(async () => {
            const latestProjectionCheck = await eventstore2.getProjectionAsync(projectionConfig.projectionId);
            expect(lastProjectionCheck.offset).toEqual(latestProjectionCheck.offset);
            done();
        }, eventstoreConfig.pollingTimeout * 2); // just wait for twice of polling. offset should not have changed

        await eventstore2.closeAsync();
    });
});