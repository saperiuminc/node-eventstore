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

describe('eventstore mysql classicist tests', function() {
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
        try {
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
            while (connectCounter < 30) {
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
            });

            Bluebird.promisifyAll(eventstore);
            await eventstore.initAsync();
        } catch (error) {
            console.error('error in beforeAll', error);
        }
    }, 60000);

    beforeEach(async function() {
        Bluebird.promisifyAll(eventstore.store);
        await eventstore.store.clearAsync();
        await eventstore.closeProjectionEventStreamBuffersAsync();

        const projections = await eventstore.getProjectionsAsync();
        for (let index = 0; index < projections.length; index++) {
            const projection = projections[index];
            await eventstore.deleteProjectionAsync(projection.projectionId);
        }
    }, 60000);

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

    it('should run the projection with multiple contexts', async function() {
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
        let projection;
        while (pollCounter < 10) {
            projection = await eventstore.getProjectionAsync(projectionConfig.projectionId);
            if (projection.processedDate && projection.state == 'running') {
                break;
            } else {
                debug(`projection has not processed yet. trying again in 1000ms`);
                await sleep(1000);
            }
        }

        expect(pollCounter).toBeLessThan(10);
        expect(projection.offset).toEqual(1);
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
                debug('projection.state is faulted. continuing with test');
                break;
            } else {
                debug(`projection.state ${projection.state} is not faulted. trying again in 1000ms`);
                await sleep(1000);
            }
        }

        debug('got projection', projection);

        expect(pollCounter).toBeLessThan(20);
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

        let lastOffset;
        while (pollCounter < 10) {
            projection = await eventstore.getProjectionAsync(projectionConfig.projectionId);
            if (!lastOffset) {
                lastOffset = projection.offset;
            }

            if (projection.offset > lastOffset) {
                debug('offset changed', lastOffset, projection.offset);
                break;
            } else {
                debug(`projection has not processed yet. trying again in 1000ms`, lastOffset, projection.offset);
                await sleep(1000);
            }
        }

        expect(projection.offset).toEqual(2);
        expect(projection.state).toEqual('running');
        expect(projection.isIdle).toEqual(0);
    });

    it('should add and update data to the stateList', async function() {
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
                context: 'vehicle',
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
            if (projection.processedDate && projection.state == 'running') {
                break;
            } else {
                debug(`projection has not processed yet. trying again in 1000ms`);
                await sleep(1000);
            }
        }

        expect(pollCounter).toBeLessThan(10);

        const playbackList = eventstore.getPlaybackList('vehicle_list');

        const filteredResults = await playbackList.query(0, 1, [{
            field: 'vehicleId',
            operator: 'is',
            value: vehicleId
        }], null);

        const result = filteredResults.rows[0];
        expect(result.data).toEqual(event2.payload);
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

        const filteredResults = await playbackList.query(0, 1, [{
            field: 'vehicleId',
            operator: 'is',
            value: vehicleId
        }], null);

        const result = filteredResults.rows[0];

        expect(result.data).toEqual(event2.payload);
    });

    it('should batch update the playbacklist data', async function() {
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
                }
            },
            query: [{
                context: 'vehicle',
                aggregate: 'vehicle'
            }, {
                context: 'profile',
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

        await eventstore.projectAsync(projectionConfig);
        await eventstore.startAllProjectionsAsync();

        await eventstore.runProjectionAsync(projectionConfig.projectionId, false);

        const dealershipId = 'CARWAVE';
        const newDealershipName = 'NEW Carwave Auctions';
        for (let i = 0; i < 2; i++) {
            const vehicleId = shortid.generate();

            const stream = await eventstore.getLastEventAsStreamAsync({
                context: 'vehicle',
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

        const dealershipStream = await eventstore.getLastEventAsStreamAsync({
            context: 'profile',
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

        const playbackList = eventstore.getPlaybackList('vehicle_batch_list');

        const filteredResults = await playbackList.query(0, 2, [], null);

        expect(filteredResults.rows[0].data.dealershipName).toEqual(newDealershipName);
        expect(filteredResults.rows[1].data.dealershipName).toEqual(newDealershipName);
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
        const filteredResults = await playbackList.query(0, 1, [{
            field: 'vehicleId',
            operator: 'is',
            value: vehicleId
        }], null);

        const result = filteredResults.rows[0];
        expect(result).toBeFalsy();
    });

    it('should get data from the playbacklistview', async function() {
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

        await eventstore.registerPlaybackListViewAsync('vehicle-list',
            'SELECT list.row_id, list.row_revision, list.row_json, list.meta_json FROM vehicle_list list @@where @@order @@limit',
            'SELECT COUNT(1) as total_count FROM vehicle_list list @@where;', {
                alias: {}
            });

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

        const playbackListView = await eventstore.getPlaybackListViewAsync('vehicle-list');
        Bluebird.promisifyAll(playbackListView);

        const filteredResults = await playbackListView.queryAsync(0, 1, [{
            field: 'vehicleId',
            operator: 'is',
            value: vehicleId
        }], null);

        const result = filteredResults.rows[0];
        expect(result.data).toEqual(event.payload);
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
                partitionBy: '',
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
        
            await eventstore.projectAsync(projectionConfig);
            await eventstore.startAllProjectionsAsync();
        
            await eventstore.runProjectionAsync(projectionConfig.projectionId, false);
        
            await eventstore.registerPlaybackListViewAsync('keyset-list-next',
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
        
            for (let i = 0; i < 5; i++) {
                const keysetId = shortid.generate();
                const stream = await eventstore.getLastEventAsStreamAsync({
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
            while (pollCounter < 10) {
                const projection = await eventstore.getProjectionAsync(projectionConfig.projectionId);
                if (projection.processedDate && projection.offset === 5) {
                    break;
                } else {
                    debug(`projection has not processed yet. trying again in 1000ms`);
                    await sleep(1000);
                }
            }
        
            expect(pollCounter).toBeLessThan(10);
        
            const playbackListView = await eventstore.getPlaybackListViewAsync('keyset-list-next');
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
                partitionBy: '',
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
        
            await eventstore.projectAsync(projectionConfig);
            await eventstore.startAllProjectionsAsync();
        
            await eventstore.runProjectionAsync(projectionConfig.projectionId, false);
        
            await eventstore.registerPlaybackListViewAsync('keyset-list-prev',
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
        
            for (let i = 0; i < 5; i++) {
                const keysetId = shortid.generate();
                const stream = await eventstore.getLastEventAsStreamAsync({
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
            while (pollCounter < 10) {
                const projection = await eventstore.getProjectionAsync(projectionConfig.projectionId);
                if (projection.processedDate && projection.offset === 5) {
                    break;
                } else {
                    debug(`projection has not processed yet. trying again in 1000ms`);
                    await sleep(1000);
                }
            }
        
            expect(pollCounter).toBeLessThan(10);
        
            const playbackListView = await eventstore.getPlaybackListViewAsync('keyset-list-prev');
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
                partitionBy: '',
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
    
            await eventstore.projectAsync(projectionConfig);
            await eventstore.startAllProjectionsAsync();
    
            await eventstore.runProjectionAsync(projectionConfig.projectionId, false);
    
            await eventstore.registerPlaybackListViewAsync('keyset-list-last',
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
    
            for (let i = 0; i < 5; i++) {
                const keysetId = shortid.generate();
                const stream = await eventstore.getLastEventAsStreamAsync({
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
            while (pollCounter < 10) {
                const projection = await eventstore.getProjectionAsync(projectionConfig.projectionId);
                if (projection.processedDate && projection.offset === 5) {
                    break;
                } else {
                    debug(`projection has not processed yet. trying again in 1000ms`);
                    await sleep(1000);
                }
            }
    
            expect(pollCounter).toBeLessThan(10);
    
            const playbackListView = await eventstore.getPlaybackListViewAsync('keyset-list-last');
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
                partitionBy: '',
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
    
            await eventstore.projectAsync(projectionConfig);
            await eventstore.startAllProjectionsAsync();
    
            await eventstore.runProjectionAsync(projectionConfig.projectionId, false);
    
            await eventstore.registerPlaybackListViewAsync('keyset-list-last-2',
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
    
            for (let i = 0; i < 6; i++) {
                const keysetId = shortid.generate();
                const stream = await eventstore.getLastEventAsStreamAsync({
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
            while (pollCounter < 10) {
                const projection = await eventstore.getProjectionAsync(projectionConfig.projectionId);
                if (projection.processedDate && projection.offset === 6) {
                    break;
                } else {
                    debug(`projection has not processed yet. trying again in 1000ms`);
                    await sleep(1000);
                }
            }
    
            expect(pollCounter).toBeLessThan(10);
    
            const playbackListView = await eventstore.getPlaybackListViewAsync('keyset-list-last-2');
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
                partitionBy: '',
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
        
            await eventstore.projectAsync(projectionConfig);
            await eventstore.startAllProjectionsAsync();
        
            await eventstore.runProjectionAsync(projectionConfig.projectionId, false);
        
            await eventstore.registerPlaybackListViewAsync('keyset-list-null',
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
        
            for (let i = 0; i < 5; i++) {
                const keysetId = shortid.generate();
                const stream = await eventstore.getLastEventAsStreamAsync({
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
            while (pollCounter < 10) {
                const projection = await eventstore.getProjectionAsync(projectionConfig.projectionId);
                if (projection.processedDate && projection.offset === 5) {
                    break;
                } else {
                    debug(`projection has not processed yet. trying again in 1000ms`);
                    await sleep(1000);
                }
            }
        
            expect(pollCounter).toBeLessThan(10);
        
            const playbackListView = await eventstore.getPlaybackListViewAsync('keyset-list-null');
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
                partitionBy: '',
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
        
            await eventstore.projectAsync(projectionConfig);
            await eventstore.startAllProjectionsAsync();
        
            await eventstore.runProjectionAsync(projectionConfig.projectionId, false);
        
            await eventstore.registerPlaybackListViewAsync('keyset-list-null-2',
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
        
            for (let i = 0; i < 5; i++) {
                const keysetId = shortid.generate();
                const stream = await eventstore.getLastEventAsStreamAsync({
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
            while (pollCounter < 10) {
                const projection = await eventstore.getProjectionAsync(projectionConfig.projectionId);
                if (projection.processedDate && projection.offset === 5) {
                    break;
                } else {
                    debug(`projection has not processed yet. trying again in 1000ms`);
                    await sleep(1000);
                }
            }
        
            expect(pollCounter).toBeLessThan(10);
        
            const playbackListView = await eventstore.getPlaybackListViewAsync('keyset-list-null-2');
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

    describe('querying playbackListViews with nested filterGroups', function() {
        it('should filter the playbackList properly', async function() {
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
                    fields: [
                        {
                            name: 'vehicleId',
                            type: 'string'
                        }
                    ]
                }
            };
    
            await eventstore.projectAsync(projectionConfig);
            await eventstore.startAllProjectionsAsync();
    
            await eventstore.runProjectionAsync(projectionConfig.projectionId, false);
    
            await eventstore.registerPlaybackListViewAsync('vehicle-list',
                'SELECT list.row_id, list.row_revision, list.row_json, list.meta_json FROM vehicle_list list @@where @@order @@limit',
                'SELECT COUNT(1) as total_count FROM vehicle_list list @@where;', {
                    alias: {}
                });
    
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
    
            const playbackListView = await eventstore.getPlaybackListViewAsync('vehicle-list');
            Bluebird.promisifyAll(playbackListView);
    
            const filteredResults = await playbackListView.queryAsync(0, 1, [
                {
                    group: 'vehicle-1',
                    groupBooleanOperator: 'or',
                    filters: [
                        {
                            field: 'vehicleId',
                            operator: 'is',
                            value: vehicleId,
                            group: 'subgroup-1',
                            groupBooleanOperator: 'and'
                        },
                        {
                            field: 'vehicleId',
                            operator: 'exists',
                            group: 'subgroup-1',
                            groupBooleanOperator: 'and'
                        }
                    ]
                },
                {
                    group: 'vehicle-1',
                    groupBooleanOperator: 'or',
                    filters: [
                        {
                            field: 'vehicleId',
                            operator: 'notExists'
                        }
                    ]
                }
            ], null);
    
            const result = filteredResults.rows[0];
            expect(result.data).toEqual(event.payload);
        });
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

    it('should handle batch events', async (done) => {
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
            },
            pollingMaxRevisions: 10,
            concurrentEventNames: ['VEHICLE_CREATED', 'VEHICLE_UPDATED'],
            concurrencyCount: 5
        };

        const expectedEventsCount = 10;
        for (let i = 0; i < expectedEventsCount; i++) {
            
            const vehicleId = shortid.generate();
            const stream = await eventstore.getLastEventAsStreamAsync({
                context: 'vehicle',
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

        
        await eventstore.projectAsync(projectionConfig);
        await eventstore.startAllProjectionsAsync();

        await eventstore.runProjectionAsync(projectionConfig.projectionId, false);

        const waitForRebalance = function() {
            return new Promise((resolve) => {
                eventstore.on('rebalance', resolve);
            });
        }

        let eventsCount = 0;
        const listener = (data) => {
            eventsCount += data.eventsCount;

            if (eventsCount == expectedEventsCount) {
                done();
            }
        };

        eventstore.on('playbackSuccess', listener);

        await waitForRebalance();
      
    });

    it('should add a projection with a proper offset if the configured fromOffset is set to latest', async function() {
        const initialProjectionConfig = {
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

        await eventstore.projectAsync(initialProjectionConfig);
        await eventstore.startAllProjectionsAsync();
        await eventstore.runProjectionAsync(initialProjectionConfig.projectionId, false);

        const vehicleId = shortid.generate();
        const stream = await eventstore.getLastEventAsStreamAsync({
            context: 'vehicle',
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

        let pollCounter = 0;
        while (pollCounter < 10) {
            const initialProjection = await eventstore.getProjectionAsync(initialProjectionConfig.projectionId);
            if (initialProjection.processedDate) {
                break;
            } else {
                debug(`projection has not processed yet. trying again in 1000ms`);
                await sleep(1000);
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
                context: 'auction',
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

        await eventstore.projectAsync(projectionWithLatestOffsetConfig);

        const projection = await eventstore.getProjectionAsync(projectionWithLatestOffsetConfig.projectionId);
        expect(projection.offset).toEqual(2);
    });

    it('should process events even if multiple concurrency exists', async function() {
        const initialProjectionConfig = {
            projectionId: 'initial-list-1',
            projectionName: 'Initial Listing',
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

                    const promises = [];
                    const targetQuery = {
                        context: 'test',
                        aggregate: 'concurrent',
                        aggregateId: event.aggregateId
                    };
                    const newEvent = {
                        name: 'test2_concurrent_event_added',
                        payload: data
                    };
                    promises.push(funcs.emit(targetQuery, newEvent));
                    promises.push(funcs.emit(targetQuery, newEvent));

                    await Promise.all(promises);
                }
            },
            query: {
                context: 'vehicle',
                aggregate: 'vehicle'
            },
            partitionBy: '',
            outputState: 'true',
            playbackList: {
                name: 'initial_list_1',
                fields: [{
                    name: 'vehicleId',
                    type: 'string'
                }]
            }
        };

        await eventstore.projectAsync(initialProjectionConfig);
        await eventstore.startAllProjectionsAsync();
        await eventstore.runProjectionAsync(initialProjectionConfig.projectionId, false);

        const vehicleId = shortid.generate();
        const stream = await eventstore.getLastEventAsStreamAsync({
            context: 'vehicle',
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
        }]
        stream.addEvent(initialEvents[0]);
        await stream.commitAsync();

        let pollCounter = 0;
        let projection;
        while (pollCounter < 10) {
            projection = await eventstore.getProjectionAsync(initialProjectionConfig.projectionId);
            if (projection.processedDate) {
                break;
            } else {
                debug(`projection has not processed yet. trying again in 1000ms`);
                await sleep(1000);
            }
        }
        expect(pollCounter).toBeLessThan(10);
        expect(projection.error).toBeNull();
        expect(projection.offset).toEqual(1);
    });

    it('should process events when there are multiple projections with the same query', async function() {
        const firstProjectionConfig = {
            projectionId: 'first-list',
            projectionName: 'First Listing',
            playbackInterface: {
                $init: function() {
                    return {
                        count: 0
                    }
                },
                VEHICLE_CREATED: async function(state, event, funcs) {
                    const playbackList = await funcs.getPlaybackList('first_list');
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
                context: 'vehicle',
                aggregate: 'vehicle'
            },
            partitionBy: '',
            outputState: 'true',
            playbackList: {
                name: 'first_list',
                fields: [{
                    name: 'vehicleId',
                    type: 'string'
                }]
            }
        };
        const secondProjectionConfig = {
            projectionId: 'second-list',
            projectionName: 'Second Listing',
            playbackInterface: {
                $init: function() {
                    return {
                        count: 0
                    }
                },
                VEHICLE_CREATED: async function(state, event, funcs) {
                    const playbackList = await funcs.getPlaybackList('second_list');
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
                context: 'vehicle',
                aggregate: 'vehicle'
            },
            partitionBy: '',
            outputState: 'true',
            playbackList: {
                name: 'second_list',
                fields: [{
                    name: 'vehicleId',
                    type: 'string'
                }]
            }
        };

        await eventstore.projectAsync(firstProjectionConfig);
        await eventstore.startAllProjectionsAsync();
        await eventstore.runProjectionAsync(firstProjectionConfig.projectionId, false);

        const vehicleId = shortid.generate();
        const vehicleId2 = shortid.generate();
        const scivId = shortid.generate();
        const vehicleStream = await eventstore.getLastEventAsStreamAsync({
            context: 'vehicle',
            aggregate: 'vehicle',
            aggregateId: vehicleId
        });
        const vehicleStream2 = await eventstore.getLastEventAsStreamAsync({
            context: 'vehicle',
            aggregate: 'vehicle',
            aggregateId: vehicleId2
        });
        const scivStream = await eventstore.getLastEventAsStreamAsync({
            context: 'auction',
            aggregate: 'salesChannelInstanceVehicle',
            aggregateId: scivId
        });

        Bluebird.promisifyAll(vehicleStream);
        Bluebird.promisifyAll(vehicleStream2);
        Bluebird.promisifyAll(scivStream);

        const initialVehicleEvents = [{
            name: "VEHICLE_CREATED",
            payload: {
                vehicleId: vehicleId,
                year: 2012,
                make: "Honda",
                model: "Jazz",
                mileage: 1245
            }
        }, {
            name: "VEHICLE_CREATED",
            payload: {
                vehicleId: vehicleId,
                year: 2013,
                make: "Honda",
                model: "Jazz",
                mileage: 1246
            }
        }, {
            name: "VEHICLE_CREATED",
            payload: {
                vehicleId: vehicleId2,
                year: 2014,
                make: "Honda",
                model: "Jazz",
                mileage: 1247
            }
        }];
        const initialSCIVEvents = [{
            name: "SCIV_CREATED",
            payload: {
                vehicleId: scivId,
                year: 2012,
                make: "Honda",
                model: "Jazz",
                mileage: 1245
            }
        }, {
            name: "VEHICLE_CREATED",
            payload: {
                vehicleId: scivId,
                year: 2013,
                make: "Honda",
                model: "Jazz",
                mileage: 1246
            }
        }, {
            name: "VEHICLE_CREATED",
            payload: {
                vehicleId: scivId,
                year: 2014,
                make: "Honda",
                model: "Jazz",
                mileage: 1247
            }
        }];
        vehicleStream.addEvent(initialVehicleEvents[0]);
        await vehicleStream.commitAsync();
        vehicleStream.addEvent(initialVehicleEvents[1]);
        await vehicleStream.commitAsync();
        scivStream.addEvent(initialSCIVEvents[0]);
        await scivStream.commitAsync();

        let pollCounter = 0;
        let projection;
        while (pollCounter < 10) {
            projection = await eventstore.getProjectionAsync(firstProjectionConfig.projectionId);
            if (projection.processedDate) {
                break;
            } else {
                debug(`projection has not processed yet. trying again in 1000ms`);
                await sleep(1000);
            }
        }

        expect(pollCounter).toBeLessThan(10);
        expect(projection.error).toBeNull();
        expect(projection.offset).toEqual(2);

        await eventstore.projectAsync(secondProjectionConfig);
        await eventstore.runProjectionAsync(secondProjectionConfig.projectionId, false);

        scivStream.addEvent(initialSCIVEvents[1]);
        await scivStream.commitAsync();
        scivStream.addEvent(initialSCIVEvents[2]);
        await scivStream.commitAsync();
        vehicleStream2.addEvent(initialVehicleEvents[2]);
        await vehicleStream2.commitAsync();

        let secondProjection;
        while (pollCounter < 10) {
            projection = await eventstore.getProjectionAsync(firstProjectionConfig.projectionId);
            secondProjection = await eventstore.getProjectionAsync(secondProjectionConfig.projectionId);
            if (projection.processedDate && secondProjection.processedDate) {
                break;
            } else {
                debug(`both projections have not processed yet. trying again in 1000ms`);
                await sleep(1000);
            }
        }

        expect(pollCounter).toBeLessThan(10);
        expect(projection.error).toBeNull();
        expect(projection.offset).toEqual(6);
        expect(secondProjection.error).toBeNull();
        expect(secondProjection.offset).toEqual(6);

        // TODO: expect playback list as well
    });

    it('should close the eventstore projection', async (done) => {
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
            eventCallbackTimeout: 1000,
            lockTimeToLive: 1000,
            pollingTimeout: eventstoreConfig.pollingTimeout, // optional,
            pollingMaxRevisions: 100,
            errorMaxRetryCount: 2,
            errorRetryExponent: 2,
            playbackEventJobCount: 10,
            context: 'vehicle',
            projectionGroup: 'default'
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