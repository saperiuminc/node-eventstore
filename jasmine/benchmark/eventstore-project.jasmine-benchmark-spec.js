const Bluebird = require('bluebird');
const eventstore = require('../../index'); // toggle v2 and v1 mysql
const shortid = require('shortid');
const _ = require('lodash');

const sleepAsync = function(sleepUntil) {
    return new Promise((resolve) => {
        setTimeout(resolve, sleepUntil);
    });
}

const numberOfVehicles = 190;

const mysqlOptions = {
    host: 'localhost',
    port: 53306,
    user: 'root',
    password: 'root',
    database: 'eventstore',
    connectionLimit: '2'
};

const redisOptions = {
    host: 'localhost',
    port: 56379
};

const eventstoreOptions = {
    type: 'mysql',
    host: mysqlOptions.host,
    port: mysqlOptions.port,
    user: mysqlOptions.user,
    password: mysqlOptions.password,
    database: mysqlOptions.database,
    connectionPoolLimit: 16,
    // projections-specific configuration below
    redisConfig: {
        host: redisOptions.host,
        port: redisOptions.port
    }, // required
    listStore: {
        host: mysqlOptions.host,
        port: mysqlOptions.port,
        user: mysqlOptions.user,
        password: mysqlOptions.password,
        database: mysqlOptions.database,
        connectionPoolLimit: 16,
    }, // required
    enableProjection: true,
    eventCallbackTimeout: 1000,
    pollingTimeout: 100, // optional,
    eventCallbackTimeout: 10000,
    pollingMaxRevisions: 10000,
    errorMaxRetryCount: 2,
    errorRetryExponent: 2,
    projectionGroup: 'vehicle'
}

const mysqlServer = (function() {
    const sleepAsync = function(sleepUntil) {
        return new Promise((resolve) => {
            setTimeout(resolve, sleepUntil);
        });
    }

    const exec = require('child_process').exec;
    const mysql = require('mysql');

    return {
        up: async function() {
            const command = `docker run --name eventstore_project_benchmark_mysql -v "$(pwd)/jasmine/benchmark/my.cnf":/etc/mysql/my.cnf -e MYSQL_ROOT_PASSWORD=${mysqlOptions.password} -e MYSQL_DATABASE=${mysqlOptions.database} -p ${mysqlOptions.port}:3306 -d mysql:5.7`;
            // const command = `echo $PWD`;
            const process = exec(command);

            // wait until process has exited
            console.log('downloading mysql image or creating a container. waiting for child process to return an exit code');
            do {
                await sleepAsync(1000);
            } while (process.exitCode == null);

            console.log('child process exited with exit code: ', process.exitCode);

            console.info('waiting for mysql database to start...');
            let retries = 0;
            let gaveUp = true;
            let conn = null;
            do {
                try {
                    conn = mysql.createConnection(mysqlOptions);

                    Bluebird.promisifyAll(conn);

                    await conn.connectAsync();
                    console.log('connected!');
                    gaveUp = false;
                    break;
                } catch (error) {
                    conn.end();
                    console.log(`mysql retry attempt ${retries + 1} after 1000ms`);
                    await sleepAsync(1000);
                    retries++;
                }
            } while (retries < 20);

            if (gaveUp) {
                console.error('given up connecting to mysql database');
                console.error('abandoning tests');
            } else {
                console.log('successfully connected to mysql database');
            }
        },
        down: async function() {
            exec('docker rm eventstore_project_benchmark_mysql --force');
        }
    }
})();

const redisServer = (function() {
    const sleepAsync = function(sleepUntil) {
        return new Promise((resolve) => {
            setTimeout(resolve, sleepUntil);
        });
    }

    const exec = require('child_process').exec;

    return {
        up: async function() {
            const command = `docker run --name eventstore_project_benchmark_redis -p ${redisOptions.port}:6379 -d redis:5.0`;
            const process = exec(command);

            // wait until process has exited
            console.log('downloading redis image or creating a container...');
            do {
                console.log('waiting for child process to return an exit code...');
                await sleepAsync(1000);
            } while (process.exitCode == null);

            console.log('child process exited with exit code: ', process.exitCode);

            console.info('waiting for redis to start...');
        },
        down: async function() {
            exec('docker rm eventstore_project_benchmark_redis --force');
        }
    }
})();

describe('eventstore-project-benchmark tests', () => {
    let listName;
    beforeAll(async (done) => {
        await mysqlServer.up();
        await redisServer.up();
        done();
    }, 60000);

    beforeEach(async (done) => {
        done();
    });

    it('should pass when concurrentAggregatesInProjection is 1', async (done) => {
        const options = _.clone(eventstoreOptions);
        options.concurrentAggregatesInProjection = 1;
        const esWithProjection = eventstore(eventstoreOptions)
        Bluebird.promisifyAll(esWithProjection);
        await esWithProjection.initAsync();

        await esWithProjection.projectAsync({
            projectionId: 'vehicle-projection',
            playbackInterface: {
                $init: function() {
                    return {
                        count: 0
                    }
                },
                vehicle_created: function(state, event, funcs, done) {
                    const targetQuery = {
                        context: 'vehicle',
                        aggregate: 'dashboardVehicle',
                        aggregateId: event.aggregateId
                    };
        
                    funcs.emit(targetQuery, event.payload, done);
                }
            },
            query: {
                context: 'vehicle',
                aggregate: 'vehicle'
            },
            partitionBy: '',
            outputState: 'false'
        });

        await esWithProjection.projectAsync({
            projectionId: 'vehicle-list-projection',
            playbackInterface: {
                $init: function() {
                    return {
                        count: 0
                    }
                },
                vehicle_created: function(state, event, funcs, done) {
                    funcs.getPlaybackList('vehicle_list', function(err, playbackList) {
                        const eventPayload = event.payload.payload;
        
                        const data = {
                            vehicleId: eventPayload.vehicleId,
                            year: eventPayload.year,
                            make: eventPayload.make,
                            model: eventPayload.model,
                            mileage: eventPayload.mileage
                        };
        
                        playbackList.add(event.aggregateId, event.streamRevision, data, {}, function(err) {
                            done();
                        });
                    });
                }
            },
            query: {
                context: 'vehicle',
                aggregate: 'dashboardVehicle'
            },
            partitionBy: '',
            outputState: 'false',
            playbackList: {
                name: 'vehicle_list',
                fields: [{
                    name: 'vehicleId',
                    type: 'string'
                }],
                secondaryKeys: {
                    idx_vehicleId: [{
                        name: 'vehicleId',
                        sort: 'ASC'
                    }]
                }
            },
        });
    
        await esWithProjection.startAllProjectionsAsync();

        const tasks = [];
        for (let index = 0; index < numberOfVehicles; index++) {
            const vehicleId = `vehicle_${index}`;

            const addVehicleEvent = async function(vehicleId) {
                const stream = await esWithProjection.getLastEventAsStreamAsync({
                    aggregateId: vehicleId,
                    aggregate: 'vehicle', // optional
                    context: 'vehicle' // optional
                });
    
                Bluebird.promisifyAll(stream);
    
                stream.addEvent({
                    name: 'vehicle_created',
                    payload: {
                        vehicleId: vehicleId,
                        year: 2012,
                        make: 'Honda',
                        model: 'Jazz',
                        mileage: 12345
                    }
                });
    
                await stream.commitAsync();
            }

            tasks.push(addVehicleEvent(vehicleId));
        }

        await Promise.all(tasks);
        
        await sleepAsync(1000);

        const vehicleList = await esWithProjection.getPlaybackListAsync('vehicle_list');
        vehicleList.query(0, 10, null, null, function(err ,result) {
            expect(result.count).toEqual(numberOfVehicles);
            done();
        });
        
    }, 30000)

    it('should pass', (done) => {
        done();
    })

    afterEach(async (done) => {
        // await mysqlStore.disconnectAsync();
        done();
    })

    afterAll(async (done) => {
        // NOTE: uncomment if we need to terminate the mysql every test
        // for now, it is okay since we are using a non-standard port (13306) and a fixed docker container name
        // not terminating will make the tests faster by around 11 secs
        await mysqlServer.down();
        await redisServer.down();

        done();
    })
});