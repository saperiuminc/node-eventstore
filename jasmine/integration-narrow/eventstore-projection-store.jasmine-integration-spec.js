const Bluebird = require('bluebird');
const EventstoreProjectionStore = require('../../lib/eventstore-projections/projection/eventstore-projection-mysql-store'); // toggle v2 and v1 mysql
const shortid = require('shortid');
const _ = require('lodash');

const mysqlOptions = {
    connection: {
        host: 'localhost',
        port: 43306,
        user: 'root',
        password: 'root',
        database: 'projections',
    },
    pool: {
        min: 2,
        max: 2
    }
};

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
            const command = `docker run --name eventstore_projection_mysql -e MYSQL_ROOT_PASSWORD=${mysqlOptions.connection.password} -e MYSQL_DATABASE=${mysqlOptions.connection.database} -p ${mysqlOptions.connection.port}:3306 -d mysql:5.7`;
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
                    conn = mysql.createConnection(mysqlOptions.connection);

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
            exec('docker rm eventstore_projection_mysql --force');
        }
    }
})();

describe('eventstore-projection-store tests', () => {
    describe('validations', () => {
        it('should validate host', (done) => {
            try {
                const store  = new EventstoreProjectionStore({});
            } catch (error) {
                expect(error.message).toEqual('host is required');
                done();
            }
        });
       
        it('should validate port', (done) => {
            try {
                const store  = new EventstoreProjectionStore({
                    connection: {
                        host: 'host'
                    }
                });
            } catch (error) {
                expect(error.message).toEqual('port is required');
                done();
            }
        });
        it('should validate user', (done) => {
            try {
                const store  = new EventstoreProjectionStore({
                    connection: {
                        host: 'host',
                        port: 1212
                    }
                });
            } catch (error) {
                expect(error.message).toEqual('user is required');
                done();
            }
        });
        it('should validate password', (done) => {
            try {
                const store  = new EventstoreProjectionStore({
                    connection: {
                        host: 'host',
                        port: 1212,
                        user: 'user'
                    }
                });
            } catch (error) {
                expect(error.message).toEqual('password is required');
                done();
            }
        });
        it('should validate database', (done) => {
            try {
                const store  = new EventstoreProjectionStore({
                    connection: {
                        host: 'host',
                        port: 1212,
                        user: 'user',
                        password: 'password'
                    }
                });
            } catch (error) {
                expect(error.message).toEqual('database is required');
                done();
            }
        });
    })

    describe('eventstore-projection methods', () => {
        let projectionStore;
        let options;

        beforeAll(async (done) => {
            await mysqlServer.up();
            done();
        }, 60000);
    
        beforeEach(async (done) => {
            options = {
                connection: mysqlOptions.connection,
                pool:  mysqlOptions.pool,
                name: 'projections_2'
            };
    
            projectionStore = new EventstoreProjectionStore(options);
            await projectionStore.init();
            done();
        });

        describe('createIfNotExists', () => {
            it('should add the projection in the store if it does not exist', async (done) => {
                const projection = {
                    projectionId: 'projection_id',
                    query: {
                        context: 'vehicle',
                        aggregate: 'vehicle'
                    },
                    playbackInterface: {
                        vehicle_created: function() {
                            return 'a';
                        }
                    }
                }
                await projectionStore.createProjectionIfNotExists(projection);
    
                const storeProjection = await projectionStore.getProjection(projection.projectionId);
    
                expect(storeProjection.projectionId).toEqual(projection.projectionId);
                expect(storeProjection.query).toEqual(projection.query);
                expect(storeProjection.processedDate).toBeFalsy();
                expect(storeProjection.isProcessing).toBeFalsy();
                expect(storeProjection.offset).toBeFalsy();
    
                done();
            });
        })
    
        describe('setProcessed', () => {
            it('should set the processed date', async (done) => {
                const projection = {
                    projectionId: 'projection_id',
                    query: {
                        context: 'vehicle',
                        aggregate: 'vehicle'
                    }
                }
    
                const processedDate = Date.now();
                const isProcessing = 1;
                const offset = undefined;
    
                await projectionStore.createProjectionIfNotExists(projection);
                await projectionStore.setProcessed(projection.projectionId, processedDate, offset, isProcessing);
    
                const storeProjection = await projectionStore.getProjection(projection.projectionId);
    
                expect(storeProjection.projectionId).toEqual(projection.projectionId);
                expect(storeProjection.processedDate).toEqual(processedDate);
                expect(storeProjection.isProcessing).toEqual(isProcessing);
                expect(storeProjection.offset).toBeNull();
    
                done();
            });
    
            it('should set the processed date and the optional offset', async (done) => {
                const projection = {
                    projectionId: 'projection_id',
                    query: {
                        context: 'vehicle',
                        aggregate: 'vehicle'
                    }
                }
    
                const processedDate = Date.now();
                const isProcessing = 1;
                const offset = 10;
    
                await projectionStore.createProjectionIfNotExists(projection);
                await projectionStore.setProcessed(projection.projectionId, processedDate, offset, isProcessing);
    
                const storeProjection = await projectionStore.getProjection(projection.projectionId);
    
                expect(storeProjection.projectionId).toEqual(projection.projectionId);
                expect(storeProjection.processedDate).toEqual(processedDate);
                expect(storeProjection.offset).toEqual(offset);
    
                done();
            });
        });
    
        afterEach(async (done) => {
            await projectionStore.clearAll();
            done();
        })
    });
   

    afterAll(async (done) => {
        // NOTE: uncomment if we need to terminate the mysql every test
        // for now, it is okay since we are using a non-standard port (13306) and a fixed docker container name
        // not terminating will make the tests faster by around 11 secs
        // await mysqlServer.down();
        done();
    })
});
