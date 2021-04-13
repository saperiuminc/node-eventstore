const Bluebird = require('bluebird');
const EventstoreStateListStore = require('../../lib/eventstore-projections/eventstore-statelist-mysql-store');
const shortid = require('shortid');

const mysqlOptions = {
    connection: {
        host: 'localhost',
        port: 53306,
        user: 'root',
        password: 'root',
        database: 'statelist_db',
    },
    pool: {
        min: 10,
        max: 10,
    },
};

const mysqlServer = (function() {
    const sleepAsync = function(sleepUntil) {
        return new Promise((resolve) => {
            setTimeout(resolve, sleepUntil);
        });
    }

    const exec = require('child_process').exec;
    const mysql = require('mysql');
    const dockerContainerName = 'eventstore_statelist_mysql';

    return {
        up: async function() {
            const command = `docker run --name ${dockerContainerName} -e MYSQL_ROOT_PASSWORD=${mysqlOptions.connection.password} -e MYSQL_DATABASE=${mysqlOptions.connection.database} -p ${mysqlOptions.connection.port}:3306 -d mysql:5.7`;
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
            exec(`docker rm ${dockerContainerName} --force`);
        }
    }
})();

describe('eventstore-playback-list-mysql-store tests', () => {
    let eventstoreStateListStore = new EventstoreStateListStore();
    let listName;
    beforeAll(async () => {
        await mysqlServer.up();
        eventstoreStateListStore = new EventstoreStateListStore(mysqlOptions);

        await eventstoreStateListStore.init();
    }, 60000);

    beforeEach(async () => {
        let randomString = 'list_' + shortid.generate();
        randomString = randomString.replace('-', '');
        listName = 'list_' + randomString;

        await eventstoreStateListStore.createList({
            name: listName,
            fields: [{
                name: 'vehicleId',
                type: 'string'
            },{
                name: 'accessDate',
                type: 'int'
            }]
        });
    });

    
    describe('push', () => {
        it('should add the data in the list', async () => {
            const rowId = shortid.generate();
            const revision = 1;
            const data = {
                vehicleId: 'vehicle_1'
            };
            const meta = {
                streamRevision: 1
            }

            const lastId = await eventstoreStateListStore.push(listName, data, meta);

            const gotItem = await eventstoreStateListStore.find(listName, lastId, [{
                field: 'vehicleId',
                operator: 'is',
                value: 'vehicle_1'
            }]);

            expect(gotItem).toEqual({
                index: 0,
                value: data,
                meta: undefined
            });
        })
    })

    describe('set', () => {
        it('should update the data in the list', async () => {
            const rowId = shortid.generate();
            const revision = 1;
            const data = {
                vehicleId: 'vehicle_1'
            };
            const meta = {
                streamRevision: 1
            }

            let lastId = await eventstoreStateListStore.push(listName, data, meta);
    
            // update
            data.inspector = 'chino';
            lastId = await eventstoreStateListStore.set(listName, 0, data, meta);

            const gotItem = await eventstoreStateListStore.find(listName, lastId, [{
                field: 'vehicleId',
                operator: 'is',
                value: 'vehicle_1'
            }]);


            expect(gotItem).toEqual({
                index: 0,
                value: data,
                meta: undefined
            });
        })
    })

    describe('delete', () => {
        it('should delete the data in the list', async () => {
            const rowId = shortid.generate();
            const revision = 1;
            const data = {
                vehicleId: 'vehicle_1'
            };
            const meta = {
                streamRevision: 1
            }

            let lastId = await eventstoreStateListStore.push(listName, data, meta);
            lastId = await eventstoreStateListStore.delete(listName, 0);

            const gotItem = await eventstoreStateListStore.find(listName, lastId, [{
                field: 'vehicleId',
                operator: 'is',
                value: 'vehicle_1'
            }]);

            expect(gotItem).toEqual(null);
        })
    })

    describe('filter', () => {
        it('should filter the data in the list', async () => {
            const rowId = shortid.generate();
            const revision = 1;
            const data1 = {
                vehicleId: '100'
            };
            const data2 = {
                vehicleId: 'vehicle_2'
            };

            let lastId = await eventstoreStateListStore.push(listName, data1);
            lastId = await eventstoreStateListStore.push(listName, data2);

            const gotItem = await eventstoreStateListStore.filter(listName, lastId, [{
                field: 'vehicleId',
                operator: 'is',
                value: '100'
            }]);

            expect(gotItem).toEqual([{
                index: 0,
                value: data1,
                meta: undefined
            }]);
        })

        it('should still be able to filter if the column is an integer and filter value is a string', async () => {
            const rowId = shortid.generate();
            const revision = 1;
            const data1 = {
                vehicleId: '100',
                accessDate: 1000
            };
            const data2 = {
                vehicleId: 'vehicle_2',
                accessDate: 1000
            };
            const data3 = {
                vehicleId: 'vehicle_2',
                accessDate: 2000
            };

            let lastId = await eventstoreStateListStore.push(listName, data1);
            lastId = await eventstoreStateListStore.push(listName, data2);
            lastId = await eventstoreStateListStore.push(listName, data3);

            const gotItem = await eventstoreStateListStore.filter(listName, lastId, [{
                field: 'accessDate',
                operator: 'is',
                value: '1000'
            }]);

            expect(gotItem).toEqual([{
                index: 0,
                value: data1,
                meta: undefined
            }, {
                index: 1,
                value: data2,
                meta: undefined
            }]);
        })
    })


    afterAll(async () => {
        // NOTE: uncomment if we need to terminate the mysql every test
        // for now, it is okay since we are using a non-standard port (13306) and a fixed docker container name
        // not terminating will make the tests faster by around 11 secs
        // await mysqlServer.down();
    })
});