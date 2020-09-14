const Bluebird = require('bluebird');
const EventstorePlaybackListStore = require('../../lib/eventstore-projections/eventstore-playbacklist-mysql-store');
const shortid = require('shortid');

const mysqlOptions = {
    host: 'localhost',
    port: 13306,
    user: 'root',
    password: 'root',
    database: 'playbacklist_db',
    connectionLimit: 1
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
            const command = `docker run --name eventstore_playbacklist_mysql -e MYSQL_ROOT_PASSWORD=${mysqlOptions.password} -e MYSQL_DATABASE=${mysqlOptions.database} -p ${mysqlOptions.port}:3306 -d mysql:5.7`;
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
            exec('docker rm eventstore_playbacklist_mysql --force');
        }
    }
})();

describe('eventstore-playback-list-mysql-store tests', () => {
    let eventstorePlaybackListStore = new EventstorePlaybackListStore();
    let listName;
    beforeAll(async (done) => {
        await mysqlServer.up();
        eventstorePlaybackListStore = new EventstorePlaybackListStore(mysqlOptions);

        Bluebird.promisifyAll(eventstorePlaybackListStore);

        await eventstorePlaybackListStore.init();
        done();
    }, 60000);

    beforeEach(async (done) => {
        let randomString = 'list_' + shortid.generate();
        randomString = randomString.replace('-', '');
        listName = 'list_' + randomString;

        await eventstorePlaybackListStore.createList({
            name: listName,
            fields: [{
                name: 'vehicleId',
                type: 'string'
            }]
        });

        done();
    });

    describe('query', () => {
        it('should return the correct results based on the query parameters passed', async (done) => {
            try {

                // add items to our list
                for (let i = 0; i < 10; i++) {
                    const rowId = shortid.generate();
                    const revision = i;
                    const data = {
                        vehicleId: 'vehicle_' + revision
                    };
                    const meta = {
                        streamRevision: revision
                    }

                    await eventstorePlaybackListStore.addAsync(listName, rowId, revision, data, meta);
                }

                const allResultsInserted = await eventstorePlaybackListStore.queryAsync(listName, 0, 10, null, null);
                expect(allResultsInserted.count).toEqual(10);
                expect(allResultsInserted.rows.length).toEqual(10);

                const pagedResults = await eventstorePlaybackListStore.queryAsync(listName, 5, 5, null, null);
                // should get revision 5 - 9
                expect(pagedResults.count).toEqual(10); // total still 10
                expect(pagedResults.rows.length).toEqual(5); // paged should be 5

                const filteredResults = await eventstorePlaybackListStore.queryAsync(listName, 0, 5, [{
                    field: 'vehicleId',
                    operator: 'is',
                    value: 'vehicle_5'
                }], null);
                expect(filteredResults.count).toEqual(1); // total still 10
                expect(filteredResults.rows.length).toEqual(1); // paged should be 5
                expect(filteredResults.rows[0].revision).toEqual(5);
                expect(filteredResults.rows[0].data.vehicleId).toEqual('vehicle_5');

                const sortedResults = await eventstorePlaybackListStore.queryAsync(listName, 0, 10, null, [{
                    field: 'vehicleId',
                    sortDirection: 'ASC'
                }]);

                expect(sortedResults.count).toEqual(10); // total still 10
                expect(sortedResults.rows.length).toEqual(10); // paged should be 5
                expect(sortedResults.rows[0].revision).toEqual(0);
                expect(sortedResults.rows[0].data.vehicleId).toEqual('vehicle_0');

                done();
            } catch (error) {
                console.log(error);
                throw error;
            }
        })
    })

    describe('add and get', () => {
        it('should add the data in the table', async (done) => {
            const rowId = shortid.generate();
            const revision = 1;
            const data = {
                vehicleId: 'vehicle_1'
            };
            const meta = {
                streamRevision: 1
            }

            await eventstorePlaybackListStore.addAsync(listName, rowId, revision, data, meta);

            const gotItem = await eventstorePlaybackListStore.getAsync(listName, rowId);

            expect(gotItem).toEqual({
                data: data,
                meta: meta,
                revision: revision,
                rowId: rowId
            });
            done();
        })
    })

    describe('update', () => {
        it('should update the data in the table', async (done) => {
            const rowId = shortid.generate();
            const revision = 1;
            const data = {
                vehicleId: 'vehicle_1'
            };
            const meta = {
                streamRevision: 1
            }

            const updatedData = {
                vehicleId: 'vehicle_1',
                mileage: 1000
            };

            const updatedMeta = {
                streamRevision: 2
            };

            const updatedRevision = 2;

            await eventstorePlaybackListStore.addAsync(listName, rowId, revision, data, meta);
            await eventstorePlaybackListStore.updateAsync(listName, rowId, updatedRevision, updatedData, updatedMeta);

            const gotItem = await eventstorePlaybackListStore.getAsync(listName, rowId);

            expect(gotItem).toEqual({
                data: updatedData,
                meta: updatedMeta,
                revision: updatedRevision,
                rowId: rowId
            });
            done();
        })
    })

    describe('delete', () => {
        it('should delete the data in the table', async (done) => {
            const rowId = shortid.generate();
            const revision = 1;
            const data = {
                vehicleId: 'vehicle_1'
            };
            const meta = {
                streamRevision: 1
            }

            await eventstorePlaybackListStore.addAsync(listName, rowId, revision, data, meta);
            await eventstorePlaybackListStore.deleteAsync(listName, rowId);

            const gotItem = await eventstorePlaybackListStore.getAsync(listName, rowId);

            expect(gotItem).toBeNull();
            done();
        })
    })

    afterAll(async (done) => {
        // NOTE: uncomment if we need to terminate the mysql every test
        // for now, it is okay since we are using a non-standard port (13306) and a fixed docker container name
        // not terminating will make the tests faster by around 11 secs
        // await mysqlServer.down();
        done();
    })
});