const Bluebird = require('bluebird');
const EventstorePlaybackListStore = require('../../lib/eventstore-projections/playbacklist/eventstore-playbacklist-mysql-store');
const EventstorePlaybackListView = require('../../lib/eventstore-projections/eventstore-playback-list-view');
const shortid = require('shortid');

const mysqlOptions = {
    connection: {
        host: 'localhost',
        port: 33306,
        user: 'root',
        password: 'root',
        database: 'playbacklist_db',
    },
    pool: {
        min: 10,
        max: 10
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
            const command = `docker run --name eventstore_playbacklist_view_mysql -e MYSQL_ROOT_PASSWORD=${mysqlOptions.connection.password} -e MYSQL_DATABASE=${mysqlOptions.connection.database} -p ${mysqlOptions.connection.port}:3306 -d mysql:5.7`;
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
            exec('docker rm eventstore_playbacklist_view_mysql --force');
        }
    }
})();

describe('eventstore-playback-list-view-mysql-store tests', () => {
    let eventstorePlaybackListStore = new EventstorePlaybackListStore();
    let eventstorePlaybackListView = new EventstorePlaybackListView();
    let eventstorePlaybackListViewOptimized = new EventstorePlaybackListView();
    let eventstorePlaybackListViewUnionOptimized = new EventstorePlaybackListView();
    let eventstorePlaybackListViewDefaultWhereOptimized = new EventstorePlaybackListView();

    let listName;
    let listName2;
    beforeAll(async (done) => {
        await mysqlServer.up();

        eventstorePlaybackListStore = new EventstorePlaybackListStore(mysqlOptions);
        await eventstorePlaybackListStore.init();

        let randomString = 'list_' + shortid.generate();
        randomString = randomString.replace('-', '');
        listName = 'list_' + randomString;

        await eventstorePlaybackListStore.createList({
            name: listName,
            fields: [{
                name: 'vehicleId',
                type: 'string'
            },
            {
                name: 'accessDate',
                type: 'date'
            },
            {
                name: 'type',
                type: 'int'
            }]
        });

        let randomString2 = 'list_' + shortid.generate();
        randomString2 = randomString2.replace('-', '');
        listName2 = 'list_' + randomString2;

        await eventstorePlaybackListStore.createList({
            name: listName2,
            fields: [{
                name: 'vehicleId',
                type: 'string'
            },
            {
                name: 'accessDate',
                type: 'date'
            },
            {
                name: 'type',
                type: 'int'
            }]
        });

        // add items to our list
        for (let i = 0; i < 10; i++) {
            const rowId = shortid.generate();
            const revision = i;
            const data = {
                vehicleId: 'vehicle_' + revision,
                accessDate: `2020-11-${(revision+1) >= 10 ? (revision+1) : '0' + (revision+1)}`,
                type: i % 2
            };
            const meta = {
                streamRevision: revision
            }

            await eventstorePlaybackListStore.add(listName, rowId, revision, data, meta);
        }

        // add items to our second list
        for (let i = 0; i < 10; i++) {
            const rowId = shortid.generate();
            const revision = i;
            const data = {
                vehicleId: 'vehicle_' + (revision + 10),
                accessDate: `2020-11-${(revision+1) >= 10 ? (revision+1) : '0' + (revision+1)}`,
                type: i % 2
            };
            const meta = {
                streamRevision: revision
            }

            await eventstorePlaybackListStore.add(listName2, rowId, revision, data, meta);
        }

        eventstorePlaybackListView = new EventstorePlaybackListView({
            connection: mysqlOptions.connection,
            pool: mysqlOptions.pool,
            listName: "list_view_1",
            listQuery: `SELECT * FROM ${listName}`,
            totalCountQuery: null,
            alias: undefined
        });
        Bluebird.promisifyAll(eventstorePlaybackListView);
        await eventstorePlaybackListView.init();

        eventstorePlaybackListViewOptimized = new EventstorePlaybackListView({
            connection: mysqlOptions.connection,
            pool: mysqlOptions.pool,
            listName: "list_view_2",
            listQuery: `SELECT * FROM ${listName} AS vehicle_list @@where @@order @@limit;`,
            totalCountQuery: `SELECT COUNT(1) AS total_count FROM ${listName} AS vehicle_list @@where;`,
            alias: {
                vehicleId: `vehicle_list.vehicleId`,
                accessDate: `vehicle_list.accessDate`
            }
        });
        Bluebird.promisifyAll(eventstorePlaybackListViewOptimized);
        await eventstorePlaybackListViewOptimized.init();

        eventstorePlaybackListViewUnionOptimized = new EventstorePlaybackListView({
            connection: mysqlOptions.connection,
            pool: mysqlOptions.pool,
            listName: "list_view_3",
            listQuery: `SELECT * FROM (( SELECT * FROM ${listName} AS vehicle_list @@where @@order @@unionlimit ) UNION ALL ` + 
                `( SELECT * FROM ${listName2} AS vehicle_list @@where @@order @@unionlimit )) vehicle_list @@order @@limit; `,
            totalCountQuery: `SELECT SUM(t_count) as total_count FROM (( SELECT COUNT(1) AS t_count FROM ${listName} AS vehicle_list @@where ) UNION ALL ` + 
            `( SELECT COUNT(1) AS t_count FROM ${listName2} AS vehicle_list @@where )) t1;`,
            alias: {
                vehicleId: `vehicle_list.vehicleId`,
                accessDate: `vehicle_list.accessDate`
            }
        });
        Bluebird.promisifyAll(eventstorePlaybackListViewUnionOptimized);
        await eventstorePlaybackListViewUnionOptimized.init();

        eventstorePlaybackListViewDefaultWhereOptimized = new EventstorePlaybackListView({
            connection: mysqlOptions.connection,
            pool: mysqlOptions.pool,
            listName: "list_view_2",
            listQuery: `SELECT * FROM ${listName} AS vehicle_list @@where and vehicle_list.type = 1 @@order @@limit;`,
            totalCountQuery: `SELECT COUNT(1) AS total_count FROM ${listName} AS vehicle_list @@where and type = 1;`,
            alias: {
                vehicleId: `vehicle_list.vehicleId`,
                accessDate: `vehicle_list.accessDate`,
                type: `vehicle_list.type`
            }
        });
        Bluebird.promisifyAll(eventstorePlaybackListViewDefaultWhereOptimized);
        await eventstorePlaybackListViewDefaultWhereOptimized.init();

        done();
    }, 60000);

    describe('query', () => {
        it('should return the correct results based on the query parameters passed using LISTVIEW', async (done) => {
            try {
                const allResultsInserted = await eventstorePlaybackListView.queryAsync(0, 10, null, null);
                expect(allResultsInserted.count).toEqual(10);
                expect(allResultsInserted.rows.length).toEqual(10);

                const pagedResults = await eventstorePlaybackListView.queryAsync(5, 5, null, null);
                // should get revision 5 - 9
                expect(pagedResults.count).toEqual(10); // total still 10
                expect(pagedResults.rows.length).toEqual(5); // paged should be 5

                const filteredResults = await eventstorePlaybackListView.queryAsync(0, 5, [{
                    field: 'vehicleId',
                    operator: 'is',
                    value: 'vehicle_5'
                },{
                    field: 'accessDate',
                    operator: 'dateRange',
                    from: '2020-11-01',
                    to: '2020-11-10'
                }], null);
                expect(filteredResults.count).toEqual(1); // total = 1 after filter
                expect(filteredResults.rows.length).toEqual(1);
                expect(filteredResults.rows[0].revision).toEqual(5);
                expect(filteredResults.rows[0].data.vehicleId).toEqual('vehicle_5');

                const sortedResults = await eventstorePlaybackListView.queryAsync(0, 10, null, [{
                    field: 'vehicleId',
                    sortDirection: 'ASC'
                }]);

                expect(sortedResults.count).toEqual(10); // total still 10
                expect(sortedResults.rows.length).toEqual(10);
                expect(sortedResults.rows[0].revision).toEqual(0);
                expect(sortedResults.rows[0].data.vehicleId).toEqual('vehicle_0');

                done();
            } catch (error) {
                console.log(error);
                throw error;
            }
        });

        it('should return the correct results based on the query parameters passed using LISTVIEW and using nested filter groups', async (done) => {
            try {
                const allResultsInserted = await eventstorePlaybackListView.queryAsync(0, 10, null, null);
                expect(allResultsInserted.count).toEqual(10);
                expect(allResultsInserted.rows.length).toEqual(10);

                const pagedResults = await eventstorePlaybackListView.queryAsync(5, 5, null, null);
                // should get revision 5 - 9
                expect(pagedResults.count).toEqual(10); // total still 10
                expect(pagedResults.rows.length).toEqual(5); // paged should be 5

                const filteredResults = await eventstorePlaybackListView.queryAsync(0, 5, [
                    {
                        group: 'group',
                        groupBooleanOperator: 'or',
                        filters: [
                            {
                                field: 'vehicleId',
                                operator: 'is',
                                value: 'vehicle_5',
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
                        group: 'group',
                        groupBooleanOperator: 'or',
                        filters: [
                            {
                                field: 'vehicleId',
                                operator: 'notExists'
                            }
                        ]
                    }
                ], null);
                expect(filteredResults.count).toEqual(1); // total = 1 after filter
                expect(filteredResults.rows.length).toEqual(1);
                expect(filteredResults.rows[0].revision).toEqual(5);
                expect(filteredResults.rows[0].data.vehicleId).toEqual('vehicle_5');

                const sortedResults = await eventstorePlaybackListView.queryAsync(0, 10, null, [{
                    field: 'vehicleId',
                    sortDirection: 'ASC'
                }]);

                expect(sortedResults.count).toEqual(10); // total still 10
                expect(sortedResults.rows.length).toEqual(10);
                expect(sortedResults.rows[0].revision).toEqual(0);
                expect(sortedResults.rows[0].data.vehicleId).toEqual('vehicle_0');

                done();
            } catch (error) {
                console.log(error);
                throw error;
            }
        });

        it('should return the correct results based on the query parameters passed using optimized query', async (done) => {
            try {
                const allResultsInserted = await eventstorePlaybackListViewOptimized.queryAsync(0, 10, null, null);
                expect(allResultsInserted.count).toEqual(10);
                expect(allResultsInserted.rows.length).toEqual(10);

                const pagedResults = await eventstorePlaybackListViewOptimized.queryAsync(5, 5, null, null);
                // should get revision 5 - 9
                expect(pagedResults.count).toEqual(10); // total still 10
                expect(pagedResults.rows.length).toEqual(5); // paged should be 5

                const filteredResults = await eventstorePlaybackListViewOptimized.queryAsync(0, 5, [{
                    field: 'vehicleId',
                    operator: 'is',
                    value: 'vehicle_5'
                }], null);
                expect(filteredResults.count).toEqual(1); // total = 1 after filter
                expect(filteredResults.rows.length).toEqual(1);
                expect(filteredResults.rows[0].revision).toEqual(5);
                expect(filteredResults.rows[0].data.vehicleId).toEqual('vehicle_5');

                const sortedResults = await eventstorePlaybackListViewOptimized.queryAsync(0, 10, null, [{
                    field: 'vehicleId',
                    sortDirection: 'ASC'
                }]);

                expect(sortedResults.count).toEqual(10); // total still 10
                expect(sortedResults.rows.length).toEqual(10);
                expect(sortedResults.rows[0].revision).toEqual(0);
                expect(sortedResults.rows[0].data.vehicleId).toEqual('vehicle_0');

                done();
            } catch (error) {
                console.log(error);
                throw error;
            }
        });

        it('should return the correct results based on the query parameters passed using union optimized query', async (done) => {
            try {
                const allResultsInserted = await eventstorePlaybackListViewUnionOptimized.queryAsync(0, 10, null, null);
                console.log(JSON.stringify(allResultsInserted));
                expect(allResultsInserted.count).toEqual(20);
                expect(allResultsInserted.rows.length).toEqual(10);

                const pagedResults = await eventstorePlaybackListViewUnionOptimized.queryAsync(5, 5, null, null);
                // should get revision 5 - 9
                expect(pagedResults.count).toEqual(20); // total still 20
                expect(pagedResults.rows.length).toEqual(5); // paged should be 5

                const filteredResults = await eventstorePlaybackListViewUnionOptimized.queryAsync(0, 5, [{
                    field: 'vehicleId',
                    operator: 'is',
                    value: 'vehicle_5'
                }], null);
                expect(filteredResults.count).toEqual(1);
                expect(filteredResults.rows.length).toEqual(1);
                expect(filteredResults.rows[0].revision).toEqual(5);
                expect(filteredResults.rows[0].data.vehicleId).toEqual('vehicle_5');
                console.log(JSON.stringify(filteredResults));

                const sortedResults = await eventstorePlaybackListViewUnionOptimized.queryAsync(0, 10, null, [{
                    field: 'accessDate',
                    sortDirection: 'ASC'
                }]);

                expect(sortedResults.count).toEqual(20);
                expect(sortedResults.rows.length).toEqual(10);
                expect(sortedResults.rows[0].revision).toEqual(0);
                expect(sortedResults.rows[0].data.vehicleId).toEqual('vehicle_0');
                expect(sortedResults.rows[1].revision).toEqual(0);
                expect(sortedResults.rows[1].data.vehicleId).toEqual('vehicle_10');
                expect(sortedResults.rows[2].revision).toEqual(1);
                expect(sortedResults.rows[2].data.vehicleId).toEqual('vehicle_1');
                expect(sortedResults.rows[3].revision).toEqual(1);
                expect(sortedResults.rows[3].data.vehicleId).toEqual('vehicle_11');

                done();
            } catch (error) {
                console.log(error);
                throw error;
            }
        });

        it('should return the correct results based on the query parameters passed using optimized query with default where clause', async (done) => {
            try {
                const allResultsInserted = await eventstorePlaybackListViewDefaultWhereOptimized.queryAsync(0, 10, null, null);
                expect(allResultsInserted.count).toEqual(5);
                expect(allResultsInserted.rows.length).toEqual(5);

                const pagedResults = await eventstorePlaybackListViewDefaultWhereOptimized.queryAsync(3, 5, null, null);
                // should get revision 5 - 9
                expect(pagedResults.count).toEqual(5); // total still 5
                expect(pagedResults.rows.length).toEqual(2); // paged should be 2

                const filteredResults = await eventstorePlaybackListViewDefaultWhereOptimized.queryAsync(0, 5, [{
                    field: 'vehicleId',
                    operator: 'is',
                    value: 'vehicle_5'
                }], null);
                expect(filteredResults.count).toEqual(1); // total = 1 after filter
                expect(filteredResults.rows.length).toEqual(1);
                expect(filteredResults.rows[0].revision).toEqual(5);
                expect(filteredResults.rows[0].data.vehicleId).toEqual('vehicle_5');

                const sortedResults = await eventstorePlaybackListViewDefaultWhereOptimized.queryAsync(0, 10, null, [{
                    field: 'vehicleId',
                    sortDirection: 'ASC'
                }]);

                expect(sortedResults.count).toEqual(5); // total still 5
                expect(sortedResults.rows.length).toEqual(5);
                expect(sortedResults.rows[0].revision).toEqual(1);
                expect(sortedResults.rows[0].data.vehicleId).toEqual('vehicle_1');

                done();
            } catch (error) {
                console.log(error);
                throw error;
            }
        });

    });

    afterAll(async (done) => {
        // NOTE: uncomment if we need to terminate the mysql every test
        // for now, it is okay since we are using a non-standard port (13306) and a fixed docker container name
        // not terminating will make the tests faster by around 11 secs
        await eventstorePlaybackListStore.close();
        // await mysqlServer.down();
        done();
    })
});
