'use strict';
const Docker = require('dockerode');
const path = require('path');
const mysql = require('mysql');
const childProcess = require('child_process');
const exec = childProcess.exec;
const shortid = require('shortid');
const Bluebird = require('bluebird');
const fs = require('fs/promises');
const debug = require('debug')('BENCH');

// NOTE: use mysql-based eventstore w/expanded bookmarked getEvents and archiving function
var EventstorePlaybackListMySqlStore = require('../../lib/eventstore-projections/eventstore-playbacklist-mysql-store');

const extension = path.extname(module.filename);
const containerName = path.basename(module.filename, extension);

const mysqlOptions = {
    host: 'localhost',
    port: 43306,
    user: 'root',
    password: 'root',
    database: 'eventstore',
    connectionLimit: '1'
};


console.log('==========================');
console.log('ioredis: ' + require('../../package.json').version);
console.log('node_redis: ' + require('redis/package.json').version);
var os = require('os');
const { count } = require('console');
console.log('CPU: ' + os.cpus().length);
console.log('OS: ' + os.platform() + ' ' + os.arch());
console.log('node version: ' + process.version);
console.log('mysql connectionLimit: ' + mysqlOptions.connectionLimit);
console.log('current commit: ' + childProcess.execSync('git rev-parse --short HEAD'));
console.log('==========================');

const sleepAsync = function(sleepUntil) {
    return new Promise((resolve) => {
        setTimeout(resolve, sleepUntil);
    });
}

set('setup', async function() {
    try {
        debug('creating the container...');
        const command = `docker run --name ${containerName} -v ${path.join(__dirname, 'mysql.conf.d')}:/etc/mysql/mysql.conf.d -e MYSQL_ROOT_PASSWORD=${mysqlOptions.password} -e MYSQL_DATABASE=${mysqlOptions.database} -p ${mysqlOptions.port}:3306 -d mysql:5.7`;
        debug('running the command', command);
        const process = exec(command);

        // wait until process has exited
        debug('downloading mysql image or creating a container. waiting for child process to return an exit code');
        do {
            await sleepAsync(1000);
        } while (process.exitCode == null);

        debug('child process exited with exit code: ', process.exitCode);


        debug('waiting for mysql database to start...');
        let retries = 0;
        let gaveUp = true;
        let conn = null;
        do {
            try {
                conn = mysql.createConnection(mysqlOptions);

                Bluebird.promisifyAll(conn);

                await conn.connectAsync();
                debug('connected!');
                gaveUp = false;
                break;
            } catch (error) {
                conn.end();
                debug(`mysql retry attempt ${retries + 1} after 1000ms`);
                await sleepAsync(1000);
                retries++;
            }
        } while (retries < 20);

        if (gaveUp) {
            console.error('given up connecting to mysql database');
            console.error('abandoning tests');
        } else {
            debug('successfully connected to mysql database');
        }
    } catch (error) {
        console.error('error in setting up the container', error);
    }

});

set('onComplete', async function() {
    debug('onComplete')
    // exec(`docker rm ${containerName} --force`);
});

// suite('eventstore playbacklist mysql store add', () => {
//     let playbackListStore;
//     const listName = 'vehicle_list';
//     set('setup', async function() {
//         debug('setup');
//         playbackListStore = new EventstorePlaybackListMySqlStore(mysqlOptions);
//         await playbackListStore.init();
//         await playbackListStore.createList({
//             name: listName,
//             fields: [{
//                 name: 'vehicleId',
//                 type: 'string'
//             }, {
//                 name: 'spread',
//                 type: 'decimal'
//             }],
//             secondaryKeys: {
//                 idx_vehicleId: [{
//                     name: 'vehicleId',
//                     sort: 'ASC'
//                 }]
//             }
//         });
//     });

//     set('teardown', async function() {
//         await playbackListStore.deleteList(listName);
//     });

//     bench('ops', async function() {
//         const vehicleId = shortid.generate();
//         const data = {
//             vehicleId: vehicleId,
//             spread: 1000
//         }
//         await playbackListStore.add(listName, data.vehicleId, 0, data);
//     });
// });


// suite('eventstore playbacklist mysql store update', () => {
//     let playbackListStore;
//     const listName = 'vehicle_list';
//     let numberOfVehicles = 100;
//     let counter = 0;
//     set('setup', async function() {
//         debug('setup');
//         playbackListStore = new EventstorePlaybackListMySqlStore(mysqlOptions);
//         await playbackListStore.init();
//         await playbackListStore.createList({
//             name: listName,
//             fields: [{
//                 name: 'vehicleId',
//                 type: 'string'
//             }, {
//                 name: 'spread',
//                 type: 'decimal'
//             }],
//             secondaryKeys: {
//                 idx_vehicleId: [{
//                     name: 'vehicleId',
//                     sort: 'ASC'
//                 }]
//             }
//         });

//         for (let index = 0; index < numberOfVehicles; index++) {
//             const data = {
//                 vehicleId: 'vehicle_' + index,
//                 spread: 1000
//             }
//             await playbackListStore.add(listName, data.vehicleId, 0, data);
//         }
//     });

//     set('teardown', async function() {
//         await playbackListStore.deleteList(listName);
//     });

//     bench('ops', async function() {
//         // listName, rowId, revision, data, meta
//         const vehicleId = 'vehicle_' + Math.floor(Math.random() * numberOfVehicles);

//         counter++;
//         const data = {
//             vehicleId: vehicleId,
//             spread: counter
//         }
//         await playbackListStore.update(listName, vehicleId, counter, data);
//     });
// });


// suite('eventstore statelist mysql store delete', () => {
//     let playbackListStore;
//     const listName = 'vehicle_list';
//     let numberOfVehicles = 10000;
//     let counter = 0;
//     set('setup', async function() {
//         debug('setup');
//         playbackListStore = new EventstorePlaybackListMySqlStore(mysqlOptions);
//         await playbackListStore.init();
//         await playbackListStore.createList({
//             name: listName,
//             fields: [{
//                 name: 'vehicleId',
//                 type: 'string'
//             }, {
//                 name: 'spread',
//                 type: 'decimal'
//             }],
//             secondaryKeys: {
//                 idx_vehicleId: [{
//                     name: 'vehicleId',
//                     sort: 'ASC'
//                 }]
//             }
//         });

//         for (let index = 0; index < numberOfVehicles; index++) {
//             const data = {
//                 vehicleId: 'vehicle_' + index,
//                 spread: 1000
//             }
//             await playbackListStore.add(listName, data.vehicleId, 0, data);
//         }
//     });

//     set('teardown', async function() {
//         await playbackListStore.deleteList(listName);
//     });

//     bench('ops', async function() {
//         // listName, rowId, revision, data, meta
//         const vehicleId = 'vehicle_' + counter++;

//         await playbackListStore.delete(listName, vehicleId);
//     });
// });

// suite('eventstore statelist mysql store get', () => {
//     let playbackListStore;
//     const listName = 'vehicle_list';
//     let numberOfVehicles = 100;
//     let counter = 0;
//     set('setup', async function() {
//         debug('setup');
//         playbackListStore = new EventstorePlaybackListMySqlStore(mysqlOptions);
//         await playbackListStore.init();
//         await playbackListStore.createList({
//             name: listName,
//             fields: [{
//                 name: 'vehicleId',
//                 type: 'string'
//             }, {
//                 name: 'spread',
//                 type: 'decimal'
//             }],
//             secondaryKeys: {
//                 idx_vehicleId: [{
//                     name: 'vehicleId',
//                     sort: 'ASC'
//                 }]
//             }
//         });

//         for (let index = 0; index < numberOfVehicles; index++) {
//             const data = {
//                 vehicleId: 'vehicle_' + index,
//                 spread: 1000
//             }
//             await playbackListStore.add(listName, data.vehicleId, 0, data);
//         }
//     });

//     set('teardown', async function() {
//         await playbackListStore.deleteList(listName);
//     });

//     bench('ops', async function() {
//         const vehicleId = 'vehicle_' + Math.floor(Math.random() * numberOfVehicles);

//         await playbackListStore.get(listName, vehicleId);
//     });
// });


suite('eventstore playbacklist mysql store with 1million rows', () => {
    let playbackListStore;
    const listName = 'vehicle_list';
    let seedFileName = path.join(__dirname, 'playbacklist-vehicles.csv');
    let counter = 1000000;
    set('setup', async function() {
        debug('setup');
        playbackListStore = new EventstorePlaybackListMySqlStore(mysqlOptions);
        await playbackListStore.init();
        await playbackListStore.createList({
            name: listName,
            fields: [{
                name: 'vehicleId',
                type: 'string'
            }, {
                name: 'spread',
                type: 'decimal'
            }],
            secondaryKeys: {
                idx_vehicleId: [{
                    name: 'vehicleId',
                    sort: 'ASC'
                }]
            }
        });

        const createTestFile = async function() {
            const async = require('async');
            const q = async.queue(async (index) => {
                const data = {
                    vehicleId: 'vehicle_' + index,
                    spread: index
                }

                await fs.appendFile(seedFileName,`"${data.vehicleId}","0",${JSON.stringify(JSON.stringify(data))},"null"\r\n`);
            }, 100000);
    
            for (let index = 0; index < counter; index++) {
                q.push(index);
            }
    
            const waitToDrain = async function() {
                return new Promise((resolve) => {
                    q.drain = resolve;
                });
            }
        
            await waitToDrain();
        }
        
        if (!require('fs').existsSync(seedFileName)) {
            debug(`file ${seedFileName} does not exist. creating.`);
            await createTestFile();
        } else {
            debug(`file ${seedFileName} exists. skipping.`);
        }

        const sql = `
            LOAD DATA LOCAL INFILE '${seedFileName}' INTO TABLE ${listName}
            FIELDS TERMINATED BY ',' 
            ENCLOSED BY '"' 
            LINES TERMINATED BY '\r\n'
            (row_id, row_revision, row_json, meta_json);
        `;

        const conn = mysql.createConnection(mysqlOptions);

        Bluebird.promisifyAll(conn);

        await conn.connectAsync();
        await conn.queryAsync(sql);
        await conn.endAsync();
    });

    set('teardown', async function() {
        await playbackListStore.deleteList(listName);
    });

    bench('add ops', async function() {
        counter++;
        const vehicleId = 'vehicle_' + counter;
        const data = {
            vehicleId: vehicleId,
            spread: counter
        }
        await playbackListStore.add(listName, data.vehicleId, 0, data);
    });

    bench('update ops', async function() {
        const vehicleId = 'vehicle_' + Math.floor(Math.random() * counter);

        const data = {
            vehicleId: vehicleId,
            spread: counter
        }
        await playbackListStore.update(listName, vehicleId, counter, data);
    });


    bench('get ops', async function() {
        const vehicleId = 'vehicle_' + Math.floor(Math.random() * counter);
        await playbackListStore.get(listName, vehicleId);
    });

    
    bench('delete ops', async function() {
        const vehicleId = 'vehicle_' + Math.floor(Math.random() * counter);
        await playbackListStore.delete(listName, vehicleId);
    });

});
