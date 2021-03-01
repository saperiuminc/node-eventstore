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
var EventstoreStateListMySqlStore = require('../../lib/eventstore-projections/eventstore-statelist-mysql-store');

const extension = path.extname(module.filename);
const containerName = path.basename(module.filename, extension);

const mysqlOptions = {
    host: 'localhost',
    port: 33306,
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

suite('eventstore statelist mysql store push', () => {
    let stateListStore;
    const listName = 'user_state_list';
    set('setup', async function() {
        debug('setup');
        stateListStore = new EventstoreStateListMySqlStore(mysqlOptions);
        await stateListStore.init();
        await stateListStore.createList({
            name: listName,
            fields: [{
                name: 'userId',
                type: 'string'
            }],
            secondaryKeys: {
                idx_userId: [{
                    name: 'userId',
                    sort: 'ASC'
                }]
            }
        });
    });

    set('teardown', async function() {
        await stateListStore.deleteList(listName);
    });

    bench('ops', async function() {
        await stateListStore.push(listName, { userId: 'user_id', firstName: 'Ryan', lastName: 'Goce'});
    });
});


suite('eventstore statelist mysql store set', () => {
    let stateListStore;
    const listName = 'user_state_list';
    set('setup', async function() {
        debug('setup');
        stateListStore = new EventstoreStateListMySqlStore(mysqlOptions);
        await stateListStore.init();
        await stateListStore.createList({
            name: listName,
            fields: [{
                name: 'userId',
                type: 'string'
            }],
            secondaryKeys: {
                idx_userId: [{
                    name: 'userId',
                    sort: 'ASC'
                }]
            }
        });

        // add one item
        await stateListStore.push(listName, { userId: 'user_id', firstName: 'Ryan', lastName: 'Goce'});
    });

    set('teardown', async function() {
        await stateListStore.deleteList(listName);
    });

    bench('ops', async function() {
        await stateListStore.set(listName, 0, { userId: 'user_id', firstName: 'Ryan', lastName: 'Goce'});
    });
});


suite('eventstore statelist mysql store delete', () => {
    let stateListStore;
    const listName = 'user_state_list';
    set('setup', async function() {
        debug('setup');
        stateListStore = new EventstoreStateListMySqlStore(mysqlOptions);
        await stateListStore.init();
        await stateListStore.createList({
            name: listName,
            fields: [{
                name: 'userId',
                type: 'string'
            }],
            secondaryKeys: {
                idx_userId: [{
                    name: 'userId',
                    sort: 'ASC'
                }]
            }
        });

        // the test is only 6 seconds and the max i saw is around 200 ops/sec
        // so we can just add at least 1200. just set to 2k for now. should be more than enough
        // add 2k items
        for (let index = 0; index < 2000; index++) {
            await stateListStore.push(listName, { userId: 'user_id', firstName: 'Ryan', lastName: 'Goce'});
        }
    });

    set('teardown', async function() {
        await stateListStore.deleteList(listName);
    });

    let iteration = 0;
    bench('ops', async function() {
        await stateListStore.delete(listName, iteration);
        iteration++;
    });
});

suite('eventstore statelist mysql store find', () => {
    let stateListStore;
    const listName = 'user_state_list';
    set('setup', async function() {
        debug('setup');
        stateListStore = new EventstoreStateListMySqlStore(mysqlOptions);
        await stateListStore.init();
        await stateListStore.createList({
            name: listName,
            fields: [{
                name: 'userId',
                type: 'string'
            }],
            secondaryKeys: {
                idx_userId: [{
                    name: 'userId',
                    sort: 'ASC'
                }]
            }
        });

        await stateListStore.push(listName, { userId: 'user_id', firstName: 'Ryan', lastName: 'Goce'});
    });

    set('teardown', async function() {
        await stateListStore.deleteList(listName);
    });

    bench('ops', async function() {
        const item = await stateListStore.find(listName, 1, [{
            field: 'userId',
            operator: 'is',
            value: 'user_id'
        }]);
    });
});


suite('eventstore statelist mysql store with 1million rows', () => {
    let stateListStore;
    const listName = 'user_state_list';
    let seedFileName = path.join(__dirname, 'users.csv');
    let counter = 0;
    set('setup', async function() {
        debug('setup');
        counter = 0;
        stateListStore = new EventstoreStateListMySqlStore(mysqlOptions);
        await stateListStore.init();
        await stateListStore.createList({
            name: listName,
            fields: [{
                name: 'userId',
                type: 'string'
            }],
            secondaryKeys: {
                idx_userId: [{
                    name: 'userId',
                    sort: 'ASC'
                }]
            }
        });

        const createTestFile = async function() {
            const async = require('async');
            const q = async.queue(async (index) => {
                const state = {
                    userId: 'user' + index,
                    accessDate: Date.now()
                }

                await fs.appendFile(seedFileName,`"${'CREATE'}","${index}",${JSON.stringify(JSON.stringify(state))},"null"\r\n`);
            }, 100000);
    
            for (let index = 0; index < 1000000; index++) {
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
            (row_type, row_index, state_json, meta_json);
        `;

        const conn = mysql.createConnection(mysqlOptions);

        Bluebird.promisifyAll(conn);

        await conn.connectAsync();
        await conn.queryAsync(sql);
        await conn.endAsync();
    });

    set('teardown', async function() {
        await stateListStore.deleteList(listName);
    });

    bench('push ops', async function() {
        await stateListStore.push(listName, { userId: 'user', firstName: 'Ryan', lastName: 'Goce'});
    });

    bench('set ops', async function() {
        await stateListStore.set(listName, counter, { userId: 'user' + counter, firstName: 'Ryan', lastName: 'Goce'});
        counter++;
    });

    bench('find ops', async function() {
        const item = await stateListStore.find(listName, 234567, [{
            field: 'userId',
            operator: 'is',
            value: 'user' + counter
        }]);

        counter++;
    });

});