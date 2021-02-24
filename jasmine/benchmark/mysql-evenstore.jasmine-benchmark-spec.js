'use strict';
const Docker = require('dockerode');
const path = require('path');
const mysql = require('mysql');
const childProcess = require('child_process');
const exec = childProcess.exec;
const shortid = require('shortid');
const Bluebird = require('bluebird');
const fs = require('fs');

const mysqlOptions = {
    host: 'localhost',
    port: 23306,
    user: 'root',
    password: 'root',
    database: 'eventstore',
    connectionLimit: '1'
};

const eventstoreMysqlType = 'mysql-optimized';
const usingEventDispatcher = true;
const numberOfEvents = 2;

const fileText = fs.readFileSync(path.join(__dirname, 'vehicle-created.sample-event.json'));
// const fileText = fs.readFileSync(path.join(__dirname, 'sciv-leader-computed.sample-event.json'));
const eventToWrite = JSON.parse(fileText).toString();

console.log('==========================');
console.log('ioredis: ' + require('../../package.json').version);
console.log('node_redis: ' + require('redis/package.json').version);
var os = require('os');
console.log('CPU: ' + os.cpus().length);
console.log('OS: ' + os.platform() + ' ' + os.arch());
console.log('node version: ' + process.version);
console.log('mysql connectionLimit: ' + mysqlOptions.connectionLimit);
console.log('mysql numberOfEvents: ' + numberOfEvents);
console.log('mysql event payload length: ' + fileText.length);
console.log('mysql eventstore type: ' + eventstoreMysqlType);
console.log('mysql eventstore usingEventDispatcher: ' + usingEventDispatcher);
console.log('current commit: ' + childProcess.execSync('git rev-parse --short HEAD'));
console.log('==========================');



const sleepAsync = function(sleepUntil) {
    return new Promise((resolve) => {
        setTimeout(resolve, sleepUntil);
    });
}

// NOTE: use mysql-based eventstore w/expanded bookmarked getEvents and archiving function
var eventstore = require('../../index');

const docker = new Docker({
    socketPath: '/var/run/docker.sock'
});
const dumpFolder = path.resolve('../dump');

let mysqlContainer;



console.log('module.filename', module.filename);
const containerName = 'mysql-eventstore.jasmine-benchmark-spec.js';

set('setup', async function() {
    try {
        // console.log('creating the container...');
        // mysqlContainer = await docker.createContainer({
        //     Image: 'mysql:5.7',
        //     Tty: true,
        //     Cmd: '--default-authentication-plugin=mysql_native_password',
        //     Env: [
        //         `MYSQL_USER=${mysqlOptions.user}`,
        //         `MYSQL_PASSWORD=${mysqlOptions.password}`,
        //         `MYSQL_ROOT_PASSWORD=${mysqlOptions.password}`,
        //         `MYSQL_DATABASE=${mysqlOptions.database}`
        //     ],
        //     HostConfig: {
        //         Binds: [`${dumpFolder}:/docker-entrypoint-initdb.d`],
        //         PortBindings: {
        //             '3306/tcp': [{
        //                 HostPort: mysqlOptions.port
        //             }]
        //         }
        //     },
        //     name: 'mysql-eventstore.jasmine-benchmark-spec.js'
        // });

        console.log('creating the container...');
        const command = `docker run --name ${containerName} -e MYSQL_ROOT_PASSWORD=${mysqlOptions.password} -e MYSQL_DATABASE=${mysqlOptions.database} -p ${mysqlOptions.port}:3306 -d mysql:5.7`;
        const process = exec(command);

        // wait until process has exited
        console.log('downloading mysql image or creating a container. waiting for child process to return an exit code');
        do {
            await sleepAsync(1000);
        } while (process.exitCode == null);

        console.log('child process exited with exit code: ', process.exitCode);

        // console.log('starting the container...');

        // await mysqlContainer.start();

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
    } catch (error) {
        console.error('error in setting up the container', error);
    }

});


suite('mysql eventstore', () => {
    let es;
    const snapshotTestAggregateId = 'snapshot-aggregate-id';
    set('setup', function(next) {
        es = eventstore({
            type: eventstoreMysqlType,
            host: mysqlOptions.host,
            port: mysqlOptions.port,
            user: mysqlOptions.user,
            password: mysqlOptions.password,
            database: mysqlOptions.database,
            connectionPoolLimit: mysqlOptions.connectionLimit,
            usingEventDispatcher: usingEventDispatcher
        });

        es.useEventPublisher(function(event, done) {
            done();
        });

        es.init(function() {
            console.log('es.init called');
            const query = {
                context: 'default',
                aggregate: 'default',
                aggregateId: snapshotTestAggregateId
            };

            // for snapshot tests. add a snapshot and 10 more events
            es.getLastEventAsStream(query, function(err, stream) {
                es.createSnapshot({
                    aggregateId: query.aggregateId,
                    context: query.context,
                    aggregate: query.aggregate,
                    data: eventToWrite,
                    revision: stream.lastRevision,
                    version: 1 // optional
                }, function(err) {
                    // add 10 events
                    for (let index = 0; index < 10; index++) {
                        stream.addEvent(eventToWrite);
                    }

                    stream.commit(next);
                });
            });
        });
    });
    set('teardown', function(next) {
        es = undefined;
        next();
    });

    bench('get snapshot and get 10 events', function(next) {
        const query = {
            context: 'default',
            aggregate: 'default',
            aggregateId: snapshotTestAggregateId
        };
        es.getFromSnapshot(query, next);
    });

    bench('add multiple events to single stream', function(next) {
        const query = {
            context: 'default',
            aggregate: 'default',
            aggregateId: 'streamId'
        }
        es.getLastEventAsStream(query, function(err, stream) {
            if (err) {
                next(err);
            } else {
                for (let index = 0; index < numberOfEvents; index++) {
                    stream.addEvent(eventToWrite);
                }
                
                stream.commit(next);
            }
        });
    });

    bench('add multiple events to different streams', function(next) {
        const randId = shortid.generate();
        const query = {
            context: 'default',
            aggregate: 'default',
            aggregateId: randId
        }
        es.getLastEventAsStream(query, function(err, stream) {
            if (err) {
                next(err);
            } else {
                for (let index = 0; index < numberOfEvents; index++) {
                    stream.addEvent(eventToWrite);
                }
                
                stream.commit(next);
            }
        });
    });
});

// TODO: this is not getting called
set('teardown', async function() {
    exec(`docker rm ${containerName} --force`);
});