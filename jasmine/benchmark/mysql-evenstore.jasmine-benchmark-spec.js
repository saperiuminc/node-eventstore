'use strict';
const Docker = require('dockerode');
const path = require('path');
const mysql = require('mysql');
const childProcess = require('child_process');
const exec = childProcess.exec;
const shortid = require('shortid');
const Bluebird = require('bluebird');
const fs = require('fs');
const debug = require('debug')('BENCH');

const mysqlOptions = {
    host: 'localhost',
    port: 23306,
    user: 'root',
    password: 'root',
    database: 'eventstore',
    connectionLimit: '3'
};

const eventstoreMysqlType = 'mysql';
const usingEventDispatcher = true;
const numberOfEvents = 2;

const fileText = fs.readFileSync(path.join(__dirname, 'vehicle-created.sample-event.json'));
// const fileText = fs.readFileSync(path.join(__dirname, 'sciv-leader-computed.sample-event.json'));
const eventToWrite = JSON.parse(fileText).toString();

debug('==========================');
debug('ioredis: ' + require('../../package.json').version);
debug('node_redis: ' + require('redis/package.json').version);
var os = require('os');
debug('CPU: ' + os.cpus().length);
debug('OS: ' + os.platform() + ' ' + os.arch());
debug('node version: ' + process.version);
debug('mysql connectionLimit: ' + mysqlOptions.connectionLimit);
debug('mysql numberOfEvents: ' + numberOfEvents);
debug('mysql event payload length: ' + fileText.length);
debug('mysql eventstore type: ' + eventstoreMysqlType);
debug('mysql eventstore usingEventDispatcher: ' + usingEventDispatcher);
debug('current commit: ' + childProcess.execSync('git rev-parse --short HEAD'));
debug('==========================');



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



const extension = path.extname(module.filename);
const containerName = path.basename(module.filename, extension);

set('setup', async function() {
    try {
        // debug('creating the container...');
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

        // debug('starting the container...');

        // await mysqlContainer.start();

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

        // es.useEventPublisher(function(event, done) {
        //     done();
        // });

        es.init(function() {
            debug('es.init called');
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

suite('mysql eventstore with useEventPublisher', () => {
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
            debug('es.init called');
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