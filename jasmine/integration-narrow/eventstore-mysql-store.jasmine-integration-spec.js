const Bluebird = require('bluebird');
const MySQLStore = require('../../lib/databases/mysql'); // toggle v2 and v1 mysql
const shortid = require('shortid');
const _ = require('lodash');

const mysqlOptions = {
    host: 'localhost',
    port: 23306,
    user: 'root',
    password: 'root',
    database: 'eventstore',
    connectionLimit: '2'
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
            const command = `docker run --name eventstore_mysql -e MYSQL_ROOT_PASSWORD=${mysqlOptions.password} -e MYSQL_DATABASE=${mysqlOptions.database} -p ${mysqlOptions.port}:3306 -d mysql:5.7`;
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
            exec('docker rm eventstore_mysql --force');
        }
    }
})();

describe('eventstore-mysql-store tests', () => {
    let mysqlStore = new MySQLStore({});
    let listName;
    beforeAll(async (done) => {
        await mysqlServer.up();
        done();
    }, 60000);

    beforeEach(async (done) => {
        mysqlStore = new MySQLStore(mysqlOptions);
        Bluebird.promisifyAll(mysqlStore);
        done();
    });

    describe('connect test', () => {
        it('should connect to the store with no errors', async (done) => {
            await mysqlStore.connectAsync();
            done();
        });
    })

    describe('connected tests', () => {
        beforeEach(async (done) => {
            await mysqlStore.connectAsync();
            done();
        })

        describe('disconnect', () => {
            it('should disconnect to the store with no errors', async (done) => {
                await mysqlStore.disconnectAsync();
                done();
            });
        })
    
        describe('clear', () => {
            it('should clear the store with no errors', async (done) => {
                await mysqlStore.clearAsync();
                done();
            });
        })
    
        describe('addEvents', () => {
            it('should add the event in the store', async () => {
                const aggregateId = shortid.generate();
                const newEvent = {
                    id: shortid.generate(),
                    aggregateId: aggregateId,
                    streamId: aggregateId,
                    aggregate: 'vehicle',
                    context: 'vehicle',
                    streamRevision: 5,
                    commitId: shortid.generate(),
                    commitStamp: new Date('2020-03-06T00:28:36.728Z'),
                    commitSequence: 0,
                    payload: {
                        name: 'mock_event_added',
                        payload: 'mockPayload',
                        aggregateId: aggregateId
                    },
                    restInCommitStream: 0
                };

                await mysqlStore.addEventsAsync([newEvent]);
                const query = {
                    context: 'vehicle',
                    aggregate: 'vehicle',
                    aggregateId: newEvent.aggregateId
                };
                const skip = 0;
                const limit = 1;
                const events = await mysqlStore.getEventsAsync(query, skip, limit);
                expect(events.length).toEqual(1);
            });

            it('should not throw an error even if events length is 0', async () => {
                await mysqlStore.addEventsAsync([]);
            })
        })
    
        describe('getEvents', () => {
            let expectedEvents;
            beforeEach(async (done) => {
                expectedEvents = [];
                for (let index = 0; index < 20; index++) {
                    const event = {
                        id: shortid.generate(),
                        aggregateId: `vehicle_${index}`,
                        streamId: `vehicle_${index}`,
                        aggregate: index % 2 == 0 ? 'vehicle' : 'salesChannelInstanceVehicle',
                        context: index % 2 == 0 ? 'vehicle' : 'auction',
                        streamRevision: 1,
                        commitId: shortid.generate(),
                        commitStamp: new Date(),
                        commitSequence: index,
                        payload: {
                            name: 'mock_event_added',
                            payload: 'mockPayload',
                            aggregateId: `vehicle_${index}`
                        },
                        restInCommitStream: 0,
                        position: null,
                        eventSequence: index + 1
                    }

                    expectedEvents.push(event);
                }

                const eventsToAdd = _.map(expectedEvents, (item) => {
                    const event = _.clone(item);
                    // removed event sequence in the expectation because the sequence should be added by the store
                    delete event['eventSequence'];

                    return event;
                });

                await mysqlStore.addEventsAsync(eventsToAdd);
                done();
            });

            it('should get the events from the store using context', async (done) => {
                const query = {
                    context: 'vehicle'
                };
                const skip = 0;
                const limit = 20;
                const events = await mysqlStore.getEventsAsync(query, skip, limit);
                expect(events.length).toEqual(10);
                done();
            });

            it('should get the events from the store using context and aggregate', async (done) => {
                const query = {
                    context: 'vehicle',
                    aggregate: 'vehicle'
                };
                const skip = 0;
                const limit = 20;
                const events = await mysqlStore.getEventsAsync(query, skip, limit);
                expect(events.length).toEqual(10);
                done();
            });

            it('should get the events from the store using context, aggregate and aggregateId', async (done) => {
                const query = {
                    context: 'vehicle',
                    aggregate: 'vehicle',
                    aggregateId: 'vehicle_10'
                };
                const skip = 0;
                const limit = 20;
                const events = await mysqlStore.getEventsAsync(query, skip, limit);
                expect(events[0]).toEqual(expectedEvents[10]);
                done();
            });

            it('should page using skip and limit', async (done) => {
                const query = {
                    context: 'vehicle',
                    aggregate: 'vehicle'
                };
                
                // NOTE: skip in getEvents is using the global event sequence number and not using mysql offset order by commit_stamp
                // NOTE: last_seen in this example is eventSequence = 9
                // const skip = 5;
                const skip = 9;
                const limit = 5;
                const events = await mysqlStore.getEventsAsync(query, skip, limit);

                expect(events[0].aggregateId).toEqual('vehicle_10');
                expect(events[1].aggregateId).toEqual('vehicle_12');
                expect(events[2].aggregateId).toEqual('vehicle_14');
                expect(events[3].aggregateId).toEqual('vehicle_16');
                expect(events[4].aggregateId).toEqual('vehicle_18');
                expect(events.length).toEqual(5);
                done();
            });

        })

        describe('getEventsSince', () => {
            it('should get the events since the date, skip and limit', async (done) => {
                const expectedEvents = [];
                for (let index = 0; index < 12; index++) {
                    const event = {
                        id: shortid.generate(),
                        aggregateId: `vehicle_${index}`,
                        streamId: `vehicle_${index}`,
                        aggregate: index % 2 == 0 ? 'vehicle' : 'salesChannelInstanceVehicle',
                        context: index % 2 == 0 ? 'vehicle' : 'auction',
                        streamRevision: 1,
                        commitId: shortid.generate(),
                        commitStamp: new Date(2020, (index % 12), 1),
                        commitSequence: index,
                        payload: {
                            name: 'mock_event_added',
                            payload: 'mockPayload',
                            aggregateId: `vehicle_${index}`
                        },
                        restInCommitStream: 0,
                        eventSequence: index + 1
                    }

                    expectedEvents.push(event);
                }

                const eventsToAdd = _.map(expectedEvents, (item) => {
                    const event = _.clone(item);
                    // removed event sequence in the expectation because the sequence should be added by the store
                    delete event['eventSequence'];

                    return event;
                });

                await mysqlStore.addEventsAsync(eventsToAdd);

                const events = await mysqlStore.getEventsSinceAsync(new Date(2020, 6, 1).getTime(), 3, 3);

                expect(events[0].aggregateId).toEqual('vehicle_9');
                expect(events[1].aggregateId).toEqual('vehicle_10');
                expect(events[2].aggregateId).toEqual('vehicle_11');
                expect(events.length).toEqual(3);
                done();
            })
        })

        describe('getEventsByRevision', () => {
            it('should get the events by revision, revision min and max', async (done) => {
                const expectedEvents = [];
                for (let index = 1; index <= 10; index++) {
                    const event = {
                        id: shortid.generate(),
                        aggregateId: `vehicle_1`,
                        streamId: `vehicle_1`,
                        aggregate: 'vehicle',
                        context: 'vehicle',
                        streamRevision: index,
                        commitId: shortid.generate(),
                        commitStamp: new Date(),
                        commitSequence: index,
                        payload: {
                            name: 'mock_event_added',
                            payload: 'mockPayload',
                            aggregateId: `vehicle_1`
                        },
                        restInCommitStream: 0,
                        position: null,
                        eventSequence: index
                    }

                    expectedEvents.push(event);
                }

                const eventsToAdd = _.map(expectedEvents, (item) => {
                    const event = _.clone(item);
                    // removed event sequence in the expectation because the sequence should be added by the store
                    delete event['eventSequence'];

                    return event;
                });

                await mysqlStore.addEventsAsync(eventsToAdd);

                const query = {
                    context: 'vehicle',
                    aggregate: 'vehicle',
                    aggregateId: 'vehicle_1'
                }
                const events = await mysqlStore.getEventsByRevisionAsync(query, 6, 11);

                expect(events[0]).toEqual(expectedEvents[5]);
                expect(events[1]).toEqual(expectedEvents[6]);
                expect(events[2]).toEqual(expectedEvents[7]);
                expect(events[3]).toEqual(expectedEvents[8]);
                expect(events[4]).toEqual(expectedEvents[9]);
                expect(events.length).toEqual(5);
                done();
            })
        })

        describe('getLastEvent', () => {
            let expectedEvents;
            beforeEach(async (done) => {
                let eventSequence = 1;
                expectedEvents = [];
                // 10 events for vehice vehicle vehicle_1 AND
                for (let index = 1; index <= 10; index++) {
                    const vehicleEvent = {
                        id: shortid.generate(),
                        aggregateId: `vehicle_1`,
                        streamId: `vehicle_1`,
                        aggregate: 'vehicle',
                        context:'vehicle',
                        streamRevision: index,
                        commitId: shortid.generate(),
                        commitStamp: new Date(2020, 1, 1, 1, 1, index),
                        commitSequence: index,
                        payload: {
                            name: 'mock_event_added',
                            payload: 'mockPayload',
                            aggregateId: `vehicle_1`
                        },
                        restInCommitStream: 0,
                        position: null,
                        eventSequence: eventSequence++
                    }
                    expectedEvents.push(vehicleEvent);
                }

                // 10 events for auction salesChannelInstanceVehicle sciv_1 AND
                for (let index = 1; index <= 10; index++) {
                    const scivEvent = {
                        id: shortid.generate(),
                        aggregateId: `sciv_1`,
                        streamId: `sciv_1`,
                        aggregate: 'salesChannelInstanceVehicle',
                        context:'auction',
                        streamRevision: index,
                        commitId: shortid.generate(),
                        commitStamp: new Date(2020, 1, 1, 1, 1, 10 + index),
                        commitSequence: index,
                        payload: {
                            name: 'mock_event_added',
                            payload: 'mockPayload',
                            aggregateId: `sciv_1`
                        },
                        restInCommitStream: 0,
                        position: null,
                        eventSequence: eventSequence++
                    }
                    
                    expectedEvents.push(scivEvent);
                }

                // 10 events for auction salesChannel salesChannel_1
                for (let index = 1; index <= 10; index++) {
                    const salesChannelEvent = {
                        id: shortid.generate(),
                        aggregateId: `salesChannel_1`,
                        streamId: `salesChannel_1`,
                        aggregate: 'salesChannel',
                        context:'auction',
                        streamRevision: index,
                        commitId: shortid.generate(),
                        commitStamp: new Date(2020, 1, 1, 1, 1, 20 + index),
                        commitSequence: index,
                        payload: {
                            name: 'mock_event_added',
                            payload: 'mockPayload',
                            aggregateId: `salesChannel_1`
                        },
                        restInCommitStream: 0,
                        position: null,
                        eventSequence: eventSequence++
                    }
                    
                    expectedEvents.push(salesChannelEvent);
                }

                const eventsToAdd = _.map(expectedEvents, (item) => {
                    const event = _.clone(item);
                    // removed event sequence in the expectation because the sequence should be added by the store
                    delete event['eventSequence'];

                    return event;
                });

                await mysqlStore.addEventsAsync(eventsToAdd);
                done();
            });

            it('should get the last event using context', async (done) => {
                const query = {
                    context: 'auction'
                };
                const event = await mysqlStore.getLastEventAsync(query);

                expect(event).toEqual(expectedEvents[expectedEvents.length - 1]);
                done();
            })

            it('should get the last event using context and aggregate', async (done) => {
                const query = {
                    context: 'auction',
                    aggregate: 'salesChannelInstanceVehicle'

                };
                const event = await mysqlStore.getLastEventAsync(query);

                expect(event).toEqual(expectedEvents[19]);
                done();
            })

            it('should get the last event using context, aggregate and aggregateId', async (done) => {
                const query = {
                    context: 'vehicle',
                    aggregate: 'vehicle',
                    aggregateId: 'vehicle_1'

                };
                const event = await mysqlStore.getLastEventAsync(query);

                expect(event).toEqual(expectedEvents[9]);
                done();
            })
        })

        describe('getUndispatchedEvents', () => {
            let expectedEvents;
            beforeEach(async (done) => {
                expectedEvents = [];
                for (let index = 0; index < 20; index++) {
                    const event = {
                        id: shortid.generate(),
                        aggregateId: `vehicle_${index}`,
                        streamId: `vehicle_${index}`,
                        aggregate: index % 2 == 0 ? 'vehicle' : 'salesChannelInstanceVehicle',
                        context: index % 2 == 0 ? 'vehicle' : 'auction',
                        streamRevision: 1,
                        commitId: shortid.generate(),
                        commitStamp: new Date(),
                        commitSequence: index,
                        payload: {
                            name: 'mock_event_added',
                            payload: 'mockPayload',
                            aggregateId: `vehicle_${index}`
                        },
                        position: null,
                        eventSequence: index + 1,
                        restInCommitStream: 0
                    }

                    expectedEvents.push(event);
                }

                const eventsToAdd = _.map(expectedEvents, (item) => {
                    const event = _.clone(item);
                    // removed event sequence in the expectation because the sequence should be added by the store
                    delete event['eventSequence'];

                    return event;
                });

                await mysqlStore.addEventsAsync(eventsToAdd);
                done();
            });

            it('should get the events from the store using context', async (done) => {
                const query = {
                    context: 'vehicle'
                };
                const events = await mysqlStore.getUndispatchedEventsAsync(query);
                expect(events.length).toEqual(10);
                done();
            });

            it('should get the events from the store using context and aggregate', async (done) => {
                const query = {
                    context: 'vehicle',
                    aggregate: 'vehicle'
                };
                const events = await mysqlStore.getUndispatchedEventsAsync(query);
                expect(events.length).toEqual(10);
                done();
            });

            it('should get the events from the store using context, aggregate and aggregateId', async (done) => {
                const query = {
                    context: 'vehicle',
                    aggregate: 'vehicle',
                    aggregateId: 'vehicle_10'
                };
                const events = await mysqlStore.getUndispatchedEventsAsync(query);
                expect(events[0]).toEqual(expectedEvents[10]);
                done();
            });
        })
        
        describe('setEventToDispatched', () => {
            it('should set the event to dispatched', async (done) => {
                const aggregateId = shortid.generate();
                const newEvent = {
                    id: shortid.generate(),
                    aggregateId: aggregateId,
                    streamId: aggregateId,
                    aggregate: 'vehicle',
                    context: 'vehicle',
                    streamRevision: 5,
                    commitId: shortid.generate(),
                    commitStamp: new Date('2020-03-06T00:28:36.728Z'),
                    commitSequence: 0,
                    payload: {
                        name: 'mock_event_added',
                        payload: 'mockPayload',
                        aggregateId: aggregateId
                    },
                    position: null,
                    restInCommitStream: 0,
                    eventSequence: 1
                };

                await mysqlStore.addEventsAsync([newEvent]);
               
                await mysqlStore.setEventToDispatchedAsync(newEvent.id);

                const query = {
                    context: 'vehicle',
                    aggregate: 'vehicle',
                    aggregateId: newEvent.aggregateId
                };

                const undispatchedEvents = await mysqlStore.getUndispatchedEventsAsync(query);
                expect(undispatchedEvents.length).toEqual(0);
                done();
            });
        })
        
        describe('snapshots', () => {
            it('should add and get the snapshot', async (done) => {
                const snapshotId = shortid.generate();
                const aggregateId = shortid.generate();
                const snapshot = {
                    id: snapshotId,
                    aggregateId: aggregateId,
                    streamId: aggregateId,
                    aggregate: 'vehicle',
                    context: 'vehicle',
                    revision: 3,
                    commitStamp: new Date('2020-04-06T00:28:36.728Z'),
                    data: {
                        state: 'created',
                        aggregateId: aggregateId
                    }
                };

                await mysqlStore.addSnapshotAsync(snapshot);

                const query = {
                    context: 'vehicle',
                    aggregate: 'vehicle',
                    aggregateId: aggregateId
                };

                const dbSnapshot = await mysqlStore.getSnapshotAsync(query, 10)

                expect(dbSnapshot).toEqual(snapshot);

                done();
            })
        });

        afterEach(async (done) => {
            await mysqlStore.clearAsync();
            await mysqlStore.disconnectAsync();
            done();
        })

    })

    afterEach(async (done) => {
        await mysqlStore.disconnectAsync();
        done();
    })

    afterAll(async (done) => {
        // NOTE: uncomment if we need to terminate the mysql every test
        // for now, it is okay since we are using a non-standard port (13306) and a fixed docker container name
        // not terminating will make the tests faster by around 11 secs
        // await mysqlServer.down();
        done();
    })
});