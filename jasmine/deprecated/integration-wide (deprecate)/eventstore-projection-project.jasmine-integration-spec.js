var eventstore = require('../../..'),
    uuid = require('uuid').v4;

const mysql = require('mysql');
const bluebird = require('bluebird');

const mysqlConnection = mysql.createConnection({
    host: 'dbserver',
    port: 3306,
    user: 'root',
    password: 'root',
    database: 'eventstore',
});



bluebird.promisifyAll(mysqlConnection);

describe('eventstore-projection.jasmine-integration-spec', () => {
    let es;

    beforeEach((done) => {
        es = eventstore({
            pollingMaxRevisions: 5,
            pollingTimeout: 1000,
            eventCallbackTimeout: 10000,
            type: 'mysql',
            host: 'dbserver',
            port: 3306,
            user: 'root',
            password: 'root',
            database: 'eventstore',
            eventsTableName: 'events',
            undispatchedEventsTableName: 'undispatched_events',
            snapshotsTableName: 'snapshots',
            connectionPoolLimit: 1,
            enableProjection: true,
            redisConfig: {
                host: 'redis',
                port: 6379
            },
            listStore: {
                host: 'dbserver',
                port: 3306,
                user: 'root',
                password: 'root',
                database: 'eventstore'
            }, // required
        });
        bluebird.promisifyAll(es);
        es.init(done);
    });

    describe('project', () => {
        it('should be able to receive an event based on a query', async (done) => {
            var projectionId = uuid().toString();
            const dummyId = uuid().toString();
            const dummyContext = uuid().toString();
            const dummyAggregate = uuid().toString();

            var expectedEvent = {
                name: 'DUMMY_CREATED',
                payload: {
                    dummyId: dummyId
                }
            };

            const query = {
                context: dummyContext,
                aggregate: dummyAggregate
            }

            // do a projectioon
            await es.projectAsync({
                projectionId: projectionId,
                playbackInterface: {
                    DUMMY_CREATED: (state, event, funcs, donePlayback) => {
                        // check that we get the event in the playbackFunction
                        expect(event.payload).toEqual(expectedEvent);
                        donePlayback();
                        done();
                    }
                },
                query: query,
                partitionBy: 'instance'
            });

            await es.startAllProjectionsAsync();

            // add an event to the stream
            es.getEventStream({
                aggregateId: dummyId,
                aggregate: query.aggregate,
                context: query.context
            }, function(err, stream) {
                stream.addEvent(expectedEvent);
                // stream.commit();
                stream.commit(function(err, stream) {});
                done();
            });
        });

        it('should be able to create a playback list', async (done) => {
            var projectionId = uuid().toString();
            const dummyId = uuid().toString();
            const dummyContext = uuid().toString();
            const dummyAggregate = uuid().toString();

            var expectedEvent = {
                name: 'DUMMY_CREATED',
                payload: {
                    dummyId: dummyId
                }
            };

            const query = {
                context: dummyContext,
                aggregate: dummyAggregate
            }

            // do a projection
            await es.projectAsync({
                projectionId: projectionId,
                playbackInterface: {
                    DUMMY_CREATED: (state, event, funcs, donePlayback) => {
                        // check that we get the event in the playbackFunction
                        expect(event.payload).toEqual(expectedEvent);
                        donePlayback();
                        done();
                    }
                },
                playbackList: {
                    name: 'dummy_list',
                    fields: [{
                        name: 'dummyId',
                        type: 'string'
                    }],
                    secondaryKeys: {
                        idx_vehicleId: [{
                            name: 'dummyId',
                            sort: 'ASC'
                        }]
                    }
                },
                query: query,
                partitionBy: 'instance'
            });

            await es.startAllProjectionsAsync();

            const results = await mysqlConnection.queryAsync('select count(1) as the_count from dummy_list;', {});
            expect(results[0]['the_count']).toEqual(0);
            done();
        });

        it('should be able to create a playback list', async (done) => {
            var projectionId = uuid().toString();
            const dummyId = uuid().toString();
            const dummyContext = uuid().toString();
            const dummyAggregate = uuid().toString();

            const query = {
                context: dummyContext,
                aggregate: dummyAggregate
            }

            // do a projection
            await es.projectAsync({
                projectionId: projectionId,
                stateList: {
                    name: 'dummy_state_list',
                    fields: [{
                        name: 'dummyId',
                        type: 'string'
                    }],
                    secondaryKeys: {
                        idx_vehicleId: [{
                            name: 'dummyId',
                            sort: 'ASC'
                        }]
                    }
                },
                query: query,
                partitionBy: 'instance'
            });

            await es.startAllProjectionsAsync();

            const results = await mysqlConnection.queryAsync('select count(1) as the_count from dummy_state_list;', {});

            expect(results[0]['the_count']).toEqual(0);
            done();

        });
    });
});