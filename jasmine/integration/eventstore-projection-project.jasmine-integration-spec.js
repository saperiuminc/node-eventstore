var eventstore = require('../..'),
    uuid = require('uuid').v4;

var es = eventstore({
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
    redisConfig: {
        host: 'redis',
        port: 6379
    }
});

describe('eventstore-projection.jasmine-integration-spec', () => {
    beforeAll((done) => {
        es.init(function(err) {
            done();
        });
    })

    beforeEach((done) => {
        done();
    });

    describe('project', () => {
        it('should be able to receive an event based on a query', (done) => {
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
            es.project({
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

            es.startAllProjections();

            // add an event to the stream
            es.getEventStream({
                aggregateId: dummyId,
                aggregate: query.aggregate,
                context: query.context
            }, function(err, stream) {
                stream.addEvent(expectedEvent);
                // stream.commit();
                stream.commit(function(err, stream) {});
            });
        });
    });
});