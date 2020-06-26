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

describe('mysql.eventstore.integration.jasmine-spec', () => {
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

    describe('subscribe', () => {

        it('it should be able to get event stream 0', function(done) {
            es.getEventStream({
                aggregateId: 'test1',
                aggregate: 'test',
                context: 'test'
            }, function(err, stream) {
                var events = stream.events;
                expect(events.length).toEqual(0);
                done();
            });
        });

        it('it should be able to subscribe to events', function(done) {
            var expectedEvent = {
                event: 'this is my event'
            };
            es.subscribe({
                aggregateId: 'test2',
                aggregate: 'test',
                context: 'test'
            }, 0, function(err, event) {
                expect(event.payload).toEqual(expectedEvent);
                done()
            });

            es.getEventStream({
                aggregateId: 'test2',
                aggregate: 'test',
                context: 'test'
            }, function(err, stream) {
                stream.addEvent(expectedEvent);
                stream.commit();
            });
        });

        it('it should be able to subscribe, pre commit, to events at a given offset', function(done) {
            var expectedEvent = {
                event: 'this is my first event'
            };
            var id = uuid().toString();
            es.subscribe({
                aggregateId: id,
                aggregate: 'test',
                context: 'test'
            }, 1, function(err, event) {
                console.log('event');
                console.log(event);
                expect(event.payload).toEqual(expectedEvent);
                done()
            });

            es.getEventStream({
                aggregateId: id,
                aggregate: 'test',
                context: 'test'
            }, function(err, stream) {
                stream.addEvent(expectedEvent);
                stream.addEvent({
                    event: 'this is my second event'
                });
                stream.commit();
            });
        });

        it('it should be able to subscribe, post commit, to events at a given offset', function(done) {
            var expectedEvent = {
                event: 'this is my second event'
            };
            var id = uuid().toString();

            es.getEventStream({
                aggregateId: id,
                aggregate: 'test',
                context: 'test'
            }, function(err, stream) {
                stream.addEvent({
                    event: 'this is my first event'
                });
                stream.addEvent(expectedEvent);
                // stream.commit();
                stream.commit(function(err, stream) {
                    es.subscribe({
                        aggregateId: id,
                        aggregate: 'test',
                        context: 'test'
                    }, 1, function(err, event) {
                        console.log('event');
                        console.log(event);
                        expect(event.payload).toEqual(expectedEvent);
                        done()
                    });
                });
            });
        });
    });
});