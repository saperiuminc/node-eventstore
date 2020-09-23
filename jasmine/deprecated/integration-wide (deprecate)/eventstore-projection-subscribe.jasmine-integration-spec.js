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
    enableProjection: true,
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

    describe('subscribe', () => {

        it('it should be able to get event stream 0', function(done) {
            const dummyId = uuid().toString();
            const dummyContext = uuid().toString();
            const dummyAggregate = uuid().toString();

            es.getEventStream({
                aggregateId: dummyId,
                aggregate: dummyAggregate,
                context: dummyContext
            }, function(err, stream) {
                var events = stream.events;
                expect(events.length).toEqual(0);
                done();
            });
        });

        it('it should be able to subscribe to events', function(done) {
            const dummyId = uuid().toString();
            const dummyContext = uuid().toString();
            const dummyAggregate = uuid().toString();

            var expectedEvent = {
                event: 'this is my event'
            };
            es.subscribe({
                aggregateId: dummyId,
                aggregate: dummyAggregate,
                context: dummyContext
            }, 0, function(err, event) {
                expect(event.payload).toEqual(expectedEvent);
                done()
            });

            es.getEventStream({
                aggregateId: dummyId,
                aggregate: dummyAggregate,
                context: dummyContext
            }, function(err, stream) {
                stream.addEvent(expectedEvent);
                stream.commit();
            });
        });

        it('it should be able to subscribe, pre commit, to events at a given offset', function(done) {
            const dummyId = uuid().toString();
            const dummyContext = uuid().toString();
            const dummyAggregate = uuid().toString();

            var expectedEvent = {
                event: 'this is my first event'
            };
            es.subscribe({
                aggregateId: dummyId,
                aggregate: dummyAggregate,
                context: dummyContext
            }, 1, function(err, event) {
                console.log('event');
                console.log(event);
                expect(event.payload).toEqual(expectedEvent);
                done()
            });

            es.getEventStream({
                aggregateId: dummyId,
                aggregate: dummyAggregate,
                context: dummyContext
            }, function(err, stream) {
                stream.addEvent(expectedEvent);
                stream.addEvent({
                    event: 'this is my second event'
                });
                stream.commit();
            });
        });

        it('it should be able to subscribe, post commit, to events at a given offset', function(done) {
            const dummyId = uuid().toString();
            const dummyContext = uuid().toString();
            const dummyAggregate = uuid().toString();

            var expectedEvent = {
                event: 'this is my second event'
            };
            es.getEventStream({
                aggregateId: dummyId,
                aggregate: dummyAggregate,
                context: dummyContext
            }, function(err, stream) {
                stream.addEvent({
                    event: 'this is my first event'
                });
                stream.addEvent(expectedEvent);
                // stream.commit();
                stream.commit(function(err, stream) {
                    es.subscribe({
                        aggregateId: dummyId,
                        aggregate: dummyAggregate,
                        context: dummyContext
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