var eventstore = require('../../');
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
    connectionPoolLimit: 1                              
});

describe('mysql.eventstore.integration.jasmine-spec', () => {
    beforeAll((done) => {
        es.init(function (err) {
            done();
        });
    })

    it('it should be able to get event stream 0', function (done) {
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

    it('it should be able to subscribe to events', function (done) {
        var expectedEvent = { event: 'this is my event' };
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

    it('it should be able to pre subscribe to events at a given offset', function (done) {
        var expectedEvent = { event: 'this is my second event' };

        es.subscribe({
            aggregateId: 'test3',
            aggregate: 'test',          
            context: 'test'      
        }, 1, function(err, event) {
            console.log('event');
            console.log(event);
            expect(event.payload).toEqual(expectedEvent);
            done()
        });

        es.getEventStream({
            aggregateId: 'test3',
            aggregate: 'test',          
            context: 'test'                 
          }, function(err, stream) {
            stream.addEvent({ event: 'this is my first event' });
            stream.addEvent(expectedEvent);
            stream.commit();
          });
    });

    it('it should be able to post subscribe to events at a given offset', function (done) {
        var expectedEvent = { event: 'this is my second event' };
        es.getEventStream({
            aggregateId: 'test4',
            aggregate: 'test',          
            context: 'test'                 
          }, function(err, stream) {
            stream.addEvent({ event: 'this is my first event' });
            stream.addEvent(expectedEvent);
            // stream.commit();
            stream.commit(function(err, stream) {
                es.subscribe({
                    aggregateId: 'test4',
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