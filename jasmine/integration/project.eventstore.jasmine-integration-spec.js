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
    connectionPoolLimit: 1                              
});

describe('mysql.eventstore.integration.jasmine-spec', () => {
    beforeAll((done) => {
        es.init(function (err) {
            done();
        });
    })

    it('it should be able to project events', function (done) {
        var projectionId = uuid().toString();
        var projection = {
            projectionId: projectionId,
            query: {
                aggregateId: 'test2',
                aggregate: 'test',          
                context: 'test'      
            },
            partitionBy: ''
        };

        var expectedEvent = {
            name: 'PROJECTION_CREATED',
            payload: {
                partitionBy: projection.partitionBy,
                projectionId: projection.projectionId,
                projectionGroup: 'default',
                query: {
                    aggregateId: 'test2',
                    aggregate: 'test',          
                    context: 'test'     
                }
            }
        };

        es.subscribe({
            aggregateId: `projections:${projectionId}`,
            aggregate: 'projection',
            context: '__projections__'     
        }, 0, function(err, event) {
            console.log('event');
            console.log(event);
            expect(event.payload).toEqual(expectedEvent);
            done()
        });

        es.project(projection, function() {
            console.log("projection done");
        });
    });
});