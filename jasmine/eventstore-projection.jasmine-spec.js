let EventStoreWithProjection = require('../lib/eventstore-projections/eventstore-projection');
const BlueBird = require('bluebird');
const StreamBuffer = require('../lib/eventStreamBuffer');


fdescribe('eventstore-projection tests', () => {
    // just instantiating for vscode jsdoc intellisense
    let esWithProjection = new EventStoreWithProjection();
    let eventsDataStore;
    let options;
    let defaultStream;
    let getEventStreamResult;
    let distributedLock;
    let jobsManager;
    let playbackListStore;
    let playbackListViewStore;
    let redisSub;
    let redisPub;
    let eventStoreStatelist;
    let eventStorePlaybacklist;
    let eventStorePlaybacklistView;
    let EventStoreStateListFunction;
    let EventStorePlaybackListFunction;
    let EventstorePlaybackListViewFunction;

    beforeEach((done) => {
        const InMemoryStore = require('../lib/databases/inmemory');
        const JobsManager = require('./test-doubles/fakes/jobs-manager.fake');
        const EventstorePlaybackListInMemoryStore = require('./test-doubles/fakes/eventstore-playbacklist-inmemory-store.fake');
        options = {
            pollingMaxRevisions: 10,
            pollingTimeout: 0, // so that polling is immediate
            eventCallbackTimeout: 0,
            projectionGroup: 'test',
            enableProjection: true,
            listStore: {
                host: 'host',
                port: 'port',
                database: 'database',
                user: 'user',
                password: 'password'
            }
        };

        eventsDataStore = new InMemoryStore(options);
        jobsManager = new JobsManager();
        playbackListStore = new EventstorePlaybackListInMemoryStore();
        esWithProjection = new EventStoreWithProjection(options, eventsDataStore, jobsManager, distributedLock, playbackListStore, playbackListViewStore);
        BlueBird.promisifyAll(esWithProjection);

        esWithProjection.initAsync();
        done();
    });

    afterEach((done) => {
        jobsManager.destroy();
        done();
    })

    describe('projections', () => {
        let projection;
        beforeEach(async (done) => {
            // arrange 
            projection = {
                projectionId: 'the_projection_id',
                query: {
                    context: 'vehicle'
                },
                playbackInterface: {
                    vehicle_created: function(state, event, funcs, done) {
                        funcs.getPlaybackList('vehicle_list', function(err, playbackList) {
                            const eventPayload = event.payload.payload;
                            const data = {
                                vehicleId: eventPayload.vehicleId,
                                year: eventPayload.year,
                                make: eventPayload.make,
                                model: eventPayload.model,
                                mileage: eventPayload.mileage
                            };
                            playbackList.add(event.aggregateId, event.streamRevision, data, {}, function(err) {
                                done();
                            })
                        });
                    },
                    vehicle_updated: function(state, event, funcs, done) {
                        funcs.getPlaybackList('vehicle_list', function(err, playbackList) {
                            const eventPayload = event.payload.payload;
                            const data = {
                                vehicleId: eventPayload.vehicleId,
                                year: eventPayload.year,
                                make: eventPayload.make,
                                model: eventPayload.model,
                                mileage: eventPayload.mileage
                            };
                            playbackList.add(event.aggregateId, event.streamRevision, data, {}, function(err) {
                                done();
                            })
                        });
                    }
                },
                playbackList: {
                    name: 'vehicle_list',
                    fields: [{
                        name: 'vehicleId',
                        type: 'string'
                    }],
                    secondaryKeys: {
                        idx_vehicleId: [{
                            name: 'vehicleId',
                            sort: 'ASC'
                        }]
                    }
                },
            };
            await esWithProjection.projectAsync(projection);
            await esWithProjection.startAllProjections();

            done();
        })

        describe('playback lists', () => {
            it('should add to the store when an item is added to the playbacklist', async (done) => {
                // arrange 
                // act
                const stream = await esWithProjection.getEventStreamAsync({
                    aggregateId: 'vehicle_1',
                    aggregate: 'vehicle', // optional
                    context: 'vehicle' // optional
                });

                stream.addEvent({
                    name: 'vehicle_created',
                    payload: {
                        vehicleId: 'vehicle_1',
                        year: 2012,
                        make: 'Honda',
                        model: 'Jazz',
                        mileage: 12345
                    }
                });
                stream.commit();

                // assert
                const data = await playbackListStore.pollingGet(projection.playbackList.name, 'vehicle_1', 1000);
                expect(data).toEqual({
                    rowId: 'vehicle_1',
                    revision: 0,
                    data: {
                        vehicleId: 'vehicle_1',
                        year: 2012,
                        make: 'Honda',
                        model: 'Jazz',
                        mileage: 12345
                    },
                    meta: {}
                })
                done();
            });
        })

        afterEach(async (done) => {
            await playbackListStore.reset();
            done();
        })
    });

    describe('subscribe', () => {

    });
})