let EventStoreWithProjection = require('../../../lib/eventstore-projections/eventstore-projection');
const mockery = require('mockery');
mockery.enable();
const StreamBuffer = require('../../../lib/eventStreamBuffer');


xdescribe('eventstore-projection tests', () => {
    // just instantiating for vscode jsdoc intellisense
    let esWithProjection = new EventStoreWithProjection();
    let options;
    let defaultStream;
    let getEventStreamResult;
    let distributedLock;
    let jobsManager;
    let redisSub;
    let redisPub;
    let eventStoreStatelist;
    let eventStorePlaybacklist;
    let eventStorePlaybacklistView;
    let EventStoreStateListFunction;
    let EventStorePlaybackListFunction;
    let EventstorePlaybackListViewFunction;

    beforeEach(() => {
        EventStorePlaybackListFunction = jasmine.createSpy('EventStorePlaybackListFunction');
        eventStorePlaybacklist = jasmine.createSpyObj('eventStorePlaybacklist', ['init']);
        eventStorePlaybacklist.init.and.returnValue(Promise.resolve());
        EventStorePlaybackListFunction.and.returnValue(eventStorePlaybacklist);
        mockery.registerMock('./eventstore-playback-list', EventStorePlaybackListFunction);

        EventStoreStateListFunction = jasmine.createSpy('EventStoreStateListFunction');
        eventStoreStatelist = jasmine.createSpyObj('eventStoreStatelist', ['init']);
        eventStoreStatelist.init.and.returnValue(Promise.resolve());
        EventStoreStateListFunction.and.returnValue(eventStoreStatelist);
        mockery.registerMock('./eventstore-state-list', EventStoreStateListFunction);

        distributedLock = jasmine.createSpyObj('distributedLock', ['lock', 'unlock']);
        distributedLock.lock.and.returnValue(Promise.resolve());
        distributedLock.unlock.and.returnValue(Promise.resolve());

        jobsManager = jasmine.createSpyObj('distributedLock', ['queueJob', 'processJobGroup']);
        jobsManager.queueJob.and.returnValue(Promise.resolve());
        jobsManager.processJobGroup.and.returnValue(Promise.resolve());

        EventstorePlaybackListViewFunction = jasmine.createSpy('EventstorePlaybackListViewFunction');
        eventStorePlaybacklistView = jasmine.createSpyObj('eventStorePlaybacklistView', ['init']);
        eventStorePlaybacklistView.init.and.returnValue(Promise.resolve());
        EventstorePlaybackListViewFunction.and.returnValue(eventStorePlaybacklistView);
        mockery.registerMock('./eventstore-playback-list-view', EventstorePlaybackListViewFunction);

        options = {
            pollingMaxRevisions: 10,
            pollingTimeout: 0, // so that polling is immediate
            eventCallbackTimeout: 0,
            projectionGroup: 'test',
            distributedLock: distributedLock,
            jobsManager: jobsManager,
            EventstorePlaybackList: EventStorePlaybackListFunction,
            EventstorePlaybackListView: EventstorePlaybackListViewFunction,
            enableProjection: true,
            listStore: {
                host: 'host',
                port: 'port',
                database: 'database',
                user: 'user',
                password: 'password'
            }
        };
        esWithProjection = new EventStoreWithProjection(options);

        esWithProjection.getLastEvent = jasmine.createSpy('getLastEvent', esWithProjection.getLastEvent);
        esWithProjection.getLastEvent.and.callFake((query, cb) => {
            cb();
        });

        defaultStream = jasmine.createSpyObj('default_stream', ['addEvent', 'commit']);
        defaultStream.events = [];
        defaultStream.commit.and.callFake((cb) => {
            cb();
        })

        getEventStreamResult = jasmine.createSpyObj('getEventStreamResult', ['addEvent', 'commit']);
        getEventStreamResult.events = [];
        getEventStreamResult.commit.and.callFake((cb) => {
            cb();
        })

        esWithProjection.getEventStream = jasmine.createSpy('getEventStream', esWithProjection.getEventStream);
        esWithProjection.getEventStream.and.callFake((query, revMin, revMax, cb) => {
            // console.log('common getEventStream');
            // by default we only poll/loop one time for the event stream
            esWithProjection.deactivatePolling();

            // set the query to the stream
            getEventStreamResult.aggregate = query.aggregate;
            getEventStreamResult.aggregateId = query.aggregateId;
            getEventStreamResult.context = query.context;
            cb(null, getEventStreamResult);
        });

        esWithProjection.getLastEventAsStream = jasmine.createSpy('getLastEventAsStream', esWithProjection.getLastEventAsStream);
        esWithProjection.getLastEventAsStream.and.callFake((query, cb) => {
            // console.log('common getLastEventAsStream');
            cb(null, defaultStream);
        });
    });

    xdescribe('setupNotifyPubSub', () => {
        it('should setup notify publish', () => {
            redisSub = jasmine.createSpyObj('redisSub', ['on', 'subscribe']);
            redisSub.on.and.callFake((event, cb) => {
                if (event === 'ready') {
                    cb();
                } else {
                    const job = {
                        query: {
                            aggregateId: 'aggregateId',
                            aggregate: 'aggregate',
                            context: 'context'
                        }
                    }
                    cb('NOTIFY_COMMIT_REDIS_CHANNEL', JSON.stringify(job))
                }
            });
            redisPub = jasmine.createSpyObj('redisPub', ['on', 'publish']);

            esWithProjection.redisSub = redisSub;
            esWithProjection.redisPub = redisPub;
            esWithProjection.setupNotifyPublish();
        });

        it('should setup notify subscribe', () => {
            redisSub = jasmine.createSpyObj('redisSub', ['on', 'subscribe']);
            redisSub.on.and.callFake((event, cb) => {
                if (event === 'ready') {
                    cb();
                } else {
                    const job = {
                        query: {
                            aggregateId: 'aggregateId',
                            aggregate: 'aggregate',
                            context: 'context'
                        }
                    }
                    cb('NOTIFY_COMMIT_REDIS_CHANNEL', JSON.stringify(job))
                }
            });
            redisPub = jasmine.createSpyObj('redisPub', ['on', 'publish']);

            esWithProjection.redisSub = redisSub;
            esWithProjection.redisPub = redisPub;
            esWithProjection._setupNotifySubscribe();
        });
    });

    describe('project', () => {
        xdescribe('validating params and output', () => {
            it('should validate the required param projection', (done) => {
                esWithProjection.project(null, function(error) {
                    expect(error.message).toEqual('projection is required');
                    done();
                });
            });

            it('should validate the required param prorectionId', (done) => {
                esWithProjection.project({}, function(error) {
                    expect(error.message).toEqual('projectionId is required');
                    done();
                });
            });

            it('should validate the required param query', (done) => {
                esWithProjection.project({
                    projectionId: 'the_projection_id'
                }, function(error) {
                    expect(error.message).toEqual('query is required');
                    done();
                });
            });

            it('should validate that query should have at least context, aggregate or aggregateId/streamId', (done) => {
                esWithProjection.project({
                    projectionId: 'the_projection_id',
                    query: {}
                }, function(error) {
                    expect(error.message).toEqual('at least an aggregate, context or aggregateId/streamId is required');
                    done();
                });
            });

            it('should pass when only context is passed', (done) => {
                esWithProjection.project({
                    projectionId: 'the_projection_id',
                    query: {
                        context: 'the_context'
                    }
                }, function(error) {
                    expect(error).toBeUndefined();
                    done();
                });
            });

            it('should pass when only aggregate is passed', (done) => {
                esWithProjection.project({
                    projectionId: 'the_projection_id',
                    query: {
                        aggregate: 'aggregate'
                    }
                }, function(error) {
                    expect(error).toBeUndefined();
                    done();
                });
            });

            it('should pass when only aggregateId is passed', (done) => {
                esWithProjection.project({
                    projectionId: 'the_projection_id',
                    query: {
                        aggregateId: 'aggregate_id'
                    }
                }, function(error) {
                    expect(error).toBeUndefined();
                    done();
                });
            });

            it('should pass when only streamId is passed', (done) => {
                esWithProjection.project({
                    projectionId: 'the_projection_id',
                    query: {
                        streamId: 'stream_id'
                    }
                }, function(error) {
                    expect(error).toBeUndefined();
                    done();
                });
            });

            it('should pass when only context and aggregate are passed', (done) => {
                esWithProjection.project({
                    projectionId: 'the_projection_id',
                    query: {
                        context: 'context',
                        aggregate: 'aggregate',
                    }
                }, function(error) {
                    expect(error).toBeUndefined();
                    done();
                });
            });

            it('should return void', (done) => {
                const res = esWithProjection.project({
                    projectionId: 'the_projection_id',
                    query: {
                        streamId: 'abc'
                    }
                });

                expect(res).toBeUndefined();
                done();
            });
        })

        xdescribe('adding the projection to the projection stream storage', () => {
            it('should call Eventstore.getLastEventAsStream to get the latest stream storage of the projection', (done) => {
                const query = {
                    context: 'the_context'
                };

                const projectionId = 'the_projection_id';

                var queryProjection = {
                    aggregateId: `projections:${projectionId}`,
                    aggregate: 'projection',
                    context: '__projections__'
                };
                esWithProjection.project({
                    projectionId: projectionId,
                    query: query
                }, function(error) {
                    expect(error).toBeUndefined();
                    expect(esWithProjection.getLastEventAsStream).toHaveBeenCalledWith(queryProjection, jasmine.any(Function));
                    done();
                });

            });

            it('should call stream.addEvent and commit if there are still no events for the projection stream', (done) => {
                const projection = {
                    projectionId: 'the_projection_id',
                    query: {
                        context: 'the_context'
                    }
                };

                const event = {
                    name: 'PROJECTION_CREATED',
                    payload: {
                        projectionId: projection.projectionId,
                        query: projection.query,
                        partitionBy: projection.partitionBy,
                        projectionGroup: options.projectionGroup,
                        meta: projection.meta
                    }
                };

                var job = {
                    name: `projection-group:${options.projectionGroup}:projection:${projection.projectionId}`,
                    payload: event.eventPayload
                };

                esWithProjection.project(projection, function(error) {
                    expect(error).toBeUndefined();
                    expect(defaultStream.addEvent).toHaveBeenCalledWith(event);
                    expect(defaultStream.commit).toHaveBeenCalledTimes(1);
                    done();
                });
            });

            it('should receive an error when Eventstore.getLastEventAsStream has an error', (done) => {
                const projection = {
                    projectionId: 'the_projection_id',
                    query: {
                        context: 'the_context'
                    }
                };

                const expectedError = new Error('getLastEventAsStream error');
                esWithProjection.getLastEventAsStream.and.callFake((query, cb) => {
                    cb(expectedError);
                });

                esWithProjection.project(projection, function(error) {
                    expect(error).toEqual(expectedError);
                    done();
                });
            });

            it('should receive an error when stream.addEvent has an error', (done) => {
                const projection = {
                    projectionId: 'the_projection_id',
                    query: {
                        context: 'the_context'
                    }
                };

                const expectedError = new Error('addEvent error');
                defaultStream.addEvent.and.callFake((event) => {
                    throw expectedError;
                });

                esWithProjection.project(projection, function(error) {
                    expect(error).toEqual(expectedError);
                    done();
                });
            });

            it('should receive an error when stream.commit has an error', (done) => {
                const projection = {
                    projectionId: 'the_projection_id',
                    query: {
                        context: 'the_context'
                    }
                };

                const expectedError = new Error('commit error');
                defaultStream.commit.and.callFake((cb) => {
                    cb(expectedError);
                });

                esWithProjection.project(projection, function(error) {
                    expect(error).toEqual(expectedError);
                    done();
                });
            });
        })

        xdescribe('ensuring that only one projection event is created if multiple instances are created', () => {
            it('should call lock of the distributedLock', (done) => {
                const query = {
                    context: 'the_context'
                };

                const projectionId = 'the_projection_id';

                var queryProjection = {
                    aggregateId: `projections:${projectionId}`,
                    aggregate: 'projection',
                    context: '__projections__'
                };

                const lockKey = `projection-group:${options.projectionGroup}:projection:${projectionId}`;
                esWithProjection.project({
                    projectionId: projectionId,
                    query: query
                }, function(error) {
                    expect(error).toBeUndefined();
                    expect(esWithProjection.options.distributedLock.lock).toHaveBeenCalledWith(lockKey);
                    done();
                });
            })

            it('should call unlock of the distributedLock', (done) => {
                const lockToken = 'the_lock_token';
                distributedLock.lock.and.returnValue(Promise.resolve(lockToken));
                const query = {
                    context: 'the_context'
                };

                const projectionId = 'the_projection_id';

                var queryProjection = {
                    aggregateId: `projections:${projectionId}`,
                    aggregate: 'projection',
                    context: '__projections__'
                };

                esWithProjection.project({
                    projectionId: projectionId,
                    query: query
                }, function(error) {
                    expect(error).toBeUndefined();
                    expect(esWithProjection.options.distributedLock.unlock).toHaveBeenCalledWith(lockToken);
                    done();
                });
            })

            it('should not have an error if distributedLock is not passed as an option', (done) => {
                const query = {
                    context: 'the_context'
                };

                const projectionId = 'the_projection_id';

                var queryProjection = {
                    aggregateId: `projections:${projectionId}`,
                    aggregate: 'projection',
                    context: '__projections__'
                };

                esWithProjection.options.distributedLock = undefined;

                const lockKey = `projection-group:${options.projectionGroup}:projection:${projectionId}`;
                esWithProjection.project({
                    projectionId: projectionId,
                    query: query
                }, function(error) {
                    expect(error).toBeUndefined();
                    done();
                });
            })
        })

        describe('queue a job for the projection', () => {
            xit('should call jobsManager.queueJob if jobsManager is passed as an option', (done) => {
                const query = {
                    context: 'the_context'
                };

                const projectionId = 'the_projection_id';

                const projection = {
                    projectionId: projectionId,
                    query: query
                };

                const projectionKey = `projection-group:${options.projectionGroup}:projection:${projectionId}`;

                const jobParams = {
                    id: projectionKey,
                    group: `projection-group:${options.projectionGroup}`,
                    payload: {
                        projection: projection
                    }
                };

                const jobOptions = {
                    delay: undefined
                }

                esWithProjection.project(projection, function(error) {
                    expect(error).toBeUndefined();
                    expect(esWithProjection.options.jobsManager.queueJob).toHaveBeenCalledWith(jobParams, jobOptions);
                    done();
                });
            });

            it('should not have an error if jobsManager is not defined', (done) => {
                const query = {
                    context: 'the_context'
                };

                const projection = {
                    projectionId: 'the_projection_id',
                    query: query
                };

                const projectionKey = `projection-group:${options.projectionGroup}:projection:${projection.projectionId}`;

                esWithProjection.options.jobsManager = undefined;

                esWithProjection.project(projection, function(error) {
                    expect(error).toBeUndefined();
                    done();
                });
            });
        });

        describe('process projection job group', () => {
            it('should call jobsManager.queueJob if jobsManager is passed as an option', (done) => {
                const query = {
                    context: 'the_context'
                };

                const projectionId = 'the_projection_id';

                const projection = {
                    projectionId: projectionId,
                    query: query
                };

                const projectionKey = `projection-group:${options.projectionGroup}:projection:${projectionId}`;

                const jobParams = {
                    id: projectionKey,
                    group: `projection-group:${options.projectionGroup}`,
                    payload: {
                        projection: projection
                    }
                };

                const jobOptions = {
                    delay: undefined
                };

                esWithProjection.project(projection, function(error) {
                    expect(error).toBeUndefined();
                    expect(esWithProjection.options.jobsManager.queueJob).toHaveBeenCalledWith(jobParams, jobOptions);
                    done();
                });
            });

            it('should not have an error if jobsManager is not defined', (done) => {
                const query = {
                    context: 'the_context'
                };

                const projection = {
                    projectionId: 'the_projection_id',
                    query: query
                };

                const projectionKey = `projection-group:${options.projectionGroup}:projection:${projection.projectionId}`;

                esWithProjection.options.jobsManager = undefined;

                esWithProjection.project(projection, function(error) {
                    expect(error).toBeUndefined();
                    done();
                });
            });
        });

        describe('creating playback lists', () => {
            describe('should validate some required options', () => {
                it('should validate listStore', (done) => {
                    const query = {
                        context: 'the_context'
                    };

                    const projectionId = 'the_projection_id';

                    const projection = {
                        projectionId: projectionId,
                        query: query,
                        playbackList: {}
                    };

                    // NOTE: just removing the option to test
                    esWithProjection.options.listStore = null;

                    esWithProjection.project(projection, function(error) {
                        expect(error.message).toEqual('listStore must be provided in the options');
                        done();
                    });
                });

                it('should validate listStore.host', (done) => {
                    const query = {
                        context: 'the_context'
                    };

                    const projectionId = 'the_projection_id';

                    const projection = {
                        projectionId: projectionId,
                        query: query,
                        playbackList: {}
                    };

                    // NOTE: just removing the option to test
                    esWithProjection.options.listStore = {
                        port: 'port',
                        database: 'database',
                        user: 'user',
                        password: 'password'
                    };

                    esWithProjection.project(projection, function(error) {
                        expect(error.message).toEqual('listStore.host must be provided in the options');
                        done();
                    });
                });

                it('should validate listStore.port', (done) => {
                    const query = {
                        context: 'the_context'
                    };

                    const projectionId = 'the_projection_id';

                    const projection = {
                        projectionId: projectionId,
                        query: query,
                        playbackList: {}
                    };

                    // NOTE: just removing the option to test
                    esWithProjection.options.listStore = {
                        host: 'host',
                        database: 'database',
                        user: 'user',
                        password: 'password'
                    };

                    esWithProjection.project(projection, function(error) {
                        expect(error.message).toEqual('listStore.port must be provided in the options');
                        done();
                    });
                });

                it('should validate listStore.database', (done) => {
                    const query = {
                        context: 'the_context'
                    };

                    const projectionId = 'the_projection_id';

                    const projection = {
                        projectionId: projectionId,
                        query: query,
                        playbackList: {}
                    };

                    // NOTE: just removing the option to test
                    esWithProjection.options.listStore = {
                        host: 'host',
                        port: 'port',
                        user: 'user',
                        password: 'password'
                    };

                    esWithProjection.project(projection, function(error) {
                        expect(error.message).toEqual('listStore.database must be provided in the options');
                        done();
                    });
                });

                it('should validate listStore.user', (done) => {
                    const query = {
                        context: 'the_context'
                    };

                    const projectionId = 'the_projection_id';

                    const projection = {
                        projectionId: projectionId,
                        query: query,
                        playbackList: {}
                    };

                    // NOTE: just removing the option to test
                    esWithProjection.options.listStore = {
                        host: 'host',
                        port: 'port',
                        database: 'database',
                        password: 'password'
                    };

                    esWithProjection.project(projection, function(error) {
                        expect(error.message).toEqual('listStore.user must be provided in the options');
                        done();
                    });
                });

                it('should validate listStore.password', (done) => {
                    const query = {
                        context: 'the_context'
                    };

                    const projectionId = 'the_projection_id';

                    const projection = {
                        projectionId: projectionId,
                        query: query,
                        playbackList: {}
                    };

                    // NOTE: just removing the option to test
                    esWithProjection.options.listStore = {
                        host: 'host',
                        port: 'port',
                        database: 'database',
                        user: 'user',
                    };

                    esWithProjection.project(projection, function(error) {
                        expect(error.message).toEqual('listStore.password must be provided in the options');
                        done();
                    });
                });
            });

            it('should create and init the playback list correctly', (done) => {
                const query = {
                    context: 'the_context'
                };

                const projectionId = 'the_projection_id';

                const projection = {
                    projectionId: projectionId,
                    query: query,
                    playbackList: {
                        name: '',
                        fields: [{
                            name: 'field_name',
                            type: 'string'
                        }],
                        secondaryKeys: {
                            idx_field_name: [{
                                name: 'field_name',
                                sort: 'ASC'
                            }]
                        }
                    }
                };

                esWithProjection.project(projection, function(error) {
                    expect(error).toBeUndefined();
                    expect(eventStorePlaybacklist.init).toHaveBeenCalled();
                    expect(EventStorePlaybackListFunction).toHaveBeenCalledWith({
                        host: esWithProjection.options.listStore.host,
                        port: esWithProjection.options.listStore.port,
                        database: esWithProjection.options.listStore.database,
                        user: esWithProjection.options.listStore.user,
                        password: esWithProjection.options.listStore.password,
                        listName: projection.playbackList.name,
                        fields: projection.playbackList.fields,
                        secondaryKeys: projection.playbackList.secondaryKeys
                    });
                    done();
                });
            });
        })

        describe('creating state lists', () => {
            describe('should validate some required options', () => {
                it('should validate listStore', (done) => {
                    const query = {
                        context: 'the_context'
                    };

                    const projectionId = 'the_projection_id';

                    const projection = {
                        projectionId: projectionId,
                        query: query,
                        stateList: {}
                    };

                    // NOTE: just removing the option to test
                    esWithProjection.options.listStore = null;

                    esWithProjection.project(projection, function(error) {
                        expect(error.message).toEqual('listStore must be provided in the options');
                        done();
                    });
                });

                it('should validate listStore.host', (done) => {
                    const query = {
                        context: 'the_context'
                    };

                    const projectionId = 'the_projection_id';

                    const projection = {
                        projectionId: projectionId,
                        query: query,
                        stateList: {}
                    };

                    // NOTE: just removing the option to test
                    esWithProjection.options.listStore = {
                        port: 'port',
                        database: 'database',
                        user: 'user',
                        password: 'password'
                    };

                    esWithProjection.project(projection, function(error) {
                        expect(error.message).toEqual('listStore.host must be provided in the options');
                        done();
                    });
                });

                it('should validate listStore.port', (done) => {
                    const query = {
                        context: 'the_context'
                    };

                    const projectionId = 'the_projection_id';

                    const projection = {
                        projectionId: projectionId,
                        query: query,
                        stateList: {}
                    };

                    // NOTE: just removing the option to test
                    esWithProjection.options.listStore = {
                        host: 'host',
                        database: 'database',
                        user: 'user',
                        password: 'password'
                    };

                    esWithProjection.project(projection, function(error) {
                        expect(error.message).toEqual('listStore.port must be provided in the options');
                        done();
                    });
                });

                it('should validate listStore.database', (done) => {
                    const query = {
                        context: 'the_context'
                    };

                    const projectionId = 'the_projection_id';

                    const projection = {
                        projectionId: projectionId,
                        query: query,
                        stateList: {}
                    };

                    // NOTE: just removing the option to test
                    esWithProjection.options.listStore = {
                        host: 'host',
                        port: 'port',
                        user: 'user',
                        password: 'password'
                    };

                    esWithProjection.project(projection, function(error) {
                        expect(error.message).toEqual('listStore.database must be provided in the options');
                        done();
                    });
                });

                it('should validate listStore.user', (done) => {
                    const query = {
                        context: 'the_context'
                    };

                    const projectionId = 'the_projection_id';

                    const projection = {
                        projectionId: projectionId,
                        query: query,
                        stateList: {}
                    };

                    // NOTE: just removing the option to test
                    esWithProjection.options.listStore = {
                        host: 'host',
                        port: 'port',
                        database: 'database',
                        password: 'password'
                    };

                    esWithProjection.project(projection, function(error) {
                        expect(error.message).toEqual('listStore.user must be provided in the options');
                        done();
                    });
                });

                it('should validate listStore.password', (done) => {
                    const query = {
                        context: 'the_context'
                    };

                    const projectionId = 'the_projection_id';

                    const projection = {
                        projectionId: projectionId,
                        query: query,
                        stateList: {}
                    };

                    // NOTE: just removing the option to test
                    esWithProjection.options.listStore = {
                        host: 'host',
                        port: 'port',
                        database: 'database',
                        user: 'user',
                    };

                    esWithProjection.project(projection, function(error) {
                        expect(error.message).toEqual('listStore.password must be provided in the options');
                        done();
                    });
                });
            });

            it('should create and init the stateList list correctly', (done) => {
                const query = {
                    context: 'the_context'
                };

                const projectionId = 'the_projection_id';

                const projection = {
                    projectionId: projectionId,
                    query: query,
                    stateList: {
                        name: 'state_list_name',
                        fields: [{
                            name: 'vehicleId',
                            type: 'string'
                        }],
                        secondaryKeys: {
                            idx_vehicleId: [{
                                name: 'vehicleId',
                                direction: 'asc'
                            }]
                        }
                    }
                };

                esWithProjection.project(projection, function(error) {
                    expect(error).toBeUndefined();
                    expect(eventStoreStatelist.init).toHaveBeenCalled();
                    expect(EventStoreStateListFunction).toHaveBeenCalledWith({
                        host: esWithProjection.options.listStore.host,
                        port: esWithProjection.options.listStore.port,
                        database: esWithProjection.options.listStore.database,
                        user: esWithProjection.options.listStore.user,
                        password: esWithProjection.options.listStore.password,
                        listName: projection.stateList.name,
                        fields: projection.stateList.fields,
                        secondaryKeys: projection.stateList.secondaryKeys
                    });
                    done();
                });
            });
        })
    });

    describe('startAllProjections', () => {
        describe('validating params and output', () => {
            it('should not have an error if callback is undefined', (done) => {
                const result = esWithProjection.startAllProjections();
                expect(result).toBeUndefined();
                done();
            })

            it('should have call the callback if no errors are found', (done) => {
                const result = esWithProjection.startAllProjections((error, result) => {
                    expect(error).toBeUndefined();
                    expect(result).toBeUndefined();
                    done();
                });
            })

            it('should have no errors if callback is null or undefined and jobs manager is undefined', (done) => {
                const result = esWithProjection.startAllProjections();
                done();
            })
        });

        describe('processing jobs from jobs manager', () => {
            beforeEach(() => {
                spyOn(esWithProjection, '_setupNotifySubscribe').and.callFake(async () => {
                    return Promise.resolve({});
                });
            });

            it('should call processJobGroup of jobsmanager', (done) => {
                jobsManager.processJobGroup.and.callFake(() => {
                    expect(jobsManager.processJobGroup).toHaveBeenCalled();
                    return Promise.resolve({});
                });
                esWithProjection.startAllProjections(() => {
                    done();
                });
            })

            it('should call the callback with error if jobsmanager throws an error', (done) => {
                const expectedError = new Error('error in jobsmanager.processJobGroup');
                jobsManager.processJobGroup.and.callFake(() => {
                    return Promise.reject(expectedError);
                });
                const result = esWithProjection.startAllProjections((error) => {
                    expect(error).toEqual(expectedError);
                    done();
                });
            })

            it('should call the playback interface when an event is processed', (done) => {
                const expectedEventstoreEvent = {
                    id: 'some_es_id',
                    payload: {
                        name: 'aggregate_added',
                        payload: {
                            someField: 'field1'
                        }
                    }
                }

                const projection = {
                    query: {
                        aggregate: 'aggregate',
                        context: 'context'
                    },
                    projectionId: 'projectionId',
                    playbackInterface: {
                        aggregate_added: function(state, event, funcs, playbackDone) {
                            expect(event.payload).toEqual(expectedEventstoreEvent.payload);
                            playbackDone();
                            done();
                        }
                    }
                };

                jobsManager.processJobGroup.and.callFake((owner, jobGroup, onProcessJob, onProcessCompletedJob) => {
                    onProcessJob.call(owner, 'jobId', {
                        projection: projection
                    }, {}, (error, result) => {});
                    return Promise.resolve();
                });

                esWithProjection.getEvents = jasmine.createSpy('getEvents', esWithProjection.getEvents);
                let firstLoop = 0;
                esWithProjection.getEvents.and.callFake((query, offset, limit, cb) => {
                    if (firstLoop == 0) {
                        firstLoop += 1;
                        cb(null, [expectedEventstoreEvent]);
                    }
                    cb(null, []);
                });

                esWithProjection.project(projection);
                const result = esWithProjection.startAllProjections();
            })

            it('should still continue with the playback even if an event got an error', (done) => {
                const eventstoreEvents = [{
                    id: 'some_es_id',
                    payload: {
                        name: 'aggregate_added',
                        payload: {
                            someField: 'field1'
                        }
                    }
                }, {
                    id: 'some_es_id',
                    payload: {
                        name: 'aggregate_updated',
                        payload: {
                            someField: 'field2'
                        }
                    }
                }]

                const expectedError = new Error('error in playing back aggregate_added event');
                const projection = {
                    query: {
                        aggregate: 'aggregate',
                        context: 'context'
                    },
                    projectionId: 'projectionId',
                    playbackInterface: {
                        aggregate_added: function(state, event, funcs, playbackDone) {
                            throw expectedError;
                        },
                        aggregate_updated: function(state, event, funcs, playbackDone) {
                            const expectedEventstoreEvent = eventstoreEvents[1];
                            expect(event.payload).toEqual(expectedEventstoreEvent.payload);
                            playbackDone();
                            done();
                        }
                    }
                };

                jobsManager.processJobGroup.and.callFake((owner, jobGroup, onProcessJob, onProcessCompletedJob) => {
                    onProcessJob.call(owner, 'jobId', {
                        projection: projection
                    }, {}, (error, result) => {});
                    return Promise.resolve();
                });

                esWithProjection.getEvents = jasmine.createSpy('getEvents', esWithProjection.getEvents);
                let firstLoop = 0;
                esWithProjection.getEvents.and.callFake((query, offset, limit, cb) => {
                    if (firstLoop == 0) {
                        firstLoop += 1;
                        cb(null, eventstoreEvents);
                    }
                    cb(null, []);
                });

                esWithProjection.project(projection);

                const result = esWithProjection.startAllProjections();
            })

            it('should still continue with the playback even if the playback interface explicitly sends out an error', (done) => {
                const eventstoreEvents = [{
                    id: 'some_es_id',
                    payload: {
                        name: 'aggregate_added',
                        payload: {
                            someField: 'field1'
                        }
                    }
                }, {
                    id: 'some_es_id',
                    payload: {
                        name: 'aggregate_updated',
                        payload: {
                            someField: 'field2'
                        }
                    }
                }]

                const expectedError = new Error('error in playing back aggregate_added event');
                const projection = {
                    query: {
                        aggregate: 'aggregate',
                        context: 'context'
                    },
                    projectionId: 'projectionId',
                    playbackInterface: {
                        aggregate_added: function(state, event, funcs, playbackDone) {
                            playbackDone(expectedError);
                        },
                        aggregate_updated: function(state, event, funcs, playbackDone) {
                            const expectedEventstoreEvent = eventstoreEvents[1];
                            expect(event.payload).toEqual(expectedEventstoreEvent.payload);
                            playbackDone();
                            done();
                        }
                    }
                };

                jobsManager.processJobGroup.and.callFake((owner, jobGroup, onProcessJob, onProcessCompletedJob) => {
                    onProcessJob.call(owner, 'jobId', {
                        projection: projection
                    }, {}, (error, result) => {});
                    return Promise.resolve();
                });

                esWithProjection.getEvents = jasmine.createSpy('getEvents', esWithProjection.getEvents);
                let firstLoop = 0;
                esWithProjection.getEvents.and.callFake((query, offset, limit, cb) => {
                    if (firstLoop == 0) {
                        firstLoop += 1;
                        cb(null, eventstoreEvents);
                    }
                    cb(null, []);
                });

                esWithProjection.project(projection);

                const result = esWithProjection.startAllProjections();
            })

            it('should still continue with the playback even if the playback interface timedout', (done) => {
                const eventstoreEvents = [{
                    id: 'some_es_id',
                    payload: {
                        name: 'aggregate_added',
                        payload: {
                            someField: 'field1'
                        }
                    }
                }, {
                    id: 'some_es_id',
                    payload: {
                        name: 'aggregate_updated',
                        payload: {
                            someField: 'field2'
                        }
                    }
                }]

                const expectedError = new Error('error in playing back aggregate_added event');
                const projection = {
                    query: {
                        aggregate: 'aggregate',
                        context: 'context'
                    },
                    projectionId: 'projectionId',
                    playbackInterface: {
                        aggregate_added: function(state, event, funcs, playbackDone) {
                            // let this time out
                            // throw expectedError;
                        },
                        aggregate_updated: function(state, event, funcs, playbackDone) {
                            const expectedEventstoreEvent = eventstoreEvents[1];
                            expect(event.payload).toEqual(expectedEventstoreEvent.payload);
                            playbackDone();
                            done();
                        },
                        $init: function() {

                        }
                    }
                };

                jobsManager.processJobGroup.and.callFake((owner, jobGroup, onProcessJob, onProcessCompletedJob) => {
                    onProcessJob.call(owner, 'jobId', {
                        projection: projection
                    }, {}, (error, result) => {});
                    return Promise.resolve();
                });

                esWithProjection.getEvents = jasmine.createSpy('getEvents', esWithProjection.getEvents);
                let firstLoop = 0;
                esWithProjection.getEvents.and.callFake((query, offset, limit, cb) => {
                    if (firstLoop == 0) {
                        firstLoop += 1;
                        cb(null, eventstoreEvents);
                    }
                    cb(null, []);
                });

                esWithProjection.project(projection);

                const result = esWithProjection.startAllProjections();
            })

            it('should save the event to a projection-errors stream if playback throws an error', (done) => {
                const eventstoreEvents = [{
                    id: 'some_es_id',
                    payload: {
                        name: 'aggregate_added',
                        payload: {
                            someField: 'field1'
                        }
                    }
                }]

                const projection = {
                    query: {
                        aggregate: 'aggregate',
                        context: 'context'
                    },
                    projectionId: 'projectionId',
                    playbackInterface: {
                        aggregate_added: function(state, event, funcs, playbackDone) {
                            throw new Error('error in playing back aggregate_added event');;
                        }
                    }
                };

                let addEventForErrorStreamCalled = false;
                getEventStreamResult.addEvent.and.callFake((event) => {
                    if (getEventStreamResult.context == 'projection-errors' &&
                        getEventStreamResult.aggregate == 'projectionId' &&
                        getEventStreamResult.aggregateId == 'projectionId-errors') {
                        addEventForErrorStreamCalled = true;
                    }
                });

                getEventStreamResult.commit.and.callFake((cb) => {
                    if (addEventForErrorStreamCalled) {
                        done();
                    }
                });

                jobsManager.processJobGroup.and.callFake((owner, jobGroup, onProcessJob, onProcessCompletedJob) => {
                    onProcessJob.call(owner, 'jobId', {
                        projection: projection
                    }, {}, (error, result) => {});
                    return Promise.resolve();
                });

                esWithProjection.getEvents = jasmine.createSpy('getEvents', esWithProjection.getEvents);
                let firstLoop = 0;
                esWithProjection.getEvents.and.callFake((query, offset, limit, cb) => {
                    if (firstLoop == 0) {
                        firstLoop += 1;
                        cb(null, eventstoreEvents);
                    }
                    cb(null, []);
                });

                esWithProjection.project(projection);

                const result = esWithProjection.startAllProjections();
            })

            it('should still continue with the playback even if there is an error saving to the errors stream', (done) => {
                const eventstoreEvents = [{
                    id: 'some_es_id',
                    payload: {
                        name: 'aggregate_added',
                        payload: {
                            someField: 'field1'
                        }
                    }
                }, {
                    id: 'some_es_id',
                    payload: {
                        name: 'aggregate_updated',
                        payload: {
                            someField: 'field2'
                        }
                    }
                }];

                const projection = {
                    query: {
                        aggregate: 'aggregate',
                        context: 'context'
                    },
                    projectionId: 'projectionId',
                    playbackInterface: {
                        aggregate_added: function(state, event, funcs, playbackDone) {
                            throw new Error('error in playing back aggregate_added event');;
                        },
                        aggregate_updated: function(state, event, funcs, playbackDone) {
                            const expectedEventstoreEvent = eventstoreEvents[1];
                            expect(event.payload).toEqual(expectedEventstoreEvent.payload);
                            playbackDone();
                            done();
                        },
                    }
                };

                let addEventForErrorStreamCalled = false;
                getEventStreamResult.addEvent.and.callFake((event) => {
                    if (getEventStreamResult.context == 'projection-errors' &&
                        getEventStreamResult.aggregate == 'projectionId' &&
                        getEventStreamResult.aggregateId == 'projectionId-errors') {
                        throw new Error('saving in error stream');
                    }
                });

                jobsManager.processJobGroup.and.callFake((owner, jobGroup, onProcessJob, onProcessCompletedJob) => {
                    onProcessJob.call(owner, 'jobId', {
                        projection: projection
                    }, {}, (error, result) => {});
                    return Promise.resolve();
                });

                esWithProjection.getEvents = jasmine.createSpy('getEvents', esWithProjection.getEvents);
                let firstLoop = 0;
                esWithProjection.getEvents.and.callFake((query, offset, limit, cb) => {
                    if (firstLoop == 0) {
                        firstLoop += 1;
                        cb(null, eventstoreEvents);
                    }
                    cb(null, []);
                });

                esWithProjection.project(projection);

                const result = esWithProjection.startAllProjections();
            })

            it('should call queue another job when a job is completed', (done) => {
                const projection = {
                    query: {
                        aggregate: 'aggregate',
                        context: 'context'
                    },
                    projectionId: 'projectionId',
                    playbackInterface: {}
                };


                spyOn(esWithProjection, '_waitUntilNotifyProject').and.callFake(async (jobId, query, delay) => {
                    return "token";
                });

                jobsManager.processJobGroup.and.callFake((owner, jobGroup, onProcessJob, onProcessCompletedJob) => {
                    console.log('test 456');
                    onProcessCompletedJob.call(owner, 'jobId', {
                        projection: projection
                    });
                    return Promise.resolve({});
                });

                // Note: need to add counter since code passes jobsManager.queueJob twice
                let counter = 0;
                jobsManager.queueJob.and.callFake((job, opts) => {
                    counter += 1;
                    if (counter == 2) {
                        const jobExpected = {
                            id: "projection-group:test:projection:projectionId",
                            group: "projection-group:test",
                            payload: {
                                projection: projection,
                                _meta: {
                                    token: "token"
                                }
                            }
                        };
                        expect(job).toEqual(jobExpected);
                        done();
                    }
                });

                esWithProjection.project(projection);
                const result = esWithProjection.startAllProjections();
            })

            describe('outputting a state', () => {
                it('should call the getLastEvent with correct params', (done) => {
                    const expectedEventstoreEvent = {
                        id: 'some_es_id',
                        payload: {
                            name: 'aggregate_added',
                            payload: {
                                someField: 'field1'
                            }
                        }
                    }

                    const projection = {
                        query: {
                            aggregate: 'aggregate',
                            context: 'context'
                        },
                        projectionId: 'projectionId',
                        playbackInterface: {
                            aggregate_added: function(state, event, funcs, playbackDone) {
                                playbackDone();
                            }
                        },
                        outputState: 'true'
                    };

                    jobsManager.processJobGroup.and.callFake(async (owner, jobGroup, onProcessJob, onProcessCompletedJob) => {
                        onProcessJob.call(owner, 'jobId', {
                            projection: projection
                        }, {}, (error, result) => {});
                        return Promise.resolve();
                    });

                    esWithProjection.getEvents = jasmine.createSpy('getEvents', esWithProjection.getEvents);
                    let firstLoop = 0;
                    esWithProjection.getEvents.and.callFake((query, offset, limit, cb) => {
                        if (firstLoop == 0) {
                            firstLoop += 1;
                            cb(null, [expectedEventstoreEvent]);
                        }
                        cb(null, []);
                    });

                    esWithProjection.getLastEvent.and.callFake((query, cb) => {
                        const expectedQuery = {
                            aggregate: 'states',
                            context: options.stateContextName,
                            aggregateId: `${projection.projectionId}-result`
                        }
                        expect(query).toEqual(expectedQuery);
                        done();
                    });

                    esWithProjection.project(projection);

                    const result = esWithProjection.startAllProjections();
                })

                it('should call $init if there is no event yet for the projection', (done) => {
                    const expectedEventstoreEvent = {
                        id: 'some_es_id',
                        payload: {
                            name: 'aggregate_added',
                            payload: {
                                someField: 'field1'
                            }
                        }
                    }

                    const projection = {
                        query: {
                            aggregate: 'aggregate',
                            context: 'context'
                        },
                        projectionId: 'projectionId',
                        playbackInterface: {
                            $init: function() {
                                done();
                            },
                            aggregate_added: function(state, event, funcs, playbackDone) {
                                playbackDone();
                            }
                        },
                        outputState: 'true'
                    };

                    jobsManager.processJobGroup.and.callFake((owner, jobGroup, onProcessJob, onProcessCompletedJob) => {
                        onProcessJob.call(owner, 'jobId', {
                            projection: projection
                        }, {}, (error, result) => {});
                        return Promise.resolve();
                    });

                    esWithProjection.getEvents = jasmine.createSpy('getEvents', esWithProjection.getEvents);
                    let firstLoop = 0;
                    esWithProjection.getEvents.and.callFake((query, offset, limit, cb) => {
                        if (firstLoop == 0) {
                            firstLoop += 1;
                            cb(null, [expectedEventstoreEvent]);
                        }
                        cb(null, []);
                    });

                    const lastEvent = {
                        payload: {
                            count: 1
                        }
                    }
                    esWithProjection.getLastEvent.and.callFake((query, cb) => {
                        cb();
                    });

                    esWithProjection.project(projection);

                    const result = esWithProjection.startAllProjections();
                })

                it('should set the state to an empty object if $init returns void/null/undefined', (done) => {
                    const projection = {
                        query: {
                            aggregate: 'aggregate',
                            context: 'context'
                        },
                        projectionId: 'projectionId',
                        playbackInterface: {
                            $init: function() {
                                // no return
                            },
                            aggregate_added: function(state, event, funcs, playbackDone) {
                                expect(event).toEqual(expectedEventstoreEvent);
                                expect(state).toEqual({})
                                playbackDone();
                                done();
                            }
                        },
                        outputState: 'true'
                    };

                    const expectedEventstoreEvent = {
                        id: 'some_es_id',
                        payload: {
                            name: 'aggregate_added',
                            payload: {
                                someField: 'field1'
                            }
                        }
                    }

                    jobsManager.processJobGroup.and.callFake((owner, jobGroup, onProcessJob, onProcessCompletedJob) => {
                        onProcessJob.call(owner, 'jobId', {
                            projection: projection
                        }, {}, (error, result) => {});
                        return Promise.resolve();
                    });

                    esWithProjection.getEvents = jasmine.createSpy('getEvents', esWithProjection.getEvents);
                    let firstLoop = 0;
                    esWithProjection.getEvents.and.callFake((query, offset, limit, cb) => {
                        if (firstLoop == 0) {
                            firstLoop += 1;
                            cb(null, [expectedEventstoreEvent]);
                        }
                        cb(null, []);
                    });

                    const lastEvent = {
                        payload: {
                            count: 1
                        }
                    }
                    esWithProjection.getLastEvent.and.callFake((query, cb) => {
                        cb();
                    });

                    esWithProjection.project(projection);

                    const result = esWithProjection.startAllProjections();
                })

                it('should call the $any event handler if it is defined and no event handler for the given event name is present', (done) => {
                    const projection = {
                        query: {
                            aggregate: 'aggregate',
                            context: 'context'
                        },
                        projectionId: 'projectionId',
                        playbackInterface: {
                            $init: function() {
                                // no return
                            },
                            $any: function(state, event, funcs, playbackDone) {
                                playbackDone();
                                done();
                            }
                        },
                        outputState: 'true'
                    };

                    const expectedEventstoreEvent = {
                        id: 'some_es_id',
                        payload: {
                            name: 'aggregate_added',
                            payload: {
                                someField: 'field1'
                            }
                        }
                    }

                    jobsManager.processJobGroup.and.callFake((owner, jobGroup, onProcessJob, onProcessCompletedJob) => {
                        onProcessJob.call(owner, 'jobId', {
                            projection: projection
                        }, {}, (error, result) => {});
                        return Promise.resolve();
                    });

                    esWithProjection.getEvents = jasmine.createSpy('getEvents', esWithProjection.getEvents);
                    let firstLoop = 0;
                    esWithProjection.getEvents.and.callFake((query, offset, limit, cb) => {
                        if (firstLoop == 0) {
                            firstLoop += 1;
                            cb(null, [expectedEventstoreEvent]);
                        }
                        cb(null, []);
                    });

                    const lastEvent = {
                        payload: {
                            count: 1
                        }
                    }
                    esWithProjection.getLastEvent.and.callFake((query, cb) => {
                        cb();
                    });

                    esWithProjection.project(projection);

                    const result = esWithProjection.startAllProjections();
                })

                it('should get the correct state stream if partitionBy is set to "stream"', (done) => {
                    const projection = {
                        query: {
                            aggregate: 'aggregate',
                            context: 'context'
                        },
                        projectionId: 'projectionId',
                        playbackInterface: {
                            $init: function() {
                                // no return
                            },
                            aggregate_added: function(state, event, funcs, playbackDone) {
                                playbackDone();
                            }
                        },
                        outputState: 'true',
                        partitionBy: 'stream'
                    };

                    const expectedEventstoreEvent = {
                        id: 'some_es_id',
                        payload: {
                            name: 'aggregate_added',
                            payload: {
                                someField: 'field1'
                            }
                        },
                        aggregate: projection.query.aggregate,
                        aggregateId: 'aggregate_id',
                        context: projection.query.context
                    }

                    jobsManager.processJobGroup.and.callFake((owner, jobGroup, onProcessJob, onProcessCompletedJob) => {
                        onProcessJob.call(owner, 'jobId', {
                            projection: projection
                        }, {}, (error, result) => {});
                        return Promise.resolve();
                    });

                    esWithProjection.getEvents = jasmine.createSpy('getEvents', esWithProjection.getEvents);
                    let firstLoop = 0;
                    esWithProjection.getEvents.and.callFake((query, offset, limit, cb) => {
                        if (firstLoop == 0) {
                            firstLoop += 1;
                            cb(null, [expectedEventstoreEvent]);
                        }
                        cb(null, []);
                    });

                    esWithProjection.getLastEvent.and.callFake((query, cb) => {
                        // <projectionid>[-<context>][-<aggregate>]-<aggregateId></aggregateId>-result
                        const expectedQuery = {
                            aggregate: 'states',
                            context: options.stateContextName,
                            aggregateId: `${projection.projectionId}-${projection.query.context}-${projection.query.aggregate}-${expectedEventstoreEvent.aggregateId}-result`
                        }
                        expect(query).toEqual(expectedQuery)
                        done();
                    });

                    esWithProjection.project(projection);

                    const result = esWithProjection.startAllProjections();
                })

                it('should get the correct state stream if partitionBy is set to a function callback', (done) => {
                    const projection = {
                        query: {
                            aggregate: 'aggregate',
                            context: 'context'
                        },
                        projectionId: 'projectionId',
                        playbackInterface: {
                            $init: function() {
                                // no return
                            },
                            aggregate_added: function(state, event, funcs, playbackDone) {
                                playbackDone();
                            }
                        },
                        outputState: 'true',
                        partitionBy: function(event) {
                            return event.payload.payload.someField
                        }
                    };

                    const expectedEventstoreEvent = {
                        id: 'some_es_id',
                        payload: {
                            name: 'aggregate_added',
                            payload: {
                                someField: 'field1'
                            }
                        },
                        aggregate: projection.query.aggregate,
                        aggregateId: 'aggregate_id',
                        context: projection.query.context
                    }

                    jobsManager.processJobGroup.and.callFake((owner, jobGroup, onProcessJob, onProcessCompletedJob) => {
                        onProcessJob.call(owner, 'jobId', {
                            projection: projection
                        }, {}, (error, result) => {});
                        return Promise.resolve();
                    });

                    esWithProjection.getEvents = jasmine.createSpy('getEvents', esWithProjection.getEvents);
                    let firstLoop = 0;
                    esWithProjection.getEvents.and.callFake((query, offset, limit, cb) => {
                        if (firstLoop == 0) {
                            firstLoop += 1;
                            cb(null, [expectedEventstoreEvent]);
                        }
                        cb(null, []);
                    });

                    esWithProjection.getLastEvent.and.callFake((query, cb) => {
                        // <projectionid>[-<context>][-<aggregate>]-<aggregateId></aggregateId>-result
                        const expectedQuery = {
                            aggregate: 'states',
                            context: options.stateContextName,
                            aggregateId: `${projection.projectionId}-${expectedEventstoreEvent.payload.payload.someField}-result`
                        }
                        expect(query).toEqual(expectedQuery)
                        done();
                    });

                    esWithProjection.project(projection);

                    const result = esWithProjection.startAllProjections();
                })

                it('should use no partitioning if partitionBy function callback returns falsy', (done) => {
                    const projection = {
                        query: {
                            aggregate: 'aggregate',
                            context: 'context'
                        },
                        projectionId: 'projectionId',
                        playbackInterface: {
                            $init: function() {
                                // no return
                            },
                            aggregate_added: function(state, event, funcs, playbackDone) {
                                playbackDone();
                                done();
                            }
                        },
                        outputState: 'true',
                        partitionBy: function(event) {
                            return null;
                        }
                    };

                    const expectedEventstoreEvent = {
                        id: 'some_es_id',
                        payload: {
                            name: 'aggregate_added',
                            payload: {
                                someField: 'field1'
                            }
                        },
                        aggregate: projection.query.aggregate,
                        aggregateId: 'aggregate_id',
                        context: projection.query.context
                    }

                    jobsManager.processJobGroup.and.callFake((owner, jobGroup, onProcessJob, onProcessCompletedJob) => {
                        onProcessJob.call(owner, 'jobId', {
                            projection: projection
                        }, {}, (error, result) => {});
                        return Promise.resolve();
                    });

                    esWithProjection.getEvents = jasmine.createSpy('getEvents', esWithProjection.getEvents);
                    let firstLoop = 0;
                    esWithProjection.getEvents.and.callFake((query, offset, limit, cb) => {
                        if (firstLoop == 0) {
                            firstLoop += 1;
                            cb(null, [expectedEventstoreEvent]);
                        }
                        cb(null, []);
                    });

                    esWithProjection.getLastEvent.and.callFake((query, cb) => {
                        // <projectionid>[-<context>][-<aggregate>]-<aggregateId></aggregateId>-result
                        const expectedQuery = {
                            aggregate: 'states',
                            context: options.stateContextName,
                            aggregateId: `${projection.projectionId}-result`
                        }
                        expect(query).toEqual(expectedQuery)
                        done();
                    });

                    esWithProjection.project(projection);

                    const result = esWithProjection.startAllProjections();
                })

                it('should still continue with the second event if there is no handler for the first event', (done) => {
                    const projection = {
                        query: {
                            aggregate: 'aggregate',
                            context: 'context'
                        },
                        projectionId: 'projectionId',
                        playbackInterface: {
                            aggregate_updated: function(state, event, funcs, playbackDone) {
                                playbackDone();
                                done();
                            }
                        },
                        outputState: 'true',
                        partitionBy: function(event) {
                            return null;
                        }
                    };

                    const expectedEventstoreEvent = {
                        id: 'some_es_id',
                        payload: {
                            name: 'aggregate_added',
                            payload: {
                                someField: 'field1'
                            }
                        },
                        aggregate: projection.query.aggregate,
                        aggregateId: 'aggregate_id',
                        context: projection.query.context
                    }

                    const expectedEventstoreEvent2 = {
                        id: 'some_es_id_2',
                        payload: {
                            name: 'aggregate_updated',
                            payload: {
                                someField: 'field1'
                            }
                        },
                        aggregate: projection.query.aggregate,
                        aggregateId: 'aggregate_id',
                        context: projection.query.context
                    }

                    jobsManager.processJobGroup.and.callFake((owner, jobGroup, onProcessJob, onProcessCompletedJob) => {
                        onProcessJob.call(owner, 'jobId', {
                            projection: projection
                        }, {}, (error, result) => {});
                        return Promise.resolve();
                    });

                    esWithProjection.getEvents = jasmine.createSpy('getEvents', esWithProjection.getEvents);
                    let firstLoop = 0;
                    esWithProjection.getEvents.and.callFake((query, offset, limit, cb) => {
                        if (firstLoop == 0) {
                            firstLoop += 1;
                            cb(null, [expectedEventstoreEvent, expectedEventstoreEvent2]);
                        }
                        cb(null, []);
                    });

                    esWithProjection.getLastEvent.and.callFake((query, cb) => {
                        // <projectionid>[-<context>][-<aggregate>]-<aggregateId></aggregateId>-result
                        const expectedQuery = {
                            aggregate: 'states',
                            context: options.stateContextName,
                            aggregateId: `${projection.projectionId}-result`
                        }
                        expect(query).toEqual(expectedQuery)
                        done();
                    });

                    esWithProjection.project(projection);

                    const result = esWithProjection.startAllProjections();
                })

                it('should pass the correct state and event to the correct event handler', (done) => {
                    const projection = {
                        query: {
                            aggregate: 'aggregate',
                            context: 'context'
                        },
                        projectionId: 'projectionId',
                        playbackInterface: {
                            $init: function() {

                            },
                            aggregate_added: function(state, event, funcs, playbackDone) {
                                expect(event).toEqual(expectedEventstoreEvent);
                                expect(state).toEqual(expectedProjectionState.state)
                                playbackDone();
                                done();
                            }
                        },
                        outputState: 'true'
                    };

                    const expectedEventstoreEvent = {
                        id: 'some_es_id',
                        payload: {
                            name: 'aggregate_added',
                            payload: {
                                someField: 'field1'
                            }
                        }
                    }

                    const expectedProjectionState = {
                        id: `${projection.projectionId}-result`,
                        state: {
                            count: 10
                        }
                    }

                    jobsManager.processJobGroup.and.callFake((owner, jobGroup, onProcessJob, onProcessCompletedJob) => {
                        onProcessJob.call(owner, 'jobId', {
                            projection: projection
                        }, {}, (error, result) => {});
                        return Promise.resolve();
                    });

                    esWithProjection.getEvents = jasmine.createSpy('getEvents', esWithProjection.getEvents);
                    let firstLoop = 0;
                    esWithProjection.getEvents.and.callFake((query, offset, limit, cb) => {
                        if (firstLoop == 0) {
                            firstLoop += 1;
                            cb(null, [expectedEventstoreEvent]);
                        }
                        cb(null, []);
                    });

                    const lastEvent = {
                        payload: {
                            count: 1
                        }
                    }
                    esWithProjection.getLastEvent.and.callFake((query, cb) => {
                        cb(null, {
                            payload: expectedProjectionState.state
                        });
                    });

                    esWithProjection.project(projection);

                    const result = esWithProjection.startAllProjections();
                })

                it('should save the projection state if the state changes', (done) => {
                    const projection = {
                        query: {
                            aggregate: 'aggregate',
                            context: 'context'
                        },
                        projectionId: 'projectionId',
                        playbackInterface: {
                            $init: function() {

                            },
                            aggregate_added: function(state, event, funcs, playbackDone) {
                                state.count++;
                                playbackDone();
                            }
                        },
                        outputState: 'true'
                    };

                    const expectedEventstoreEvent = {
                        id: 'some_es_id',
                        payload: {
                            name: 'aggregate_added',
                            payload: {
                                someField: 'field1'
                            }
                        }
                    }

                    const expectedProjectionState = {
                        id: `${projection.projectionId}-result`,
                        state: {
                            count: 10
                        }
                    }

                    jobsManager.processJobGroup.and.callFake((owner, jobGroup, onProcessJob, onProcessCompletedJob) => {
                        onProcessJob.call(owner, 'jobId', {
                            projection: projection
                        }, {}, (error, result) => {});
                        return Promise.resolve();

                    });

                    esWithProjection.getEvents = jasmine.createSpy('getEvents', esWithProjection.getEvents);
                    let firstLoop = 0;
                    esWithProjection.getEvents.and.callFake((query, offset, limit, cb) => {
                        if (firstLoop == 0) {
                            firstLoop += 1;
                            cb(null, [expectedEventstoreEvent]);
                        }
                        cb(null, []);
                    });

                    const lastEvent = {
                        payload: {
                            count: 1
                        }
                    }
                    esWithProjection.getLastEvent.and.callFake((query, cb) => {
                        cb(null, {
                            payload: expectedProjectionState.state
                        });
                    });

                    const projectionStream = jasmine.createSpyObj('projectionStream', ['addEvent', 'commit']);
                    projectionStream.commit.and.callFake((cb) => {
                        expect(projectionStream.addEvent).toHaveBeenCalledWith({
                            count: expectedProjectionState.state.count + 1,
                            _meta: {
                                fromEvent: expectedEventstoreEvent
                            }
                        });
                        done();
                    })

                    esWithProjection.getEventStream.and.callFake((query, revMin, revMax, cb) => {
                        cb(null, projectionStream);
                    });

                    esWithProjection.project(projection);

                    const result = esWithProjection.startAllProjections();
                })

                it('should save the projection metadata on state changes', (done) => {
                    const projection = {
                        query: {
                            aggregate: 'aggregate',
                            context: 'context'
                        },
                        projectionId: 'projectionId',
                        playbackInterface: {
                            $init: function() {

                            },
                            aggregate_added: function(state, event, funcs, playbackDone) {
                                state.count++;
                                playbackDone();
                            }
                        },
                        outputState: 'true'
                    };

                    const expectedEventstoreEvent = {
                        id: 'some_es_id',
                        payload: {
                            name: 'aggregate_added',
                            payload: {
                                someField: 'field1'
                            }
                        }
                    }

                    const expectedProjectionState = {
                        id: `${projection.projectionId}-result`,
                        state: {
                            count: 10
                        }
                    }

                    jobsManager.processJobGroup.and.callFake((owner, jobGroup, onProcessJob, onProcessCompletedJob) => {
                        onProcessJob.call(owner, 'jobId', {
                            projection: projection
                        }, {}, (error, result) => {});
                        return Promise.resolve();

                    });

                    esWithProjection.getEvents = jasmine.createSpy('getEvents', esWithProjection.getEvents);
                    let firstLoop = 0;
                    esWithProjection.getEvents.and.callFake((query, offset, limit, cb) => {
                        if (firstLoop == 0) {
                            firstLoop += 1;
                            cb(null, [expectedEventstoreEvent]);
                        }
                        cb(null, []);
                    });

                    esWithProjection.getLastEvent.and.callFake((query, cb) => {
                        cb(null, {
                            payload: expectedProjectionState.state
                        });
                    });

                    const projectionStream = jasmine.createSpyObj('projectionStream', ['addEvent', 'commit']);
                    projectionStream.commit.and.callFake((cb) => {
                        expect(projectionStream.addEvent).toHaveBeenCalledWith({
                            count: expectedProjectionState.state.count + 1,
                            _meta: {
                                fromEvent: expectedEventstoreEvent
                            }
                        });
                        done();
                    })

                    esWithProjection.getEventStream.and.callFake((query, revMin, revMax, cb) => {
                        cb(null, projectionStream);
                    });

                    esWithProjection.project(projection);

                    const result = esWithProjection.startAllProjections();
                })

                it('should detect for changes even on deep/nested object state changes', (done) => {
                    const projection = {
                        query: {
                            aggregate: 'aggregate',
                            context: 'context'
                        },
                        projectionId: 'projectionId',
                        playbackInterface: {
                            $init: function() {
                                return {
                                    aggregates: []
                                }
                            },
                            aggregate_added: function(state, event, funcs, playbackDone) {
                                state.aggregates.push({
                                    aggregateId: expectedEventstoreEvent.aggregateId
                                });

                                playbackDone();
                            }
                        },
                        outputState: 'true'
                    };

                    const expectedEventstoreEvent = {
                        id: 'some_es_id',
                        payload: {
                            name: 'aggregate_added',
                            payload: {
                                someField: 'field1'
                            }
                        },
                        aggregateId: 'aggregateId'
                    }

                    const expectedProjectionState = {
                        id: `${projection.projectionId}-result`,
                        state: {
                            aggregates: []
                        }
                    }

                    jobsManager.processJobGroup.and.callFake((owner, jobGroup, onProcessJob, onProcessCompletedJob) => {
                        onProcessJob.call(owner, 'jobId', {
                            projection: projection
                        }, {}, (error, result) => {});
                        return Promise.resolve();

                    });

                    esWithProjection.getEvents = jasmine.createSpy('getEvents', esWithProjection.getEvents);
                    let firstLoop = 0;
                    esWithProjection.getEvents.and.callFake((query, offset, limit, cb) => {
                        if (firstLoop == 0) {
                            firstLoop += 1;
                            cb(null, [expectedEventstoreEvent]);
                        }
                        cb(null, []);
                    });

                    const lastEvent = {
                        payload: {
                            count: 1
                        }
                    }
                    esWithProjection.getLastEvent.and.callFake((query, cb) => {
                        cb(null, {
                            payload: expectedProjectionState.state
                        });
                    });

                    const projectionStream = jasmine.createSpyObj('projectionStream', ['addEvent', 'commit']);
                    projectionStream.commit.and.callFake((cb) => {
                        expect(projectionStream.addEvent).toHaveBeenCalledWith({
                            aggregates: [{
                                aggregateId: expectedEventstoreEvent.aggregateId
                            }],
                            _meta: {
                                fromEvent: expectedEventstoreEvent
                            }
                        });
                        done();
                    })

                    esWithProjection.getEventStream.and.callFake((query, revMin, revMax, cb) => {
                        cb(null, projectionStream);
                    });

                    esWithProjection.project(projection);

                    const result = esWithProjection.startAllProjections();
                })

                it('should save the new event if emit is called', (done) => {
                    const targetQuery = {
                        aggregate: 'target_aggregate',
                        context: 'context',
                        aggregateId: 'target_aggregate_id'
                    }
                    const projection = {
                        query: {
                            aggregate: 'aggregate',
                            context: 'context'
                        },
                        projectionId: 'projectionId',
                        playbackInterface: {
                            $init: function() {

                            },
                            aggregate_added: function(state, event, funcs, playbackDone) {
                                funcs.emit(targetQuery, event.payload, playbackDone);
                            }
                        },
                        outputState: 'true'
                    };

                    const expectedEventstoreEvent = {
                        id: 'some_es_id',
                        payload: {
                            name: 'aggregate_added',
                            payload: {
                                someField: 'field1'
                            }
                        },
                        aggregateId: 'aggregateId',
                        aggregate: 'aggregate',
                        context: 'context'
                    }

                    const expectedProjectionState = {
                        id: `${projection.projectionId}-result`,
                        state: {
                            count: 10
                        }
                    }

                    jobsManager.processJobGroup.and.callFake((owner, jobGroup, onProcessJob, onProcessCompletedJob) => {
                        onProcessJob.call(owner, 'jobId', {
                            projection: projection
                        }, {}, (error, result) => {});
                        return Promise.resolve();
                    });

                    esWithProjection.getEvents = jasmine.createSpy('getEvents', esWithProjection.getEvents);
                    let firstLoop = 0;
                    esWithProjection.getEvents.and.callFake((query, offset, limit, cb) => {
                        if (firstLoop == 0) {
                            firstLoop += 1;
                            cb(null, [expectedEventstoreEvent]);
                        }
                        cb(null, []);
                    });

                    const lastEvent = {
                        payload: {
                            count: 1
                        }
                    }
                    esWithProjection.getLastEvent.and.callFake((query, cb) => {
                        if (query.aggregateId == expectedProjectionState.id) {
                            cb(null, {
                                payload: expectedProjectionState.state
                            });
                        } else if (targetQuery.aggregateId == query.aggregateId) {
                            // make sure that the call to get aggregateId to the target is here
                            expect(query).toEqual(targetQuery);
                            cb(null, expectedEventstoreEvent);
                        }
                    });

                    const projectionStream = jasmine.createSpyObj('projectionStream', ['addEvent', 'commit']);
                    projectionStream.commit.and.callFake((cb) => {
                        if (projectionStream.addEvent.calls.count() == 1) {
                            // expect that the event that committed second is the emit call
                            expect(projectionStream.addEvent).toHaveBeenCalledWith(expectedEventstoreEvent.payload);
                            cb();
                            done();
                        } else {
                            cb();
                        }
                    })

                    esWithProjection.getEventStream.and.callFake((query, revMin, revMax, cb) => {
                        cb(null, projectionStream);
                    });

                    esWithProjection.project(projection);

                    const result = esWithProjection.startAllProjections();
                })
            })
        });
    })

    // TODO: Review tests - equivalent tests are placed on BufferedEventSubscription
    xdescribe('activatePolling', () => {
        it('should activate the polling', (done) => {
            esWithProjection.activatePolling(function(error) {
                expect(esWithProjection.pollingActive).toEqual(true);
                done();
            });
        })
    });

    // TODO: Review tests - equivalent tests are placed on BufferedEventSubscription
    xdescribe('deactivatePolling', () => {
        it('should deactivate the polling', (done) => {
            esWithProjection.activatePolling(function(error) {
                expect(esWithProjection.pollingActive).toEqual(false);
                done();
            });
        })
    });

    describe('subscribe', () => {
        describe('validating params and output', () => {
            it('should validate required param query as object', (done) => {
                try {
                    esWithProjection.subscribe();
                } catch (error) {
                    expect(error).toBeInstanceOf(Error);
                    expect(error.message).toEqual('query is required');
                    done();
                }
            });

            it('should validate that query should have at least aggregateId or streamId', (done) => {
                try {
                    esWithProjection.subscribe({});
                } catch (error) {
                    expect(error).toBeInstanceOf(Error);
                    expect(error.message).toEqual('aggregateId or streamId should be present in query');
                    done();
                }
            });

            it('should throw an error if offset is not a number', (done) => {
                try {
                    esWithProjection.subscribe({
                        aggregateId: 'aggregate_id'
                    }, null);
                } catch (error) {
                    expect(error).toBeInstanceOf(Error);
                    expect(error.message).toEqual('offset should be greater than or equal to 0');
                    done();
                }
            });

            it('should throw an error if offset is less than 0', (done) => {
                try {
                    esWithProjection.subscribe({
                        aggregateId: 'aggregate_id'
                    }, -1);
                } catch (error) {
                    expect(error).toBeInstanceOf(Error);
                    expect(error.message).toEqual('offset should be greater than or equal to 0');
                    done();
                }
            });

            it('should pass if streamId is passed', (done) => {
                try {
                    const token = esWithProjection.subscribe({
                        streamId: 'stream_id'
                    }, 0, function() {}, function() {});
                    expect(token).toBeInstanceOf(String);
                    done();
                } catch (error) {
                    // do nothing
                }
            });

            it('should pass if aggregateId is passed', (done) => {
                try {
                    const token = esWithProjection.subscribe({
                        aggregateId: 'aggregate_id'
                    }, 0);
                    expect(token).toBeInstanceOf(String);
                    done();
                } catch (error) {
                    // do nothing
                }
            });

            it('should return a token when no error', (done) => {
                const token = esWithProjection.subscribe({
                    aggregateId: 'aggregate_id'
                }, 0);
                expect(token).toBeInstanceOf(String);
                done();
            });

            it('should return a token when a query is passed as a string no error', (done) => {
                const token = esWithProjection.subscribe('aggregate_id', 0);
                expect(token).toBeInstanceOf(String);
                done();
            });

            it('should not have an error when callback is not defined', (done) => {
                esWithProjection.subscribe({
                    aggregateId: 'aggregate_id'
                }, 0, null);
                done();
            });
        });

        // TODO: Review tests - should not access private methods and variables.
        xdescribe('stream buffer', () => {
            beforeEach(() => {
                spyOn(esWithProjection, '_getHeapPercentage');
            });

            it('should initialize stream buffer, bucket and subscription', () => {
                const mockChannel = 'mockChannel';
                const mockBucket = 'mockBucket';
                const mockQuery = 'mockQuery';

                // spyOn(esWithProjection, '_onStreamBufferEventOffered');
                spyOn(esWithProjection, '_getChannel').and.returnValue(mockChannel);
                spyOn(esWithProjection, '_getCurrentStreamBufferBucket').and.returnValue(mockBucket);
                esWithProjection.subscribe(mockQuery, 0, function() {}, function() {});

                const streamBuffer = esWithProjection._streamBuffers[mockChannel];
                expect(streamBuffer.es).toEqual(esWithProjection);
                expect(streamBuffer.query).toEqual({
                    aggregateId: mockQuery
                });
                expect(streamBuffer.channel).toEqual(mockChannel);
                expect(streamBuffer.bucket).toEqual(mockBucket);
                expect(streamBuffer.bufferCapacity).toEqual(10);
                expect(streamBuffer.ttl).toEqual((1000 * 60 * 60 * 24));

                expect(esWithProjection._streamBufferBuckets[mockBucket][mockChannel]).toBeTruthy();
                expect(esWithProjection._streamBuffers[mockChannel]).toBeTruthy();
            });

            it('should call _onStreamBufferEventOffered with channel and bucket for the stream buffer onOfferEvent function', (done) => {
                const mockChannel = 'mockChannel';
                const mockBucket = 'mockBucket';
                const mockQuery = 'mockQuery';

                spyOn(esWithProjection, '_getChannel').and.returnValue(mockChannel);
                spyOn(esWithProjection, '_getCurrentStreamBufferBucket').and.returnValue(mockBucket);
                spyOn(esWithProjection, '_onStreamBufferEventOffered').and.callFake((bucket, channel) => {
                    expect(bucket).toEqual(mockBucket);
                    expect(channel).toEqual(mockChannel);
                    done();
                });

                esWithProjection.subscribe(mockQuery, 0);

                const streamBuffer = esWithProjection._streamBuffers[mockChannel];
                streamBuffer.onOfferEvent(mockBucket, mockChannel);
            });

            it('should delete stream buffer when stream buffer onCleanUp is called', () => {
                const mockChannel = 'mockChannel';
                const mockBucket = 'mockBucket';
                const mockQuery = 'mockQuery';

                spyOn(esWithProjection, '_onStreamBufferEventOffered');
                spyOn(esWithProjection, '_getChannel').and.returnValue(mockChannel);
                spyOn(esWithProjection, '_getCurrentStreamBufferBucket').and.returnValue(mockBucket);
                esWithProjection.subscribe(mockQuery, 0);


                expect(esWithProjection._streamBuffers[mockChannel]).toBeTruthy();
                expect(esWithProjection._streamBufferBuckets[mockBucket][mockChannel]).toBeTruthy();
                expect(esWithProjection._streamBuffers[mockChannel]).toBeTruthy();

                const streamBuffer = esWithProjection._streamBuffers[mockChannel];
                streamBuffer.onCleanUp(mockBucket, mockChannel);

                expect(esWithProjection._streamBuffers[mockChannel]).toBeFalsy();
                expect(esWithProjection._streamBufferBuckets[mockBucket][mockChannel]).toBeFalsy();
                expect(esWithProjection._streamBuffers[mockChannel]).toBeFalsy();
            });

            it('should call stream buffer offerEvent function when PubSub message is received', (done) => {
                const mockChannel = 'mockChannel';
                const mockBucket = 'mockBucket';
                const mockQuery = 'mockQuery';
                const mockData = {
                    query: {
                        channel: mockChannel
                    }
                };

                spyOn(esWithProjection, '_onStreamBufferEventOffered');
                spyOn(esWithProjection, '_getChannel').and.returnValue(mockChannel);
                spyOn(esWithProjection, '_getCurrentStreamBufferBucket').and.returnValue(mockBucket);

                esWithProjection.subscribe(mockQuery, 0);
                spyOn(esWithProjection._streamBuffers[mockChannel], 'offerEvent').and.callFake((data) => {
                    expect(data).toEqual(mockData);
                    done();
                });

                esWithProjection._processReceivedRedisMessage(esWithProjection.options.notifyCommitRedisChannel, JSON.stringify(mockData));
            });
        });
    });

    describe('unsubscribe', () => {
        describe('checking unsubscribe result', () => {
            it('should return true if subscription is existing', (done) => {
                const token = esWithProjection.subscribe({
                    aggregateId: 'aggregate_id'
                }, 0);
                const result = esWithProjection.unsubscribe(token);
                expect(result).toEqual(true);
                done();
            });

            it('should return false if subscription is missing', (done) => {
                const result = esWithProjection.unsubscribe('garbage');
                expect(result).toEqual(false);
                done();
            });
        })
    });

    describe('getPlaybackList', () => {
        it('should return no playbacklist if it still does not exist', (done) => {
            const query = {
                context: 'the_context'
            };

            const projectionId = 'the_projection_id';

            const projection = {
                projectionId: projectionId,
                query: query,
                playbackList: {
                    name: 'playbacklist_name',
                    fields: [{
                        name: 'field_name',
                        type: 'string'
                    }],
                    secondaryKeys: {
                        idx_field_name: [{
                            name: 'field_name',
                            sort: 'ASC'
                        }]
                    }
                }
            };

            esWithProjection.project(projection, function(error) {
                esWithProjection.getPlaybackList('not_existing', (err, pb) => {
                    expect(err).toBeFalsy();
                    expect(pb).toBeFalsy();
                    done();
                });
            });
        })

        it('should return the correct playback list', (done) => {
            const query = {
                context: 'the_context'
            };

            const projectionId = 'the_projection_id';

            const projection = {
                projectionId: projectionId,
                query: query,
                playbackList: {
                    name: 'playbacklist_name',
                    fields: [{
                        name: 'field_name',
                        type: 'string'
                    }],
                    secondaryKeys: {
                        idx_field_name: [{
                            name: 'field_name',
                            sort: 'ASC'
                        }]
                    }
                }
            };

            esWithProjection.project(projection, function(error) {
                esWithProjection.getPlaybackList('playbacklist_name', (err, pb) => {
                    expect(err).toBeFalsy();
                    expect(pb).toBeTruthy();
                    done();
                });
            });
        })
    });

    describe('getStateList', () => {
        it('should return no statelist if it still does not exist', (done) => {
            const query = {
                context: 'the_context'
            };

            const projectionId = 'the_projection_id';

            const projection = {
                projectionId: projectionId,
                query: query,
                stateList: {
                    name: 'statelist_name',
                    fields: [{
                        name: 'field_name',
                        type: 'string'
                    }],
                    secondaryKeys: {
                        idx_field_name: [{
                            name: 'field_name',
                            sort: 'ASC'
                        }]
                    }
                }
            };

            esWithProjection.project(projection, function(error) {
                esWithProjection.getStateList('not_existing', (err, pb) => {
                    expect(err).toBeFalsy();
                    expect(pb).toBeFalsy();
                    done();
                });
            });
        })

        it('should return the correct playback list', (done) => {
            const query = {
                context: 'the_context'
            };

            const projectionId = 'the_projection_id';

            const projection = {
                projectionId: projectionId,
                query: query,
                stateList: {
                    name: 'statelist_name',
                    fields: [{
                        name: 'field_name',
                        type: 'string'
                    }],
                    secondaryKeys: {
                        idx_field_name: [{
                            name: 'field_name',
                            sort: 'ASC'
                        }]
                    }
                }
            };

            esWithProjection.project(projection, function(error) {
                esWithProjection.getStateList('statelist_name', (err, pb) => {
                    expect(err).toBeFalsy();
                    expect(pb).toBeTruthy();
                    done();
                });
            });
        })
    });

    describe('registerPlaybackListView', () => {
        describe('should validate some required options', () => {

            it('should validate listStore', (done) => {
                // NOTE: just removing the option to test
                esWithProjection.options.listStore = null;

                esWithProjection.registerPlaybackListView('list_name', 'select * from list_name', function(error) {
                    expect(error.message).toEqual('listStore must be provided in the options');
                    done();
                });
            });

            it('should validate listStore.host', (done) => {
                // NOTE: just removing the option to test
                esWithProjection.options.listStore = {
                    port: 'port',
                    database: 'database',
                    user: 'user',
                    password: 'password'
                };

                esWithProjection.registerPlaybackListView('list_name', 'select * from list_name', function(error) {
                    expect(error.message).toEqual('listStore.host must be provided in the options');
                    done();
                });
            });

            it('should validate listStore.port', (done) => {

                // NOTE: just removing the option to test
                esWithProjection.options.listStore = {
                    host: 'host',
                    database: 'database',
                    user: 'user',
                    password: 'password'
                };

                esWithProjection.registerPlaybackListView('list_name', 'select * from list_name', function(error) {
                    expect(error.message).toEqual('listStore.port must be provided in the options');
                    done();
                });
            });

            it('should validate listStore.database', (done) => {

                // NOTE: just removing the option to test
                esWithProjection.options.listStore = {
                    host: 'host',
                    port: 'port',
                    user: 'user',
                    password: 'password'
                };

                esWithProjection.registerPlaybackListView('list_name', 'select * from list_name', function(error) {
                    expect(error.message).toEqual('listStore.database must be provided in the options');
                    done();
                });
            });

            it('should validate listStore.user', (done) => {
                // NOTE: just removing the option to test
                esWithProjection.options.listStore = {
                    host: 'host',
                    port: 'port',
                    database: 'database',
                    password: 'password'
                };

                esWithProjection.registerPlaybackListView('list_name', 'select * from list_name', function(error) {
                    expect(error.message).toEqual('listStore.user must be provided in the options');
                    done();
                });
            });

            it('should validate listStore.password', (done) => {
                // NOTE: just removing the option to test
                esWithProjection.options.listStore = {
                    host: 'host',
                    port: 'port',
                    database: 'database',
                    user: 'user',
                };

                esWithProjection.registerPlaybackListView('list_name', 'select * from list_name', function(error) {
                    expect(error.message).toEqual('listStore.password must be provided in the options');
                    done();
                });
            });
        });

        it('should register the correct playback list view', (done) => {
            esWithProjection.registerPlaybackListView('list_name', 'select * from list_name', function(error) {
                expect(EventstorePlaybackListViewFunction).toHaveBeenCalledWith({
                    host: esWithProjection.options.listStore.host,
                    port: esWithProjection.options.listStore.port,
                    database: esWithProjection.options.listStore.database,
                    user: esWithProjection.options.listStore.user,
                    password: esWithProjection.options.listStore.password,
                    listName: 'list_name',
                    query: 'select * from list_name'
                });
                expect(eventStorePlaybacklistView.init).toHaveBeenCalledTimes(1);
                done();
            });
        })
    });

    describe('getPlaybackListView', () => {
        it('should return falsy if playbacklistview is not existing', (done) => {
            esWithProjection.registerPlaybackListView('list_name', 'select * from list_name', function(error) {
                esWithProjection.getPlaybackListView('not_existing', function(err, pb) {
                    expect(err).toBeFalsy();
                    expect(pb).toBeFalsy();
                    done();
                });
            });
        })

        it('should return truthy if playbacklistview exists', (done) => {
            esWithProjection.registerPlaybackListView('list_name', 'select * from list_name', function(error) {
                esWithProjection.getPlaybackListView('list_name', function(err, pb) {
                    expect(err).toBeFalsy();
                    expect(pb).toBeTruthy();
                    done();
                });
            });
        })
    });

    describe('_streamBufferLRUCleaner queue', () => {
        describe('task', () => {
            it('should delete streamBuffer cleanup', (done) => {
                const mockStreamBuffer = jasmine.createSpyObj('mockStreamBuffer', ['close']);
                const mockChannel = 'mockChannel';
                esWithProjection._streamBuffers[mockChannel] = mockStreamBuffer;

                esWithProjection._streamBufferLRUCleaner.drain = () => {
                    expect(mockStreamBuffer.close).toHaveBeenCalled();
                    done();
                };

                esWithProjection._streamBufferLRUCleaner.push({
                    channel: mockChannel,
                });
            });
        });

        describe('drain', () => {
            it('should explicitly call garbage collection', () => {
                global.gc = jasmine.createSpy('global.gc');
                esWithProjection._streamBufferLRUCleaner.drain();
                expect(global.gc).toHaveBeenCalled();
            });

            it('should call _cleanOldestStreamBufferBucket if heap percentage is above threshold', () => {
                spyOn(esWithProjection, '_getHeapPercentage').and.returnValue(0.8);
                const spy = spyOn(esWithProjection, '_cleanOldestStreamBufferBucket');
                esWithProjection._streamBufferLRUCleaner.drain();
                expect(spy).toHaveBeenCalled();
            });

            it('should set _streamBufferLRULocked to false if heap percentage is less than threshold', () => {
                esWithProjection._streamBufferLRULocked = true;
                spyOn(esWithProjection, '_getHeapPercentage').and.returnValue(0.79999999);
                esWithProjection._streamBufferLRUCleaner.drain();
                expect(esWithProjection._streamBufferLRULocked).toBeFalsy();
            });
        });
    });

    // TODO: Review tests - should not access private methods and variables.
    xdescribe('_onStreamBufferEventOffered', () => {
        let mockBucket;
        let mockChannel;
        let mockStreamBuffer;

        beforeEach(() => {
            mockBucket = 'mockBucket';
            mockChannel = 'mockChannel';

            mockStreamBuffer = new StreamBuffer({
                es: esWithProjection,
                query: 'mockQuery',
                channel: mockChannel,
                bucket: mockBucket,
                bufferCapacity: 10,
                poolCapacity: 5,
                ttl: (1000 * 60 * 60 * 24)
            });

            esWithProjection._streamBuffers[mockChannel] = mockStreamBuffer;
            esWithProjection._streamBufferBuckets[mockBucket] = {};
            esWithProjection._streamBufferBuckets[mockBucket][mockChannel] = new Date().getTime();
        });

        it('should call _cleanOldestStreamBufferBucket if _streamBufferLRULocked is false and heapPercentage is over the threshold', () => {
            esWithProjection._streamBufferLRULocked = false;
            spyOn(esWithProjection, '_getHeapPercentage').and.returnValue(0.8);
            spyOn(esWithProjection, '_cleanOldestStreamBufferBucket');
            esWithProjection._onStreamBufferEventOffered(mockBucket, mockChannel);
            expect(esWithProjection._cleanOldestStreamBufferBucket).toHaveBeenCalled();
        });

        it('should not call _cleanOldestStreamBufferBucket _streamBufferLRULocked is false and heapPercentage is not over the threshold', () => {
            esWithProjection._streamBufferLRULocked = false;
            spyOn(esWithProjection, '_getHeapPercentage').and.returnValue(0.79999);
            spyOn(esWithProjection, '_cleanOldestStreamBufferBucket');
            esWithProjection._onStreamBufferEventOffered(mockBucket, mockChannel);
            expect(esWithProjection._cleanOldestStreamBufferBucket).not.toHaveBeenCalled();
        });

        it('should not check for heapPercentage if _streamBufferLRULocked is true', () => {
            esWithProjection._streamBufferLRULocked = true;
            spyOn(esWithProjection, '_getHeapPercentage');
            esWithProjection._onStreamBufferEventOffered(mockBucket, mockChannel);
            expect(esWithProjection._getHeapPercentage).not.toHaveBeenCalled();
        });

        it('should transfer stream buffer from current bucket to new bucket', () => {
            const newMockBucket = 'newMockBucket';
            esWithProjection._streamBufferLRULocked = true;
            spyOn(esWithProjection, '_getCurrentStreamBufferBucket').and.returnValue(newMockBucket);
            esWithProjection._onStreamBufferEventOffered(mockBucket, mockChannel);
            expect(esWithProjection._streamBufferBuckets[mockBucket][mockChannel]).toBeFalsy();
            expect(esWithProjection._streamBufferBuckets[newMockBucket][mockChannel]).toBeTruthy();
            expect(mockStreamBuffer.bucket).toEqual(newMockBucket);
        });
    });

    // TODO: Review tests - should not access private methods and variables.
    describe('_cleanOldestStreamBufferBucket', () => {
        it('should lock LRU execution', () => {
            esWithProjection._streamBufferLRULocked = false;
            esWithProjection._cleanOldestStreamBufferBucket();
            expect(esWithProjection._streamBufferLRULocked).toBeTruthy();
        });

        it('should delete oldest bucket and push all items inside bucket to _streamBufferLRUCleaner', () => {
            spyOn(esWithProjection._streamBufferLRUCleaner, 'push');
            esWithProjection._streamBufferBuckets = {
                '2020-07-29T11': {
                    channel1: 1,
                    channel2: 1,
                },
                '2020-07-28T15': {
                    channel3: 1,
                    channel4: 1
                },
                '2020-07-28T12': {
                    channel0: 1,
                    channel5: 1
                },
                '2020-07-28T13': {
                    channel8: 1
                }
            };

            esWithProjection._cleanOldestStreamBufferBucket();
            expect(esWithProjection._streamBufferLRUCleaner.push).toHaveBeenCalledWith({
                channel: 'channel0'
            });
            expect(esWithProjection._streamBufferLRUCleaner.push).toHaveBeenCalledWith({
                channel: 'channel5'
            });
            expect(esWithProjection._streamBufferBuckets['2020-07-28T12']).toBeFalsy();
        });
    });
})