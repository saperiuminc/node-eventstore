const EventStoreWithProjection = require('../lib/eventstore-projections/eventstore-projection');

describe('eventstore-projection tests', () => {
    // just instantiating for vscode jsdoc intellisense
    let esWithProjection = new EventStoreWithProjection();
    let options;
    let defaultStream;
    let distributedLock;
    let jobsManager;
    beforeEach(() => {
        distributedLock = jasmine.createSpyObj('distributedLock', ['lock', 'unlock']);
        distributedLock.lock.and.returnValue(Promise.resolve());
        distributedLock.unlock.and.returnValue(Promise.resolve());

        jobsManager = jasmine.createSpyObj('distributedLock', ['queueJob']);
        jobsManager.queueJob.and.returnValue(Promise.resolve());

        options = {
            pollingMaxRevisions: 10,
            pollingTimeout: 0, // so that polling is immediate
            eventCallbackTimeout: 0,
            projectionGroup: 'test',
            distributedLock: distributedLock,
            jobsManager: jobsManager
        };
        esWithProjection = new EventStoreWithProjection(options);

        esWithProjection.getLastEvent = jasmine.createSpy('getLastEvent', esWithProjection.getLastEvent);
        esWithProjection.getLastEvent.and.callFake((query, cb) => {
            cb();
        });


        esWithProjection.getEventStream = jasmine.createSpy('getEventStream', esWithProjection.getEventStream);
        esWithProjection.getEventStream.and.callFake((query, revMin, revMax, cb) => {
            console.log('common getEventStream');
            // by default we only poll/loop one time for the event stream
            esWithProjection.deactivatePolling();
            cb();
        });

        defaultStream = jasmine.createSpyObj('default_stream', ['addEvent', 'commit']);
        defaultStream.events = [];
        defaultStream.commit.and.callFake((cb) => {
            cb();
        })
        esWithProjection.getLastEventAsStream = jasmine.createSpy('getLastEventAsStream', esWithProjection.getLastEventAsStream);
        esWithProjection.getLastEventAsStream.and.callFake((query, cb) => {
            console.log('common getLastEventAsStream');
            cb(null, defaultStream);
        });
    });

    describe('project', () => {
        describe('validating params and output', () => {
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

            it('should not throw an error if callback is not a function', (done) => {
                esWithProjection.project({
                    projectionId: 'the_projection_id',
                    query: {}
                });
                expect(true).toBeTruthy();
                done();
            });


            it('should return void', (done) => {
                const res = esWithProjection.project({
                    projectionId: 'the_projection_id',
                    query: {}
                });

                expect(res).toBeUndefined();
                done();
            });
        })

        describe('adding the projection to the projection stream storage', () => {
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
                    name: `projection-groups:${options.projectionGroup}:projections:${projection.projectionId}`,
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

        describe('ensuring that only one projection event is created if multiple instances are created', () => {
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

                const lockKey = `projection-groups:${options.projectionGroup}:projections:${projectionId}`;
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

                const lockKey = `projection-groups:${options.projectionGroup}:projections:${projectionId}`;
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
            it('should call jobsManager.queueJob if jobsManager is passed as an option', (done) => {
                const query = {
                    context: 'the_context'
                };

                const projectionId = 'the_projection_id';

                const projection = {
                    projectionId: projectionId,
                    query: query
                };

                const projectionKey = `projection-groups:${options.projectionGroup}:projections:${projectionId}`;

                const jobParams = {
                    jobId: projectionKey,
                    jobGroup: `projection-groups:${options.projectionGroup}`,
                    jobPayload: {
                        projectionId: projection.projectionId,
                        query: projection.query,
                        partitionBy: projection.partitionBy,
                        projectionGroup: options.projectionGroup,
                        meta: projection.meta
                    }
                };

                esWithProjection.project(projection, function(error) {
                    expect(error).toBeUndefined();
                    expect(esWithProjection.options.jobsManager.queueJob).toHaveBeenCalledWith(jobParams);
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

                const projectionKey = `projection-groups:${options.projectionGroup}:projections:${projection.projectionId}`;

                esWithProjection.options.jobsManager = undefined;

                esWithProjection.project(projection, function(error) {
                    expect(error).toBeUndefined();
                    done();
                });
            });
        });
    });

    describe('activatePolling', () => {
        it('should activate the polling', () => {
            esWithProjection.activatePolling(function(error) {
                expect(esWithProjection.pollingActive).toEqual(true);
                done();
            });
        })
    })

    describe('deactivatePolling', () => {
        it('should deactivate the polling', () => {
            esWithProjection.activatePolling(function(error) {
                expect(esWithProjection.pollingActive).toEqual(false);
                done();
            });
        })
    })

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
                    esWithProjection.subscribe({ aggregateId: 'aggregate_id' }, null);
                } catch (error) {
                    expect(error).toBeInstanceOf(Error);
                    expect(error.message).toEqual('offset should be greater than or equal to 0');
                    done();
                }
            });

            it('should throw an error if offset is less than 0', (done) => {
                try {
                    esWithProjection.subscribe({ aggregateId: 'aggregate_id' }, -1);
                } catch (error) {
                    expect(error).toBeInstanceOf(Error);
                    expect(error.message).toEqual('offset should be greater than or equal to 0');
                    done();
                }
            });

            it('should pass if streamId is passed', (done) => {
                try {
                    const token = esWithProjection.subscribe({ streamId: 'stream_id' }, 0);
                    expect(token).toBeInstanceOf(String);
                    done();
                } catch (error) {
                    // do nothing
                }
            });

            it('should pass if aggregateId is passed', (done) => {
                try {
                    const token = esWithProjection.subscribe({ aggregateId: 'aggregate_id' }, 0);
                    expect(token).toBeInstanceOf(String);
                    done();
                } catch (error) {
                    // do nothing
                }
            });

            it('should return a token when no error', (done) => {
                const token = esWithProjection.subscribe({ aggregateId: 'aggregate_id' }, 0);
                expect(token).toBeInstanceOf(String);
                done();
            });

            it('should return a token when a query is passed as a string no error', (done) => {
                const token = esWithProjection.subscribe('aggregate_id', 0);
                expect(token).toBeInstanceOf(String);
                done();
            });

            it('should not have an error when callback is not defined', (done) => {
                esWithProjection.subscribe({ aggregateId: 'aggregate_id' }, 0, null);
                done();
            });
        });

        describe('getting streams using offset and its logical boundaries', () => {
            it('should call Eventstore.getLastEvent with correct params', (done) => {
                const query = { aggregateId: 'aggregate_id' };
                esWithProjection.getLastEvent.and.callFake((query, cb) => {
                    expect(esWithProjection.getLastEvent).toHaveBeenCalledWith(query, jasmine.any(Function));
                    done();
                });

                esWithProjection.subscribe(query, 0);
            });

            it('should call Eventstore.getEventStream with revMin as zero when there are no events yet for that stream. revMax should just add pollingMaxRevisions to revMin', (done) => {
                const query = { aggregateId: 'aggregate_id' };
                esWithProjection.getLastEvent.and.callFake((query, cb) => {
                    // no events yet for this stream
                    cb();
                });

                esWithProjection.getEventStream.and.callFake((query, revMin, revMax, cb) => {
                    const exptectedRevMin = 0;
                    const expectedRevMax = exptectedRevMin + options.pollingMaxRevisions;
                    expect(esWithProjection.getEventStream).toHaveBeenCalledWith(query, exptectedRevMin, expectedRevMax, jasmine.any(Function));
                    esWithProjection.deactivatePolling();
                    cb();
                    done();
                });

                esWithProjection.subscribe(query, 0);


            });

            it('should call Eventstore.getEventStream with revMin as minimum revision (last streamRrevision + 1) when the passed offset is later than the minimum revision (last streamRrevision + 1). revMax should just add pollingMaxRevisions to revMin', (done) => {
                const query = { aggregateId: 'aggregate_id' };
                const offset = 15;
                esWithProjection.getLastEvent.and.callFake((query, cb) => {
                    // no events yet for this stream
                    cb(null, {
                        streamRevision: 10
                    });
                });

                esWithProjection.getEventStream.and.callFake((query, revMin, revMax, cb) => {
                    const exptectedRevMin = 11;
                    const expectedRevMax = exptectedRevMin + options.pollingMaxRevisions;
                    expect(esWithProjection.getEventStream).toHaveBeenCalledWith(query, exptectedRevMin, expectedRevMax, jasmine.any(Function));
                    esWithProjection.deactivatePolling();
                    cb();
                    done();
                });

                esWithProjection.subscribe(query, offset);
            });

            it('should call Eventstore.getEventStream with correct revMin when passed an offset', (done) => {
                const query = { aggregateId: 'aggregate_id' };
                const offset = 6;
                esWithProjection.getLastEvent.and.callFake((query, cb) => {
                    // no events yet for this stream
                    cb(null, {
                        streamRevision: 10
                    });
                });

                esWithProjection.getEventStream.and.callFake((query, revMin, revMax, cb) => {
                    const exptectedRevMin = 6;
                    const expectedRevMax = exptectedRevMin + options.pollingMaxRevisions;
                    expect(esWithProjection.getEventStream).toHaveBeenCalledWith(query, exptectedRevMin, expectedRevMax, jasmine.any(Function));
                    esWithProjection.deactivatePolling();
                    cb();
                    done();
                });

                esWithProjection.subscribe(query, offset);
            });
        });

        describe('polling the event stream', () => {
            it('should call getEventStream 5 times (poll)', (done) => {
                let getEventStreamCallCounter = 0;
                // do spyOn again to override default one time call for getEventStream
                esWithProjection.getEventStream.and.callFake((query, revMin, revMax, cb) => {
                    getEventStreamCallCounter++;
                    if (getEventStreamCallCounter == 5) {
                        esWithProjection.deactivatePolling();
                        expect(getEventStreamCallCounter).toEqual(5);
                        done();
                    }
                    cb();
                });

                const query = { aggregateId: 'aggregate_id' };
                const offset = 15;

                esWithProjection.subscribe(query, offset);
            })

            it('should call onEventCallback when there is a new event', (done) => {
                // do spyOn again to override default one time call for getEventStream
                const eventStream = {
                    events: [
                        { streamRevision: 1 },
                        { streamRevision: 2 },
                        { streamRevision: 3 },
                        { streamRevision: 4 },
                        { streamRevision: 5 }
                    ]
                }
                esWithProjection.getEventStream.and.callFake((query, revMin, revMax, cb) => {
                    cb(null, eventStream);
                });

                const query = { aggregateId: 'aggregate_id' };
                const offset = 15;

                let onEventCounter = 0;
                esWithProjection.subscribe(query, offset, (error, event, next) => {
                    expect(eventStream.events[onEventCounter]) == event;
                    onEventCounter++;
                    next();

                    if (onEventCounter == 5) {
                        esWithProjection.deactivatePolling();
                        done();
                    }
                });
            })

            it('should continue with the loop even if first getEventStream call throws an error', (done) => {
                // do spyOn again to override default one time call for getEventStream
                const eventStream = {
                    events: [
                        { streamRevision: 1 },
                        { streamRevision: 2 },
                        { streamRevision: 3 },
                        { streamRevision: 4 },
                        { streamRevision: 5 }
                    ]
                };
                esWithProjection.getEventStream.and.callFake((query, revMin, revMax, cb) => {
                    if (esWithProjection.getEventStream.calls.count() == 1) {

                        throw new Error('unhandled error!');
                    } else if (esWithProjection.getEventStream.calls.count() == 2) {
                        cb(null, eventStream);
                        esWithProjection.deactivatePolling();
                        done();
                    }
                });

                const query = { aggregateId: 'aggregate_id' };
                const offset = 0;
                esWithProjection.subscribe(query, offset, (error, event, next) => {
                    next();
                });
            })

            it('should continue with the loop even if onEventCallback throws an error', (done) => {
                // do spyOn again to override default one time call for getEventStream
                const eventStream = {
                    events: [
                        { streamRevision: 1 },
                        { streamRevision: 2 },
                        { streamRevision: 3 }
                    ]
                };
                esWithProjection.getEventStream.and.callFake((query, revMin, revMax, cb) => {
                    if (esWithProjection.getEventStream.calls.count() == 1) {
                        cb(null, eventStream);
                    } else if (esWithProjection.getEventStream.calls.count() == 2) {
                        esWithProjection.deactivatePolling();
                        done();
                    }
                });

                const query = { aggregateId: 'aggregate_id' };
                const offset = 0;
                esWithProjection.subscribe(query, offset, (error, event, next) => {
                    throw new Error('unhandled error on event callback');
                });
            })

            it('should pass the correct revMin to getEventStream after processing a set of events from a stream', (done) => {
                // do spyOn again to override default one time call for getEventStream
                const numOfEventsPerStream = 3;
                const streams = [{
                        events: [
                            { streamRevision: 1 },
                            { streamRevision: 2 },
                            { streamRevision: 3 }
                        ]
                    },
                    {
                        events: [
                            { streamRevision: 4 },
                            { streamRevision: 5 },
                            { streamRevision: 6 }
                        ]
                    }
                ];

                esWithProjection.getEventStream.and.callFake((query, revMin, revMax, cb) => {
                    const callIndex = esWithProjection.getEventStream.calls.count() - 1;
                    if (streams.length < callIndex) {
                        cb(null, streams[callIndex]);
                    } else {
                        esWithProjection.deactivatePolling();
                        done();
                    }
                });

                const query = { aggregateId: 'aggregate_id' };
                const offset = 0;
                let eventCounter = 0;
                esWithProjection.subscribe(query, offset, (error, event, next) => {

                    const streamIndex = Math.floor(eventCounter / numOfEventsPerStream);
                    const stream = streams[streamIndex];

                    const eventIndex = streamIndex * numOfEventsPerStream + (eventCounter % numOfEventsPerStream);
                    expect(stream.events[eventIndex]).toEqual(event);
                    eventCounter++;
                    next();
                });
            })

            it('should still continue if next iterator is not called within the timeout period or timedout', (done) => {
                // do spyOn again to override default one time call for getEventStream
                const stream = {
                    events: [
                        { streamRevision: 1 },
                        { streamRevision: 2 },
                        { streamRevision: 3 }
                    ]
                };

                esWithProjection.getEventStream.and.callFake((query, revMin, revMax, cb) => {
                    cb(null, stream);
                });

                const query = { aggregateId: 'aggregate_id' };
                const offset = 0;
                let eventCounter = 0;
                esWithProjection.subscribe(query, offset, (error, event, next) => {
                    if (eventCounter == 0) {
                        // explicitly do not call next on the first event
                    } else {
                        // i should still be able to get the next event
                        esWithProjection.deactivatePolling();
                        expect(event).toEqual(stream.events[eventCounter]);
                        done();
                    }
                    eventCounter++;
                });
            })
        });
    });

    describe('unsubscribe', () => {
        describe('checking unsubscribe result', () => {
            it('should return true if subscription is existing', (done) => {
                const token = esWithProjection.subscribe('aggregate_id', 0);
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

        describe('breaking the poll loop', () => {
            it('should stop getting the events if unsubscribed', (done) => {
                let token = null;
                const stream = {
                    events: [
                        { streamRevision: 1 },
                        { streamRevision: 2 },
                        { streamRevision: 3 }
                    ]
                };

                esWithProjection.getEventStream.and.callFake((query, revMin, revMax, cb) => {
                    esWithProjection.unsubscribe(token);
                    cb(null, stream);
                });

                token = esWithProjection.subscribe('aggregate_id', 0);

                // TODO: in order to test if the loop stopped i did a simple set timeout to check later on. need to find a better way without
                // doing a setTimeout
                setTimeout(() => {
                    expect(esWithProjection.getEventStream.calls.count()).toEqual(1);
                    done();
                }, 10);
            });
        })

    })
})