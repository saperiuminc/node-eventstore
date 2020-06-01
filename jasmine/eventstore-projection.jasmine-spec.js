const EventStoreWithProjection = require('../lib/eventstore-projections/eventstore-projection');

describe('eventstore-projection tests', () => {
    let esWithProjection;
    let options;
    beforeEach(() => {
        options = {
            pollingMaxRevisions: 10,
            pollingTimeout: 0, // so that polling is immediate
            eventCallbackTimeout: 0
        };
        esWithProjection = new EventStoreWithProjection(options);

        esWithProjection.getLastEventAsStream = jasmine.createSpy('getLastEventAsStream', esWithProjection.getLastEventAsStream);
        esWithProjection.getLastEventAsStream.and.callFake((query, cb) => {
            cb();
        })

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
    });

    // TODO: will go back to project after subscribe
    xdescribe('project', () => {
        describe('validating params and output', () => {
            it('should validate the required param projectionParams', (done) => {
                esWithProjection.project(null, function(error) {
                    expect(error.message).toEqual('projectionParams is required');
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

        describe('adding to the projection stream', () => {
            it('should call Eventstore.getLastEventAsStream', (done) => {
                esWithProjection.project({
                    projectionId: 'the_projection_id',
                    query: {}
                }, function(error) {
                    expect(error).toBeUndefined();
                    expect(esWithProjection.getLastEventAsStream).toHaveBeenCalled();
                    done();
                });

            });
        })
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
})