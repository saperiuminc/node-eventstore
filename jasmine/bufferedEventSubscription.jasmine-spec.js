const BufferedEventSubscription = require('../lib/bufferedEventSubscription');
const PubSub = require('pubsub-js');
const DistributedSignal = require('./test-doubles/fakes/distributed-signal.fake');

xdescribe('bufferedEventSubscription', () => {
    let mockOptions;
    let mockES;
    let mockStreamBuffer;
    let mockCallback;
    let bufferedEventSubscription;
    let mockDistributedSignal;

    beforeEach(() => {
        mockES = jasmine.createSpyObj('mockES', ['getLastEvent', 'getEventStream']);
        mockStreamBuffer = jasmine.createSpyObj('mockStreamBuffer', ['getEventsInBufferAsStream', 'offerEvents']);
        mockDistributedSignal = new DistributedSignal();
        mockOptions = {
            es: mockES,
            distributedSignal: mockDistributedSignal,
            streamBuffer: mockStreamBuffer,
            query: {
                aggregate: 'mockAggregate',
                aggregateId: 'mockAggregateId',
                context: 'mockContext'
            },
            channel: 'mockContext.mockAggregate.mockAggregateId',
            eventCallbackTimeout: 250,
            pollingTimeout: 50,
            pollingMaxRevisions: 2
        };
        mockCallback = {
            onEventCallback: function(error, event, callback) {
                callback();
            },
            onErrorCallback: function() {}
        };
    });

    afterEach(() => {
        if (bufferedEventSubscription) {
            bufferedEventSubscription.deactivate();
        }
    });

    describe('subscribe', () => {
        it('should return the input subscriptionToken on initial subscribe, and not trigger onEventCallback and onErrorCallback when there are no events', () => {
            const mockEmptyEventStream = {
                events: []
            };
            mockES.getLastEvent.and.callFake((query, callback) => {
                callback(null, null);
            });
            mockES.getEventStream.and.callFake((query, revMin, revMax, callback) => {
                callback(null, mockEmptyEventStream);
            });
            mockStreamBuffer.getEventsInBufferAsStream.and.returnValue(mockEmptyEventStream);
            const mockToken = 'mockToken';
            const mockRevision = 0;
            spyOn(mockCallback, 'onEventCallback').and.callThrough();
            spyOn(mockCallback, 'onErrorCallback').and.callThrough();

            bufferedEventSubscription = new BufferedEventSubscription(mockOptions);
            const result = bufferedEventSubscription.subscribe(mockToken, mockRevision, mockCallback.onEventCallback, mockCallback.onErrorCallback);

            expect(result).toEqual(mockToken);
            expect(mockCallback.onEventCallback).not.toHaveBeenCalled();
            expect(mockCallback.onErrorCallback).not.toHaveBeenCalled();
        });

        describe('catch-up async behavior', () => {
            it('should call es.getLastEvent async on initial subscribe', (done) => {
                const mockEmptyEventStream = {
                    events: []
                };
                mockES.getLastEvent.and.callFake((query, callback) => {
                    callback(null, null);
                });
                mockES.getEventStream.and.callFake((query, revMin, revMax, callback) => {
                    callback(null, mockEmptyEventStream);
                });
                mockStreamBuffer.getEventsInBufferAsStream.and.returnValue(mockEmptyEventStream);
                const mockToken = 'mockToken';
                const mockRevision = 0;
                spyOn(mockCallback, 'onEventCallback').and.callThrough();
                spyOn(mockCallback, 'onErrorCallback').and.callThrough();
    
                bufferedEventSubscription = new BufferedEventSubscription(mockOptions);
                bufferedEventSubscription.subscribe(mockToken, mockRevision, mockCallback.onEventCallback, mockCallback.onErrorCallback);
    
                setTimeout(() => {
                    expect(mockES.getLastEvent).toHaveBeenCalledWith(mockOptions.query, jasmine.any(Function));
                    done();
                }, 25);
            }, 50);
    
            it('should call streamBuffer.getEventsInBufferAsStream async with proper revision range if there is a lastEvent from the es with a revision equal or greater than the input revision', (done) => {
                const mockEmptyEventStream = {
                    events: []
                };
                const mockLastEvent = {
                    streamRevision: 5
                };
                mockES.getLastEvent.and.callFake((query, callback) => {
                    callback(null, mockLastEvent);
                });
                mockES.getEventStream.and.callFake((query, revMin, revMax, callback) => {
                    callback(null, mockEmptyEventStream);
                });
                mockStreamBuffer.getEventsInBufferAsStream.and.returnValue(mockEmptyEventStream);
                const mockToken = 'mockToken';
                const mockRevision = 5;
                spyOn(mockCallback, 'onEventCallback').and.callThrough();
                spyOn(mockCallback, 'onErrorCallback').and.callThrough();
    
                bufferedEventSubscription = new BufferedEventSubscription(mockOptions);
                bufferedEventSubscription.subscribe(mockToken, mockRevision, mockCallback.onEventCallback, mockCallback.onErrorCallback);
    
                setTimeout(() => {
                    expect(mockStreamBuffer.getEventsInBufferAsStream).toHaveBeenCalledWith(mockRevision, mockRevision + mockOptions.pollingMaxRevisions);
                    done();
                }, 25);
            }, 50);
    
            it('should call streamBuffer.getEventsInBufferAsStream async with proper revision range if there is a lastEvent from the es and the input revision is greater than the lastEvent.streamRevision + 1', (done) => {
                const mockEmptyEventStream = {
                    events: []
                };
                const mockLastEvent = {
                    streamRevision: 5
                };
                mockES.getLastEvent.and.callFake((query, callback) => {
                    callback(null, mockLastEvent);
                });
                mockES.getEventStream.and.callFake((query, revMin, revMax, callback) => {
                    callback(null, mockEmptyEventStream);
                });
                mockStreamBuffer.getEventsInBufferAsStream.and.returnValue(mockEmptyEventStream);
                const mockToken = 'mockToken';
                const mockRevision = 10;
                spyOn(mockCallback, 'onEventCallback').and.callThrough();
                spyOn(mockCallback, 'onErrorCallback').and.callThrough();
    
                bufferedEventSubscription = new BufferedEventSubscription(mockOptions);
                bufferedEventSubscription.subscribe(mockToken, mockRevision, mockCallback.onEventCallback, mockCallback.onErrorCallback);
    
                const expectedMinRev = mockLastEvent.streamRevision + 1;
                const expectedMaxRev = expectedMinRev + mockOptions.pollingMaxRevisions;
    
                setTimeout(() => {
                    expect(mockStreamBuffer.getEventsInBufferAsStream).toHaveBeenCalledWith(expectedMinRev, expectedMaxRev);
                    done();
                }, 25);
            }, 50);
    
            it('should call streamBuffer.getEventsInBufferAsStream async with proper revision range if there is a lastEvent from the es and the input revision is less than to the lastEvent.streamRevision + 1', (done) => {
                const mockEmptyEventStream = {
                    events: []
                };
                const mockLastEvent = {
                    streamRevision: 5
                };
                mockES.getLastEvent.and.callFake((query, callback) => {
                    callback(null, mockLastEvent);
                });
                mockES.getEventStream.and.callFake((query, revMin, revMax, callback) => {
                    callback(null, mockEmptyEventStream);
                });
                mockStreamBuffer.getEventsInBufferAsStream.and.returnValue(mockEmptyEventStream);
                const mockToken = 'mockToken';
                const mockRevision = 3;
                spyOn(mockCallback, 'onEventCallback').and.callThrough();
                spyOn(mockCallback, 'onErrorCallback').and.callThrough();
    
                bufferedEventSubscription = new BufferedEventSubscription(mockOptions);
                bufferedEventSubscription.subscribe(mockToken, mockRevision, mockCallback.onEventCallback, mockCallback.onErrorCallback);
    
                const expectedMinRev = mockRevision;
                const expectedMaxRev = expectedMinRev + mockOptions.pollingMaxRevisions;
    
                setTimeout(() => {
                    expect(mockStreamBuffer.getEventsInBufferAsStream).toHaveBeenCalledWith(expectedMinRev, expectedMaxRev);
                    done();
                }, 25);
            }, 50);
    
            it('should call streamBuffer.getEventsInBufferAsStream async with proper revision range if there is a lastEvent from the es and the input revision is equal to the lastEvent.streamRevision + 1', (done) => {
                const mockEmptyEventStream = {
                    events: []
                };
                const mockLastEvent = {
                    streamRevision: 5
                };
                mockES.getLastEvent.and.callFake((query, callback) => {
                    callback(null, mockLastEvent);
                });
                mockES.getEventStream.and.callFake((query, revMin, revMax, callback) => {
                    callback(null, mockEmptyEventStream);
                });
                mockStreamBuffer.getEventsInBufferAsStream.and.returnValue(mockEmptyEventStream);
                const mockToken = 'mockToken';
                const mockRevision = 5;
                spyOn(mockCallback, 'onEventCallback').and.callThrough();
                spyOn(mockCallback, 'onErrorCallback').and.callThrough();
    
                bufferedEventSubscription = new BufferedEventSubscription(mockOptions);
                bufferedEventSubscription.subscribe(mockToken, mockRevision, mockCallback.onEventCallback, mockCallback.onErrorCallback);
    
                const expectedMinRev = mockRevision;
                const expectedMaxRev = expectedMinRev + mockOptions.pollingMaxRevisions;
    
                setTimeout(() => {
                    expect(mockStreamBuffer.getEventsInBufferAsStream).toHaveBeenCalledWith(expectedMinRev, expectedMaxRev);
                    done();
                }, 25);
            }, 50);
    
            it('should call streamBuffer.getEventsInBufferAsStream async with proper revision range if there is no lastEvent from the es', (done) => {
                const mockEmptyEventStream = {
                    events: []
                };
                mockES.getLastEvent.and.callFake((query, callback) => {
                    callback(null, null);
                });
                mockES.getEventStream.and.callFake((query, revMin, revMax, callback) => {
                    callback(null, mockEmptyEventStream);
                });
                mockStreamBuffer.getEventsInBufferAsStream.and.returnValue(mockEmptyEventStream);
                const mockToken = 'mockToken';
                const mockRevision = 5;
                spyOn(mockCallback, 'onEventCallback').and.callThrough();
                spyOn(mockCallback, 'onErrorCallback').and.callThrough();
    
                bufferedEventSubscription = new BufferedEventSubscription(mockOptions);
                bufferedEventSubscription.subscribe(mockToken, mockRevision, mockCallback.onEventCallback, mockCallback.onErrorCallback);
    
                const expectedMinRev = 0;
                const expectedMaxRev = expectedMinRev + mockOptions.pollingMaxRevisions;
    
                setTimeout(() => {
                    expect(mockStreamBuffer.getEventsInBufferAsStream).toHaveBeenCalledWith(expectedMinRev, expectedMaxRev);
                    done();
                }, 25);
            }, 50);

            it('should call streamBuffer.getEventsInBufferAsStream and es.getEventStream async, and emit the events to the subscriber if there are events in the buffer', (done) => {
                const mockEmptyEventStream = {
                    events: []
                };
                const mockEventStreams = [
                    {
                        events: [
                            { streamRevision: 0, payload: 'test0' },
                            { streamRevision: 1, payload: 'test1' },
                            { streamRevision: 2, payload: 'test2' },
                        ]
                    },
                    {
                        events: [
                            { streamRevision: 3, payload: 'test3' },
                            { streamRevision: 4, payload: 'test4' },
                            { streamRevision: 5, payload: 'test5' },
                        ]
                    }
                ]
                const mockLastEvent = {
                    streamRevision: 5
                };
                mockES.getLastEvent.and.callFake((query, callback) => {
                    callback(null, mockLastEvent);
                });
                mockES.getEventStream.and.callFake((query, revMin, revMax, callback) => {
                    callback(null, mockEmptyEventStream);
                });
                mockStreamBuffer.getEventsInBufferAsStream.and.callFake((min, max) => {
                    if (min === 0 && max === 2) {
                        return mockEventStreams[0];
                    } else if (min === 3 && max === 5) {
                        return mockEventStreams[1];
                    } else {
                        return mockEmptyEventStream;
                    }
                });
                const mockToken = 'mockToken';
                const mockRevision = 0;
                spyOn(mockCallback, 'onEventCallback').and.callThrough();
                spyOn(mockCallback, 'onErrorCallback').and.callThrough();
    
                bufferedEventSubscription = new BufferedEventSubscription(mockOptions);
                bufferedEventSubscription.subscribe(mockToken, mockRevision, mockCallback.onEventCallback, mockCallback.onErrorCallback);

                setTimeout(() => {
                    // First Catch-Up loop: Buffer Hit
                    expect(mockStreamBuffer.getEventsInBufferAsStream.calls.argsFor(0)).toEqual([0, 2]);
                    expect(mockES.getEventStream).not.toHaveBeenCalledWith(mockOptions.query, 0, 2, jasmine.any(Function));
                    expect(mockStreamBuffer.offerEvents).not.toHaveBeenCalled();
                    expect(mockCallback.onEventCallback.calls.argsFor(0)).toEqual([null, mockEventStreams[0].events[0], jasmine.any(Function)]);
                    expect(mockCallback.onEventCallback.calls.argsFor(1)).toEqual([null, mockEventStreams[0].events[1], jasmine.any(Function)]);
                    expect(mockCallback.onEventCallback.calls.argsFor(2)).toEqual([null, mockEventStreams[0].events[2], jasmine.any(Function)]);

                    // Second Catch-Up loop: Buffer Hit
                    expect(mockStreamBuffer.getEventsInBufferAsStream.calls.argsFor(1)).toEqual([3, 5]);
                    expect(mockES.getEventStream).not.toHaveBeenCalledWith(mockOptions.query, 3, 5, jasmine.any(Function));
                    expect(mockStreamBuffer.offerEvents).not.toHaveBeenCalled();
                    expect(mockCallback.onEventCallback.calls.argsFor(3)).toEqual([null, mockEventStreams[1].events[0], jasmine.any(Function)]);
                    expect(mockCallback.onEventCallback.calls.argsFor(4)).toEqual([null, mockEventStreams[1].events[1], jasmine.any(Function)]);
                    expect(mockCallback.onEventCallback.calls.argsFor(5)).toEqual([null, mockEventStreams[1].events[2], jasmine.any(Function)]);
                    
                    // Third Catch-Up loop: Buffer Miss, ES Miss
                    expect(mockStreamBuffer.getEventsInBufferAsStream.calls.argsFor(2)).toEqual([6, 8]);
                    expect(mockES.getEventStream.calls.argsFor(0)).toEqual([mockOptions.query, 6, 8, jasmine.any(Function)]);
                    expect(mockCallback.onEventCallback.calls.count()).toEqual(6);
                    done();
                }, 25);
            }, 50);

            it('should call streamBuffer.getEventsInBufferAsStream, es.getEventStream async and streambuffer.offerEvents, and emit the events to the subscriber if there are no events in the buffer and there are events in the eventstore', (done) => {
                const mockEmptyEventStream = {
                    events: []
                };
                const mockEventStreams = [
                    {
                        events: [
                            { streamRevision: 0, payload: 'test0' },
                            { streamRevision: 1, payload: 'test1' },
                            { streamRevision: 2, payload: 'test2' },
                        ]
                    },
                    {
                        events: [
                            { streamRevision: 3, payload: 'test3' },
                            { streamRevision: 4, payload: 'test4' },
                            { streamRevision: 5, payload: 'test5' },
                        ]
                    }
                ]
                const mockLastEvent = {
                    streamRevision: 5
                };
                mockES.getLastEvent.and.callFake((query, callback) => {
                    callback(null, mockLastEvent);
                });
                mockES.getEventStream.and.callFake((query, revMin, revMax, callback) => {
                    if (revMin === 0 && revMax === 2) {
                        return callback(null, mockEventStreams[0]);
                    } else if (revMin === 3 && revMax === 5) {
                        return callback(null, mockEventStreams[1]);
                    } else {
                        return callback(null, mockEmptyEventStream);
                    }
                });
                mockStreamBuffer.getEventsInBufferAsStream.and.returnValue(mockEmptyEventStream);
                mockStreamBuffer.offerEvents.and.callThrough();
                const mockToken = 'mockToken';
                const mockRevision = 0;
                spyOn(mockCallback, 'onEventCallback').and.callThrough();
                spyOn(mockCallback, 'onErrorCallback').and.callThrough();
    
                bufferedEventSubscription = new BufferedEventSubscription(mockOptions);
                bufferedEventSubscription.subscribe(mockToken, mockRevision, mockCallback.onEventCallback, mockCallback.onErrorCallback);

                setTimeout(() => {
                    // First Catch-Up loop: Buffer Miss, ES Hit
                    expect(mockStreamBuffer.getEventsInBufferAsStream.calls.argsFor(0)).toEqual([0, 2]);
                    expect(mockES.getEventStream.calls.argsFor(0)).toEqual([mockOptions.query, 0, 2, jasmine.any(Function)]);
                    expect(mockStreamBuffer.offerEvents.calls.argsFor(0)).toEqual([mockEventStreams[0].events]);
                    expect(mockCallback.onEventCallback.calls.argsFor(0)).toEqual([null, mockEventStreams[0].events[0], jasmine.any(Function)]);
                    expect(mockCallback.onEventCallback.calls.argsFor(1)).toEqual([null, mockEventStreams[0].events[1], jasmine.any(Function)]);
                    expect(mockCallback.onEventCallback.calls.argsFor(2)).toEqual([null, mockEventStreams[0].events[2], jasmine.any(Function)]);

                    // Second Catch-Up loop: Buffer Miss, ES Hit
                    expect(mockStreamBuffer.getEventsInBufferAsStream.calls.argsFor(1)).toEqual([3, 5]);
                    expect(mockES.getEventStream.calls.argsFor(1)).toEqual([mockOptions.query, 3, 5, jasmine.any(Function)]);
                    expect(mockStreamBuffer.offerEvents.calls.argsFor(1)).toEqual([mockEventStreams[1].events]);
                    expect(mockCallback.onEventCallback.calls.argsFor(3)).toEqual([null, mockEventStreams[1].events[0], jasmine.any(Function)]);
                    expect(mockCallback.onEventCallback.calls.argsFor(4)).toEqual([null, mockEventStreams[1].events[1], jasmine.any(Function)]);
                    expect(mockCallback.onEventCallback.calls.argsFor(5)).toEqual([null, mockEventStreams[1].events[2], jasmine.any(Function)]);
                    
                    // Third Catch-Up loop: Buffer Miss, ES Miss
                    expect(mockStreamBuffer.getEventsInBufferAsStream.calls.argsFor(2)).toEqual([6, 8]);
                    expect(mockES.getEventStream.calls.argsFor(2)).toEqual([mockOptions.query, 6, 8, jasmine.any(Function)]);
                    expect(mockCallback.onEventCallback.calls.count()).toEqual(6);
                    done();
                }, 25);
            }, 50);
            
            it('should call streamBuffer.getEventsInBufferAsStream and es.getEventStream async, and emit no events if there are no events on both the buffer and the eventstore', (done) => {
                const mockEmptyEventStream = {
                    events: []
                };
                mockES.getLastEvent.and.callFake((query, callback) => {
                    callback(null, null);
                });
                mockES.getEventStream.and.callFake((query, revMin, revMax, callback) => {
                    callback(null, mockEmptyEventStream);
                });
                mockStreamBuffer.getEventsInBufferAsStream.and.returnValue(mockEmptyEventStream);
                mockStreamBuffer.offerEvents.and.callThrough();
                const mockToken = 'mockToken';
                const mockRevision = 0;
                spyOn(mockCallback, 'onEventCallback').and.callThrough();
                spyOn(mockCallback, 'onErrorCallback').and.callThrough();
    
                bufferedEventSubscription = new BufferedEventSubscription(mockOptions);
                bufferedEventSubscription.subscribe(mockToken, mockRevision, mockCallback.onEventCallback, mockCallback.onErrorCallback);

                setTimeout(() => {
                    // First Catch-Up loop: Buffer Miss, ES Miss
                    expect(mockStreamBuffer.getEventsInBufferAsStream.calls.argsFor(0)).toEqual([0, 2]);
                    expect(mockES.getEventStream.calls.argsFor(0)).toEqual([mockOptions.query, 0, 2, jasmine.any(Function)]);
                    expect(mockCallback.onEventCallback).not.toHaveBeenCalled();
                    done();
                }, 25);
            }, 50);

            it('should call onErrorCallback with the proper error if es.getLastEvent threw an error', (done) => {
                const mockError = new Error('test error');
                mockES.getLastEvent.and.callFake((query, callback) => {
                    callback(mockError, null);
                });
                const mockToken = 'mockToken';
                const mockRevision = 0;
                spyOn(mockCallback, 'onEventCallback').and.callThrough();
                spyOn(mockCallback, 'onErrorCallback').and.callThrough();
    
                bufferedEventSubscription = new BufferedEventSubscription(mockOptions);
                bufferedEventSubscription.subscribe(mockToken, mockRevision, mockCallback.onEventCallback, mockCallback.onErrorCallback);
    
                setTimeout(() => {
                    expect(mockES.getLastEvent).toHaveBeenCalledWith(mockOptions.query, jasmine.any(Function));
                    expect(mockCallback.onErrorCallback).toHaveBeenCalledWith(mockError);
                    done();
                }, 25);
            }, 50);

            it('should call onErrorCallback with the proper error if es.getEventStream threw an error', (done) => {
                const mockError = new Error('test error');
                const mockEmptyEventStream = {
                    events: []
                };
                mockES.getLastEvent.and.callFake((query, callback) => {
                    callback(null, null);
                });
                mockES.getEventStream.and.callFake((query, revMin, revMax, callback) => {
                    callback(mockError, null);
                });
                mockStreamBuffer.getEventsInBufferAsStream.and.returnValue(mockEmptyEventStream);
                mockStreamBuffer.offerEvents.and.callThrough();
                const mockToken = 'mockToken';
                const mockRevision = 0;
                spyOn(mockCallback, 'onEventCallback').and.callThrough();
                spyOn(mockCallback, 'onErrorCallback').and.callThrough();
    
                bufferedEventSubscription = new BufferedEventSubscription(mockOptions);
                bufferedEventSubscription.subscribe(mockToken, mockRevision, mockCallback.onEventCallback, mockCallback.onErrorCallback);

                setTimeout(() => {
                    // First Catch-Up loop: Buffer Miss, ES Miss
                    expect(mockStreamBuffer.getEventsInBufferAsStream.calls.argsFor(0)).toEqual([0, 2]);
                    expect(mockES.getEventStream.calls.argsFor(0)).toEqual([mockOptions.query, 0, 2, jasmine.any(Function)]);
                    expect(mockCallback.onErrorCallback).toHaveBeenCalledWith(mockError);
                    done();
                }, 25);
            }, 50);
            
            it('should call onErrorCallback with the proper error if es.getEventStream initially returned no error but eventually threw an error during the catch-up phase', (done) => {
                const mockError = new Error('test error');
                const mockEmptyEventStream = {
                    events: []
                };
                const mockEventStreams = [
                    {
                        events: [
                            { streamRevision: 0, payload: 'test0' },
                            { streamRevision: 1, payload: 'test1' },
                            { streamRevision: 2, payload: 'test2' },
                        ]
                    },
                    {
                        events: [
                            { streamRevision: 3, payload: 'test3' },
                            { streamRevision: 4, payload: 'test4' },
                            { streamRevision: 5, payload: 'test5' },
                        ]
                    }
                ]
                const mockLastEvent = {
                    streamRevision: 5
                };
                mockES.getLastEvent.and.callFake((query, callback) => {
                    callback(null, mockLastEvent);
                });
                mockES.getEventStream.and.callFake((query, revMin, revMax, callback) => {
                    if (revMin === 0 && revMax === 2) {
                        return callback(null, mockEventStreams[0]);
                    } else if (revMin === 3 && revMax === 5) {
                        return callback(null, mockEventStreams[1]);
                    } else {
                        return callback(mockError, null);
                    }
                });
                mockStreamBuffer.getEventsInBufferAsStream.and.returnValue(mockEmptyEventStream);
                mockStreamBuffer.offerEvents.and.callThrough();
                const mockToken = 'mockToken';
                const mockRevision = 0;
                spyOn(mockCallback, 'onEventCallback').and.callThrough();
                spyOn(mockCallback, 'onErrorCallback').and.callThrough();
    
                bufferedEventSubscription = new BufferedEventSubscription(mockOptions);
                bufferedEventSubscription.subscribe(mockToken, mockRevision, mockCallback.onEventCallback, mockCallback.onErrorCallback);

                setTimeout(() => {
                    // First Catch-Up loop: Buffer Miss, ES Hit
                    expect(mockStreamBuffer.getEventsInBufferAsStream.calls.argsFor(0)).toEqual([0, 2]);
                    expect(mockES.getEventStream.calls.argsFor(0)).toEqual([mockOptions.query, 0, 2, jasmine.any(Function)]);
                    expect(mockStreamBuffer.offerEvents.calls.argsFor(0)).toEqual([mockEventStreams[0].events]);
                    expect(mockCallback.onEventCallback.calls.argsFor(0)).toEqual([null, mockEventStreams[0].events[0], jasmine.any(Function)]);
                    expect(mockCallback.onEventCallback.calls.argsFor(1)).toEqual([null, mockEventStreams[0].events[1], jasmine.any(Function)]);
                    expect(mockCallback.onEventCallback.calls.argsFor(2)).toEqual([null, mockEventStreams[0].events[2], jasmine.any(Function)]);

                    // Second Catch-Up loop: Buffer Miss, ES Hit
                    expect(mockStreamBuffer.getEventsInBufferAsStream.calls.argsFor(1)).toEqual([3, 5]);
                    expect(mockES.getEventStream.calls.argsFor(1)).toEqual([mockOptions.query, 3, 5, jasmine.any(Function)]);
                    expect(mockStreamBuffer.offerEvents.calls.argsFor(1)).toEqual([mockEventStreams[1].events]);
                    expect(mockCallback.onEventCallback.calls.argsFor(3)).toEqual([null, mockEventStreams[1].events[0], jasmine.any(Function)]);
                    expect(mockCallback.onEventCallback.calls.argsFor(4)).toEqual([null, mockEventStreams[1].events[1], jasmine.any(Function)]);
                    expect(mockCallback.onEventCallback.calls.argsFor(5)).toEqual([null, mockEventStreams[1].events[2], jasmine.any(Function)]);
                    
                    // Third Catch-Up loop: Buffer Miss, ES Error
                    expect(mockStreamBuffer.getEventsInBufferAsStream.calls.argsFor(2)).toEqual([6, 8]);
                    expect(mockES.getEventStream.calls.argsFor(2)).toEqual([mockOptions.query, 6, 8, jasmine.any(Function)]);
                    expect(mockCallback.onEventCallback.calls.count()).toEqual(6);
                    expect(mockCallback.onErrorCallback.calls.argsFor(0)).toEqual([mockError]);
                    expect(mockCallback.onErrorCallback.calls.count()).toEqual(1);
                    done();
                }, 25);
            }, 50);
        });

        describe('internal bufferedEventSubscription', () => {
            it('should start an internal bufferedEventSubscription that calls es.getEventStream per loop iteration', (done) => {
                let numExecutions = 0;
                const mockEmptyEventStream = {
                    events: []
                };
                mockES.getLastEvent.and.callFake((query, callback) => {
                    callback(null, null);
                });
                mockES.getEventStream.and.callFake((query, revMin, revMax, callback) => {
                    numExecutions++;
                    callback(null, mockEmptyEventStream);
                    if (numExecutions === 6) {
                        expect(mockES.getEventStream.calls.argsFor(0)).toEqual([mockOptions.query, 0, 2, jasmine.any(Function)]);
                        expect(mockStreamBuffer.getEventsInBufferAsStream.calls.argsFor(0)).toEqual([0, 2]);
                        
                        // Polling Loops
                        expect(mockStreamBuffer.getEventsInBufferAsStream.calls.count()).toEqual(1);
                        expect(mockES.getEventStream.calls.count()).toEqual(6);
                        expect(mockES.getEventStream.calls.argsFor(1)).toEqual([mockOptions.query, 0, 2, jasmine.any(Function)]);
                        expect(mockES.getEventStream.calls.argsFor(2)).toEqual([mockOptions.query, 0, 2, jasmine.any(Function)]);
                        expect(mockES.getEventStream.calls.argsFor(3)).toEqual([mockOptions.query, 0, 2, jasmine.any(Function)]);
                        expect(mockES.getEventStream.calls.argsFor(4)).toEqual([mockOptions.query, 0, 2, jasmine.any(Function)]);
                        expect(mockES.getEventStream.calls.argsFor(5)).toEqual([mockOptions.query, 0, 2, jasmine.any(Function)]);
                        expect(mockCallback.onEventCallback).not.toHaveBeenCalled();
                        done();
                    }
                });
                mockStreamBuffer.getEventsInBufferAsStream.and.returnValue(mockEmptyEventStream);
                mockStreamBuffer.offerEvents.and.callThrough();
                const mockToken = 'mockToken';
                const mockRevision = 0;
                spyOn(mockCallback, 'onEventCallback').and.callThrough();
                spyOn(mockCallback, 'onErrorCallback').and.callThrough();
    
                bufferedEventSubscription = new BufferedEventSubscription(mockOptions);
                bufferedEventSubscription.subscribe(mockToken, mockRevision, mockCallback.onEventCallback, mockCallback.onErrorCallback);
            }, 300);

            it('should start an internal bufferedEventSubscription that calls es.getEventStream and streambuffer.offerEvents once per poll whenever a new event is retrieved from the ES, and then pass the correct revision range to getEventStream after processing a set of events from a stream', (done) => {
                let numExecutions = 0;
                const mockEmptyEventStream = {
                    events: []
                };
                mockES.getLastEvent.and.callFake((query, callback) => {
                    callback(null, null);
                });
                const mockNewEventStreams = [
                    {
                        events: [
                            {
                                streamRevision: 0,
                                payload: 'testNewEvent0'
                            }
                        ]
                    }, {
                        events: [
                            {
                                streamRevision: 1,
                                payload: 'testNewEvent1'
                            }
                        ]
                    }
                ];
                mockES.getEventStream.and.callFake((query, revMin, revMax, callback) => {
                    numExecutions++;
                    if (numExecutions === 2) {
                        callback(null, mockNewEventStreams[0]);

                        expect(mockStreamBuffer.getEventsInBufferAsStream.calls.argsFor(0)).toEqual([0, 2]);
                        expect(mockStreamBuffer.getEventsInBufferAsStream.calls.count()).toEqual(1);
                        expect(mockES.getEventStream.calls.argsFor(0)).toEqual([mockOptions.query, 0, 2, jasmine.any(Function)]);
                        expect(mockES.getEventStream.calls.argsFor(1)).toEqual([mockOptions.query, 0, 2, jasmine.any(Function)]);
                    } else if (numExecutions === 3) {
                        callback(null, mockNewEventStreams[1]);

                        // Assert stream buffer and callback invoked
                        expect(mockStreamBuffer.offerEvents.calls.argsFor(0)).toEqual([mockNewEventStreams[0].events]);
                        expect(mockCallback.onEventCallback.calls.argsFor(0)).toEqual([null, mockNewEventStreams[0].events[0], jasmine.any(Function)]);

                        // Assert Next Loop w/ updated revision range
                        expect(mockES.getEventStream.calls.argsFor(2)).toEqual([mockOptions.query, 1, 3, jasmine.any(Function)]);
                        expect(mockES.getEventStream.calls.count()).toEqual(3);
                    } else if (numExecutions === 4) {
                        callback(null, mockEmptyEventStream);

                        // Assert stream buffer and callback invoked
                        expect(mockStreamBuffer.offerEvents.calls.argsFor(1)).toEqual([mockNewEventStreams[1].events]);
                        expect(mockCallback.onEventCallback.calls.argsFor(1)).toEqual([null, mockNewEventStreams[1].events[0], jasmine.any(Function)]);

                        // Assert Next Loop w/ updated revision range
                        expect(mockES.getEventStream.calls.argsFor(3)).toEqual([mockOptions.query, 2, 4, jasmine.any(Function)]);
                        expect(mockES.getEventStream.calls.count()).toEqual(4);
                        done();
                    } else {
                        callback(null, mockEmptyEventStream);
                    }
                });
                mockStreamBuffer.getEventsInBufferAsStream.and.returnValue(mockEmptyEventStream);
                mockStreamBuffer.offerEvents.and.callThrough();
                const mockToken = 'mockToken';
                const mockRevision = 0;
                spyOn(mockCallback, 'onEventCallback').and.callThrough();
                spyOn(mockCallback, 'onErrorCallback').and.callThrough();
    
                bufferedEventSubscription = new BufferedEventSubscription(mockOptions);
                bufferedEventSubscription.subscribe(mockToken, mockRevision, mockCallback.onEventCallback, mockCallback.onErrorCallback);
            }, 300);

            it('should emit to multiple subscribers whenever an event is retrieved from the eventstore during polling', (done) => {
                let numExecutions = 0;
                const mockEmptyEventStream = {
                    events: []
                };
                mockES.getLastEvent.and.callFake((query, callback) => {
                    callback(null, null);
                });
                const mockNewEventStreams = [
                    {
                        events: [
                            {
                                streamRevision: 0,
                                payload: 'testNewEvent0'
                            }
                        ]
                    }, {
                        events: [
                            {
                                streamRevision: 1,
                                payload: 'testNewEvent1'
                            }
                        ]
                    }
                ];
                const mockCallbacks = [
                    {
                        onEventCallback: function(error, event, callback) {
                            callback();
                        },
                        onErrorCallback: function() {}
                    },
                    {
                        onEventCallback: function(error, event, callback) {
                            callback();
                        },
                        onErrorCallback: function() {}
                    }
                ];
                spyOn(mockCallbacks[0], 'onEventCallback').and.callThrough();
                spyOn(mockCallbacks[1], 'onEventCallback').and.callThrough();

                mockES.getEventStream.and.callFake((query, revMin, revMax, callback) => {
                    numExecutions++;
                    if (numExecutions === 3) {
                        callback(null, mockNewEventStreams[0]);
                    } else if (numExecutions === 4) {
                        callback(null, mockNewEventStreams[1]);

                        // Assert stream buffer and callbacks invoked
                        expect(mockStreamBuffer.offerEvents.calls.argsFor(0)).toEqual([mockNewEventStreams[0].events]);
                        expect(mockCallbacks[0].onEventCallback.calls.argsFor(0)).toEqual([null, mockNewEventStreams[0].events[0], jasmine.any(Function)]);
                        expect(mockCallbacks[1].onEventCallback.calls.argsFor(0)).toEqual([null, mockNewEventStreams[0].events[0], jasmine.any(Function)]);

                    } else if (numExecutions === 5) {
                        callback(null, mockEmptyEventStream);

                        // Assert stream buffer and callbacks invoked
                        expect(mockStreamBuffer.offerEvents.calls.argsFor(1)).toEqual([mockNewEventStreams[1].events]);
                        expect(mockCallbacks[0].onEventCallback.calls.argsFor(1)).toEqual([null, mockNewEventStreams[1].events[0], jasmine.any(Function)]);
                        expect(mockCallbacks[1].onEventCallback.calls.argsFor(1)).toEqual([null, mockNewEventStreams[1].events[0], jasmine.any(Function)]);

                        done();
                    } else {
                        callback(null, mockEmptyEventStream);
                    }
                });
                mockStreamBuffer.getEventsInBufferAsStream.and.returnValue(mockEmptyEventStream);
                mockStreamBuffer.offerEvents.and.callThrough();
                const mockRevision = 0;

                bufferedEventSubscription = new BufferedEventSubscription(mockOptions);
                bufferedEventSubscription.subscribe('mockToken', mockRevision, mockCallbacks[0].onEventCallback, mockCallbacks[0].onErrorCallback);
                bufferedEventSubscription.subscribe('mockToken2', mockRevision, mockCallbacks[1].onEventCallback, mockCallbacks[1].onErrorCallback);
            }, 300);

            it('should support incoming subscribers even if the bufferedEventSubscription is already active, and emit to multiple subscribers immediately after an event is published via PubSub', (done) => {
                let numExecutions = 0;
                const mockOptionsWithHighTimeout = {
                    es: mockES,
                    distributedSignal: mockDistributedSignal,
                    streamBuffer: mockStreamBuffer,
                    query: {
                        aggregate: 'mockAggregate',
                        aggregateId: 'mockAggregateId',
                        context: 'mockContext'
                    },
                    channel: 'mockContext.mockAggregate.mockAggregateId',
                    eventCallbackTimeout: 250,
                    pollingTimeout: 1000,
                    pollingMaxRevisions: 2
                };
                const mockEmptyEventStream = {
                    events: []
                };
                mockES.getLastEvent.and.callFake((query, callback) => {
                    callback(null, null);
                });
                const mockNewEventStreams = [
                    {
                        events: [
                            {
                                streamRevision: 0,
                                payload: 'testNewEvent0'
                            }
                        ]
                    }, {
                        events: [
                            {
                                streamRevision: 1,
                                payload: 'testNewEvent1'
                            }
                        ]
                    }
                ];
                const mockPublishedEvents = [
                    {
                        revision: 0,
                        payload: 'testNewEvent0'
                    },
                    {
                        revision: 1,
                        payload: 'testNewEvent1'
                    }
                ];
                const mockCallbacks = [
                    {
                        onEventCallback: function(error, event, callback) {
                            callback();
                        },
                        onErrorCallback: function() {}
                    },
                    {
                        onEventCallback: function(error, event, callback) {
                            callback();
                        },
                        onErrorCallback: function() {}
                    }
                ];
                spyOn(mockCallbacks[0], 'onEventCallback').and.callThrough();
                spyOn(mockCallbacks[1], 'onEventCallback').and.callThrough();

                mockES.getEventStream.and.callFake((query, revMin, revMax, callback) => {
                    numExecutions++;
                    if (numExecutions === 1) {
                        callback(null, mockEmptyEventStream);
                        setTimeout(() => {
                            mockDistributedSignal.signal(mockOptionsWithHighTimeout.channel);

                            // Add an additional subscriber
                            bufferedEventSubscription.subscribe('mockToken2', mockRevision, mockCallbacks[1].onEventCallback, mockCallbacks[1].onErrorCallback);
                        }, 25);
                    } else if (numExecutions === 2) {
                        callback(null, mockNewEventStreams[0]);
                        setTimeout(() => {
                            mockDistributedSignal.signal(mockOptionsWithHighTimeout.channel);
                        }, 25);
                    } else if (numExecutions === 3) {
                        callback(null, mockNewEventStreams[1]);

                        setTimeout(() => {
                            // Assert stream buffer and callback invoked
                            expect(mockStreamBuffer.offerEvents.calls.argsFor(0)).toEqual([mockNewEventStreams[0].events]);
                            expect(mockCallbacks[0].onEventCallback.calls.argsFor(0)).toEqual([null, mockNewEventStreams[0].events[0], jasmine.any(Function)]);

                            // Assert stream buffer and callbacks invoked
                            expect(mockStreamBuffer.offerEvents.calls.argsFor(1)).toEqual([mockNewEventStreams[1].events]);
                            expect(mockCallbacks[0].onEventCallback.calls.argsFor(1)).toEqual([null, mockNewEventStreams[1].events[0], jasmine.any(Function)]);

                            // Assert callback invoked for additional subscriber
                            expect(mockCallbacks[1].onEventCallback.calls.argsFor(0)).toEqual([null, mockNewEventStreams[0].events[0], jasmine.any(Function)]);
                            expect(mockCallbacks[1].onEventCallback.calls.argsFor(1)).toEqual([null, mockNewEventStreams[1].events[0], jasmine.any(Function)]);
                            done();
                        }, 25);
                    } else {
                        callback(null, mockEmptyEventStream);
                    }
                });
                mockStreamBuffer.getEventsInBufferAsStream.and.returnValue(mockEmptyEventStream);
                mockStreamBuffer.offerEvents.and.callThrough();
                const mockRevision = 0;

                bufferedEventSubscription = new BufferedEventSubscription(mockOptionsWithHighTimeout);
                bufferedEventSubscription.subscribe('mockToken', mockRevision, mockCallbacks[0].onEventCallback, mockCallbacks[0].onErrorCallback);
            }, 100);

            it('should not throw an error and continue with the polling loop even if at least one of the event callbacks threw an error', (done) => {
                let numExecutions = 0;
                const mockOptionsWithHighTimeout = {
                    es: mockES,
                    distributedSignal: mockDistributedSignal,
                    streamBuffer: mockStreamBuffer,
                    query: {
                        aggregate: 'mockAggregate',
                        aggregateId: 'mockAggregateId',
                        context: 'mockContext'
                    },
                    channel: 'mockContext.mockAggregate.mockAggregateId',
                    eventCallbackTimeout: 250,
                    pollingTimeout: 1000,
                    pollingMaxRevisions: 2
                };
                const mockEmptyEventStream = {
                    events: []
                };
                mockES.getLastEvent.and.callFake((query, callback) => {
                    callback(null, null);
                });
                const mockNewEventStreams = [
                    {
                        events: [
                            {
                                streamRevision: 0,
                                payload: 'testNewEvent0'
                            }
                        ]
                    }, {
                        events: [
                            {
                                streamRevision: 1,
                                payload: 'testNewEvent1'
                            }
                        ]
                    }
                ];
                const mockPublishedEvents = [
                    {
                        revision: 0,
                        payload: 'testNewEvent0'
                    },
                    {
                        revision: 1,
                        payload: 'testNewEvent1'
                    }
                ];
                const mockCallbacks = [
                    {
                        onEventCallback: function(error, event, callback) {
                            callback();
                        },
                        onErrorCallback: function() {}
                    },
                    {
                        onEventCallback: function(error, event, callback) {
                            callback();
                        },
                        onErrorCallback: function() {}
                    }
                ];
                spyOn(mockCallbacks[0], 'onEventCallback').and.callThrough();
                spyOn(mockCallbacks[1], 'onEventCallback').and.callFake((error, event, callback) => {
                    callback(new Error('test error'));
                });

                mockES.getEventStream.and.callFake((query, revMin, revMax, callback) => {
                    numExecutions++;
                    if (numExecutions === 2) {
                        callback(null, mockEmptyEventStream);
                        setTimeout(() => {
                            mockDistributedSignal.signal(mockOptionsWithHighTimeout.channel);
                        }, 25);
                    } else if (numExecutions === 3) {
                        callback(null, mockNewEventStreams[0]);
                        setTimeout(() => {
                            mockDistributedSignal.signal(mockOptionsWithHighTimeout.channel);
                        }, 25);
                    } else if (numExecutions === 4) {
                        callback(null, mockNewEventStreams[1]);

                        setTimeout(() => {
                            // Assert stream buffer and callbacks invoked
                            expect(mockStreamBuffer.offerEvents.calls.argsFor(0)).toEqual([mockNewEventStreams[0].events]);
                            expect(mockCallbacks[0].onEventCallback.calls.argsFor(0)).toEqual([null, mockNewEventStreams[0].events[0], jasmine.any(Function)]);
                            expect(mockCallbacks[1].onEventCallback.calls.argsFor(0)).toEqual([null, mockNewEventStreams[0].events[0], jasmine.any(Function)]);

                            // Assert stream buffer and callbacks invoked
                            expect(mockStreamBuffer.offerEvents.calls.argsFor(1)).toEqual([mockNewEventStreams[1].events]);
                            expect(mockCallbacks[0].onEventCallback.calls.argsFor(1)).toEqual([null, mockNewEventStreams[1].events[0], jasmine.any(Function)]);
                            expect(mockCallbacks[1].onEventCallback.calls.argsFor(1)).toEqual([null, mockNewEventStreams[1].events[0], jasmine.any(Function)]);
                            done();
                        }, 25);
                    } else {
                        callback(null, mockEmptyEventStream);
                    }
                });
                mockStreamBuffer.getEventsInBufferAsStream.and.returnValue(mockEmptyEventStream);
                mockStreamBuffer.offerEvents.and.callThrough();
                const mockRevision = 0;

                bufferedEventSubscription = new BufferedEventSubscription(mockOptionsWithHighTimeout);
                bufferedEventSubscription.subscribe('mockToken', mockRevision, mockCallbacks[0].onEventCallback, mockCallbacks[0].onErrorCallback);
                bufferedEventSubscription.subscribe('mockToken2', mockRevision, mockCallbacks[1].onEventCallback, mockCallbacks[1].onErrorCallback);
            }, 100);

            it('should not throw an error and continue with the polling loop even if at least one of the event callbacks exceeded the callback timeout', (done) => {
                let numExecutions = 0;
                const mockOptionsWithHighTimeout = {
                    es: mockES,
                    distributedSignal: mockDistributedSignal,
                    streamBuffer: mockStreamBuffer,
                    query: {
                        aggregate: 'mockAggregate',
                        aggregateId: 'mockAggregateId',
                        context: 'mockContext'
                    },
                    channel: 'mockContext.mockAggregate.mockAggregateId',
                    eventCallbackTimeout: 10,
                    pollingTimeout: 1000,
                    pollingMaxRevisions: 2
                };
                const mockEmptyEventStream = {
                    events: []
                };
                mockES.getLastEvent.and.callFake((query, callback) => {
                    callback(null, null);
                });
                const mockNewEventStreams = [
                    {
                        events: [
                            {
                                streamRevision: 0,
                                payload: 'testNewEvent0'
                            }
                        ]
                    }, {
                        events: [
                            {
                                streamRevision: 1,
                                payload: 'testNewEvent1'
                            }
                        ]
                    }
                ];
                const mockPublishedEvents = [
                    {
                        revision: 0,
                        payload: 'testNewEvent0'
                    },
                    {
                        revision: 1,
                        payload: 'testNewEvent1'
                    }
                ];
                const mockCallbacks = [
                    {
                        onEventCallback: function(error, event, callback) {
                            callback();
                        },
                        onErrorCallback: function() {}
                    },
                    {
                        onEventCallback: function() {
                            // Do not invoke callback to force timeout
                        },
                        onErrorCallback: function() {}
                    }
                ];
                spyOn(mockCallbacks[0], 'onEventCallback').and.callThrough();
                spyOn(mockCallbacks[1], 'onEventCallback').and.callThrough();

                mockES.getEventStream.and.callFake((query, revMin, revMax, callback) => {
                    numExecutions++;
                    if (numExecutions === 2) {
                        callback(null, mockEmptyEventStream);
                        setTimeout(() => {
                            mockDistributedSignal.signal(mockOptionsWithHighTimeout.channel);
                        }, 25);
                    } else if (numExecutions === 3) {
                        callback(null, mockNewEventStreams[0]);
                        setTimeout(() => {
                            mockDistributedSignal.signal(mockOptionsWithHighTimeout.channel);
                        }, 25);
                    } else if (numExecutions === 4) {
                        callback(null, mockNewEventStreams[1]);

                        setTimeout(() => {
                            // Assert stream buffer and callbacks invoked
                            expect(mockStreamBuffer.offerEvents.calls.argsFor(0)).toEqual([mockNewEventStreams[0].events]);
                            expect(mockCallbacks[0].onEventCallback.calls.argsFor(0)).toEqual([null, mockNewEventStreams[0].events[0], jasmine.any(Function)]);
                            expect(mockCallbacks[1].onEventCallback.calls.argsFor(0)).toEqual([null, mockNewEventStreams[0].events[0], jasmine.any(Function)]);

                            // Assert stream buffer and callbacks invoked
                            expect(mockStreamBuffer.offerEvents.calls.argsFor(1)).toEqual([mockNewEventStreams[1].events]);
                            expect(mockCallbacks[0].onEventCallback.calls.argsFor(1)).toEqual([null, mockNewEventStreams[1].events[0], jasmine.any(Function)]);
                            expect(mockCallbacks[1].onEventCallback.calls.argsFor(1)).toEqual([null, mockNewEventStreams[1].events[0], jasmine.any(Function)]);
                            done();
                        }, 25);
                    } else {
                        callback(null, mockEmptyEventStream);
                    }
                });
                mockStreamBuffer.getEventsInBufferAsStream.and.returnValue(mockEmptyEventStream);
                mockStreamBuffer.offerEvents.and.callThrough();
                const mockRevision = 0;

                bufferedEventSubscription = new BufferedEventSubscription(mockOptionsWithHighTimeout);
                bufferedEventSubscription.subscribe('mockToken', mockRevision, mockCallbacks[0].onEventCallback, mockCallbacks[0].onErrorCallback);
                bufferedEventSubscription.subscribe('mockToken2', mockRevision, mockCallbacks[1].onEventCallback, mockCallbacks[1].onErrorCallback);
            }, 100);

            it('should not throw an error and continue with the polling loop even if at least one of the event callbacks is undefined', (done) => {
                let numExecutions = 0;
                const mockOptionsWithHighTimeout = {
                    es: mockES,
                    distributedSignal: mockDistributedSignal,
                    streamBuffer: mockStreamBuffer,
                    query: {
                        aggregate: 'mockAggregate',
                        aggregateId: 'mockAggregateId',
                        context: 'mockContext'
                    },
                    channel: 'mockContext.mockAggregate.mockAggregateId',
                    eventCallbackTimeout: 250,
                    pollingTimeout: 1000,
                    pollingMaxRevisions: 2
                };
                const mockEmptyEventStream = {
                    events: []
                };
                mockES.getLastEvent.and.callFake((query, callback) => {
                    callback(null, null);
                });
                const mockNewEventStreams = [
                    {
                        events: [
                            {
                                streamRevision: 0,
                                payload: 'testNewEvent0'
                            }
                        ]
                    }, {
                        events: [
                            {
                                streamRevision: 1,
                                payload: 'testNewEvent1'
                            }
                        ]
                    }
                ];
                const mockPublishedEvents = [
                    {
                        revision: 0,
                        payload: 'testNewEvent0'
                    },
                    {
                        revision: 1,
                        payload: 'testNewEvent1'
                    }
                ];
                const mockCallbacks = [
                    {
                        onEventCallback: function(error, event, callback) {
                            callback();
                        },
                        onErrorCallback: function() {}
                    }
                ];
                spyOn(mockCallbacks[0], 'onEventCallback').and.callThrough();

                mockES.getEventStream.and.callFake((query, revMin, revMax, callback) => {
                    numExecutions++;
                    if (numExecutions === 2) {
                        callback(null, mockEmptyEventStream);
                        setTimeout(() => {
                            mockDistributedSignal.signal(mockOptionsWithHighTimeout.channel);
                        }, 25);
                    } else if (numExecutions === 3) {
                        callback(null, mockNewEventStreams[0]);
                        setTimeout(() => {
                            mockDistributedSignal.signal(mockOptionsWithHighTimeout.channel);
                        }, 25);
                    } else if (numExecutions === 4) {
                        callback(null, mockNewEventStreams[1]);

                        setTimeout(() => {
                            // Assert stream buffer and callbacks invoked
                            expect(mockStreamBuffer.offerEvents.calls.argsFor(0)).toEqual([mockNewEventStreams[0].events]);
                            expect(mockCallbacks[0].onEventCallback.calls.argsFor(0)).toEqual([null, mockNewEventStreams[0].events[0], jasmine.any(Function)]);

                            // Assert stream buffer and callbacks invoked
                            expect(mockStreamBuffer.offerEvents.calls.argsFor(1)).toEqual([mockNewEventStreams[1].events]);
                            expect(mockCallbacks[0].onEventCallback.calls.argsFor(1)).toEqual([null, mockNewEventStreams[1].events[0], jasmine.any(Function)]);
                            done();
                        }, 25);
                    } else {
                        callback(null, mockEmptyEventStream);
                    }
                });
                mockStreamBuffer.getEventsInBufferAsStream.and.returnValue(mockEmptyEventStream);
                mockStreamBuffer.offerEvents.and.callThrough();
                const mockRevision = 0;

                bufferedEventSubscription = new BufferedEventSubscription(mockOptionsWithHighTimeout);
                bufferedEventSubscription.subscribe('mockToken', mockRevision, mockCallbacks[0].onEventCallback, mockCallbacks[0].onErrorCallback);
                bufferedEventSubscription.subscribe('mockToken2', mockRevision, undefined);
            }, 100);

            it('should call all error callbacks of the subscribers with the proper error if es.getEventStream threw an error during the polling phase', (done) => {
                const mockError = new Error('test error');
                let numExecutions = 0;
                const mockOptionsWithHighTimeout = {
                    es: mockES,
                    distributedSignal: mockDistributedSignal,
                    streamBuffer: mockStreamBuffer,
                    query: {
                        aggregate: 'mockAggregate',
                        aggregateId: 'mockAggregateId',
                        context: 'mockContext'
                    },
                    channel: 'mockContext.mockAggregate.mockAggregateId',
                    eventCallbackTimeout: 10,
                    pollingTimeout: 1000,
                    pollingMaxRevisions: 2
                };
                const mockEmptyEventStream = {
                    events: []
                };
                mockES.getLastEvent.and.callFake((query, callback) => {
                    callback(null, null);
                });
                const mockNewEventStreams = [
                    {
                        events: [
                            {
                                streamRevision: 0,
                                payload: 'testNewEvent0'
                            }
                        ]
                    }, {
                        events: [
                            {
                                streamRevision: 1,
                                payload: 'testNewEvent1'
                            }
                        ]
                    }
                ];
                const mockPublishedEvents = [
                    {
                        revision: 0,
                        payload: 'testNewEvent0'
                    },
                    {
                        revision: 1,
                        payload: 'testNewEvent1'
                    }
                ];
                const mockCallbacks = [
                    {
                        onEventCallback: function(error, event, callback) {
                            callback();
                        },
                        onErrorCallback: function() {}
                    },
                    {
                        onEventCallback: function(error, event, callback) {
                            callback();
                        },
                        onErrorCallback: function() {}
                    }
                ];
                spyOn(mockCallbacks[0], 'onEventCallback').and.callThrough();
                spyOn(mockCallbacks[0], 'onErrorCallback').and.callThrough();
                spyOn(mockCallbacks[1], 'onEventCallback').and.callThrough();
                spyOn(mockCallbacks[1], 'onErrorCallback').and.callThrough();

                mockES.getEventStream.and.callFake((query, revMin, revMax, callback) => {
                    numExecutions++;
                    if (numExecutions <= 1) {
                        callback(null, mockEmptyEventStream);
                    } else if (numExecutions === 2) {
                        callback(null, mockEmptyEventStream);
                        setTimeout(() => {
                            mockDistributedSignal.signal(mockOptionsWithHighTimeout.channel);
                        }, 25);
                    } else if (numExecutions === 3) {
                        callback(null, mockNewEventStreams[0]);
                        setTimeout(() => {
                            mockDistributedSignal.signal(mockOptionsWithHighTimeout.channel);
                        }, 25);
                    } else if (numExecutions === 4) {
                        callback(mockError, null);

                        setTimeout(() => {
                            // Assert stream buffer and event callbacks invoked
                            expect(mockStreamBuffer.offerEvents.calls.argsFor(0)).toEqual([mockNewEventStreams[0].events]);
                            expect(mockCallbacks[0].onEventCallback.calls.argsFor(0)).toEqual([null, mockNewEventStreams[0].events[0], jasmine.any(Function)]);
                            expect(mockCallbacks[1].onEventCallback.calls.argsFor(0)).toEqual([null, mockNewEventStreams[0].events[0], jasmine.any(Function)]);

                            // Assert error callbacks invoked
                            expect(mockCallbacks[0].onErrorCallback.calls.argsFor(0)).toEqual([mockError]);
                            expect(mockCallbacks[1].onErrorCallback.calls.argsFor(0)).toEqual([mockError]);
                            done();
                        }, 25);
                    } else {
                        callback(mockError, null);
                    }
                });
                mockStreamBuffer.getEventsInBufferAsStream.and.returnValue(mockEmptyEventStream);
                mockStreamBuffer.offerEvents.and.callThrough();
                const mockRevision = 0;

                bufferedEventSubscription = new BufferedEventSubscription(mockOptionsWithHighTimeout);
                bufferedEventSubscription.subscribe('mockToken', mockRevision, mockCallbacks[0].onEventCallback, mockCallbacks[0].onErrorCallback);
                bufferedEventSubscription.subscribe('mockToken2', mockRevision, mockCallbacks[1].onEventCallback, mockCallbacks[1].onErrorCallback);
            }, 100);
        });
    });

    describe('unsubscribe', () => {
        it('should unsubscribe from the bufferedEventSubscription, no longer invoke the unsubcribed callback, and still invoke the callbacks of the active subscribers when an event is emitted', (done) => {
            let numExecutions = 0;
            const mockOptionsWithHighTimeout = {
                es: mockES,
                distributedSignal: mockDistributedSignal,
                streamBuffer: mockStreamBuffer,
                query: {
                    aggregate: 'mockAggregate',
                    aggregateId: 'mockAggregateId',
                    context: 'mockContext'
                },
                channel: 'mockContext.mockAggregate.mockAggregateId',
                eventCallbackTimeout: 250,
                pollingTimeout: 1000,
                pollingMaxRevisions: 2
            };
            const mockEmptyEventStream = {
                events: []
            };
            mockES.getLastEvent.and.callFake((query, callback) => {
                callback(null, null);
            });
            const mockNewEventStreams = [
                {
                    events: [
                        {
                            streamRevision: 0,
                            payload: 'testNewEvent0'
                        }
                    ]
                }, {
                    events: [
                        {
                            streamRevision: 1,
                            payload: 'testNewEvent1'
                        }
                    ]
                }
            ];
            const mockPublishedEvents = [
                {
                    revision: 0,
                    payload: 'testNewEvent0'
                },
                {
                    revision: 1,
                    payload: 'testNewEvent1'
                }
            ];
            const mockCallbacks = [
                {
                    onEventCallback: function(error, event, callback) {
                        callback();
                    },
                    onErrorCallback: function() {}
                },
                {
                    onEventCallback: function(error, event, callback) {
                        callback();
                    },
                    onErrorCallback: function() {}
                }
            ];
            spyOn(mockCallbacks[0], 'onEventCallback').and.callThrough();
            spyOn(mockCallbacks[1], 'onEventCallback').and.callThrough();

            mockES.getEventStream.and.callFake((query, revMin, revMax, callback) => {
                numExecutions++;
                if (numExecutions === 2) {
                    callback(null, mockEmptyEventStream);
                    setTimeout(() => {
                        mockDistributedSignal.signal(mockOptionsWithHighTimeout.channel);
                    }, 25);
                } else if (numExecutions === 3) {
                    callback(null, mockNewEventStreams[0]);
                    setTimeout(() => {
                        bufferedEventSubscription.unsubscribe('mockToken2');
                        mockDistributedSignal.signal(mockOptionsWithHighTimeout.channel);
                    }, 25);
                } else if (numExecutions === 4) {
                    callback(null, mockNewEventStreams[1]);

                    setTimeout(() => {
                        // Assert stream buffer and callbacks invoked
                        expect(mockStreamBuffer.offerEvents.calls.argsFor(0)).toEqual([mockNewEventStreams[0].events]);
                        expect(mockCallbacks[0].onEventCallback.calls.argsFor(0)).toEqual([null, mockNewEventStreams[0].events[0], jasmine.any(Function)]);
                        expect(mockCallbacks[1].onEventCallback.calls.argsFor(0)).toEqual([null, mockNewEventStreams[0].events[0], jasmine.any(Function)]);

                        // Assert stream buffer and callback invoked for only the active subscriber
                        expect(mockStreamBuffer.offerEvents.calls.argsFor(1)).toEqual([mockNewEventStreams[1].events]);
                        expect(mockCallbacks[0].onEventCallback.calls.argsFor(1)).toEqual([null, mockNewEventStreams[1].events[0], jasmine.any(Function)]);
                        expect(mockCallbacks[1].onEventCallback.calls.count()).toEqual(1);
                        done();
                    }, 25);
                } else {
                    callback(null, mockEmptyEventStream);
                }
            });
            mockStreamBuffer.getEventsInBufferAsStream.and.returnValue(mockEmptyEventStream);
            mockStreamBuffer.offerEvents.and.callThrough();
            const mockRevision = 0;

            bufferedEventSubscription = new BufferedEventSubscription(mockOptionsWithHighTimeout);
            bufferedEventSubscription.subscribe('mockToken', mockRevision, mockCallbacks[0].onEventCallback, mockCallbacks[0].onErrorCallback);
            bufferedEventSubscription.subscribe('mockToken2', mockRevision, mockCallbacks[1].onEventCallback, mockCallbacks[1].onErrorCallback);
        }, 100);

        it('should stop calling the ES and the streamBuffer if there are no more active subscribers', (done) => {
            let numExecutions = 0;
            const mockOptionsWithHighTimeout = {
                es: mockES,
                distributedSignal: mockDistributedSignal,
                streamBuffer: mockStreamBuffer,
                query: {
                    aggregate: 'mockAggregate',
                    aggregateId: 'mockAggregateId',
                    context: 'mockContext'
                },
                channel: 'mockContext.mockAggregate.mockAggregateId',
                eventCallbackTimeout: 250,
                pollingTimeout: 1000,
                pollingMaxRevisions: 2
            };
            const mockEmptyEventStream = {
                events: []
            };
            mockES.getLastEvent.and.callFake((query, callback) => {
                callback(null, null);
            });
            const mockNewEventStreams = [
                {
                    events: [
                        {
                            streamRevision: 0,
                            payload: 'testNewEvent0'
                        }
                    ]
                }, {
                    events: [
                        {
                            streamRevision: 1,
                            payload: 'testNewEvent1'
                        }
                    ]
                }
            ];
            const mockPublishedEvents = [
                {
                    revision: 0,
                    payload: 'testNewEvent0'
                },
                {
                    revision: 1,
                    payload: 'testNewEvent1'
                }
            ];
            const mockCallbacks = [
                {
                    onEventCallback: function(error, event, callback) {
                        callback();
                    },
                    onErrorCallback: function() {}
                },
                {
                    onEventCallback: function(error, event, callback) {
                        callback();
                    },
                    onErrorCallback: function() {}
                }
            ];
            spyOn
            spyOn(mockCallbacks[0], 'onEventCallback').and.callThrough();
            spyOn(mockCallbacks[1], 'onEventCallback').and.callThrough();

            mockES.getEventStream.and.callFake((query, revMin, revMax, callback) => {
                numExecutions++;
                if (numExecutions === 2) {
                    callback(null, mockEmptyEventStream);
                    setTimeout(() => {
                        mockDistributedSignal.signal(mockOptionsWithHighTimeout.channel);
                    }, 25);
                } else if (numExecutions === 3) {
                    callback(null, mockNewEventStreams[0]);
                    setTimeout(() => {
                        bufferedEventSubscription.unsubscribe('mockToken');
                        bufferedEventSubscription.unsubscribe('mockToken2');
                        mockDistributedSignal.signal(mockOptionsWithHighTimeout.channel);
                    }, 25);

                    setTimeout(() => {
                        // Assert stream buffer and callbacks invoked
                        expect(mockStreamBuffer.offerEvents.calls.argsFor(0)).toEqual([mockNewEventStreams[0].events]);
                        expect(mockCallbacks[0].onEventCallback.calls.argsFor(0)).toEqual([null, mockNewEventStreams[0].events[0], jasmine.any(Function)]);
                        expect(mockCallbacks[1].onEventCallback.calls.argsFor(0)).toEqual([null, mockNewEventStreams[0].events[0], jasmine.any(Function)]);

                        // Assert eventstore, stream buffer and callbacks no longer invoked
                        expect(mockES.getEventStream.calls.count()).toEqual(3);
                        expect(mockStreamBuffer.offerEvents.calls.count()).toEqual(1);
                        expect(mockCallbacks[0].onEventCallback.calls.count()).toEqual(1);
                        expect(mockCallbacks[1].onEventCallback.calls.count()).toEqual(1);
                        done();
                    }, 50);
                } else {
                    callback(null, mockEmptyEventStream);
                }
            });
            mockStreamBuffer.getEventsInBufferAsStream.and.returnValue(mockEmptyEventStream);
            mockStreamBuffer.offerEvents.and.callThrough();
            const mockRevision = 0;

            bufferedEventSubscription = new BufferedEventSubscription(mockOptionsWithHighTimeout);
            bufferedEventSubscription.subscribe('mockToken', mockRevision, mockCallbacks[0].onEventCallback, mockCallbacks[0].onErrorCallback);
            bufferedEventSubscription.subscribe('mockToken2', mockRevision, mockCallbacks[1].onEventCallback, mockCallbacks[1].onErrorCallback);
        }, 100);
    });

    describe('deactivate', () => {
        it('should stop the internal bufferedEventSubscription if it is running regardless if there are any active subscribers', (done) => {
            let numExecutions = 0;
            const mockOptionsWithHighTimeout = {
                es: mockES,
                distributedSignal: mockDistributedSignal,
                streamBuffer: mockStreamBuffer,
                query: {
                    aggregate: 'mockAggregate',
                    aggregateId: 'mockAggregateId',
                    context: 'mockContext'
                },
                channel: 'mockContext.mockAggregate.mockAggregateId',
                eventCallbackTimeout: 250,
                pollingTimeout: 1000,
                pollingMaxRevisions: 2
            };
            const mockEmptyEventStream = {
                events: []
            };
            mockES.getLastEvent.and.callFake((query, callback) => {
                callback(null, null);
            });
            const mockNewEventStreams = [
                {
                    events: [
                        {
                            streamRevision: 0,
                            payload: 'testNewEvent0'
                        }
                    ]
                }, {
                    events: [
                        {
                            streamRevision: 1,
                            payload: 'testNewEvent1'
                        }
                    ]
                }
            ];
            const mockPublishedEvents = [
                {
                    revision: 0,
                    payload: 'testNewEvent0'
                },
                {
                    revision: 1,
                    payload: 'testNewEvent1'
                }
            ];
            const mockCallbacks = [
                {
                    onEventCallback: function(error, event, callback) {
                        callback();
                    },
                    onErrorCallback: function() {}
                },
                {
                    onEventCallback: function(error, event, callback) {
                        callback();
                    },
                    onErrorCallback: function() {}
                }
            ];
            spyOn(mockCallbacks[0], 'onEventCallback').and.callThrough();
            spyOn(mockCallbacks[1], 'onEventCallback').and.callThrough();

            mockES.getEventStream.and.callFake((query, revMin, revMax, callback) => {
                numExecutions++;
                if (numExecutions === 2) {
                    callback(null, mockEmptyEventStream);
                    setTimeout(() => {
                        mockDistributedSignal.signal(mockOptionsWithHighTimeout.channel);
                    }, 25);
                } else if (numExecutions === 3) {
                    callback(null, mockNewEventStreams[0]);
                    setTimeout(() => {
                        bufferedEventSubscription.deactivate();
                        mockDistributedSignal.signal(mockOptionsWithHighTimeout.channel);
                    }, 25);

                    setTimeout(() => {
                        // Assert stream buffer and callbacks invoked
                        expect(mockStreamBuffer.offerEvents.calls.argsFor(0)).toEqual([mockNewEventStreams[0].events]);
                        expect(mockCallbacks[0].onEventCallback.calls.argsFor(0)).toEqual([null, mockNewEventStreams[0].events[0], jasmine.any(Function)]);
                        expect(mockCallbacks[1].onEventCallback.calls.argsFor(0)).toEqual([null, mockNewEventStreams[0].events[0], jasmine.any(Function)]);

                        // Assert eventstore, stream buffer and callbacks no longer invoked
                        expect(mockES.getEventStream.calls.count()).toEqual(3);
                        expect(mockStreamBuffer.offerEvents.calls.count()).toEqual(1);
                        expect(mockCallbacks[0].onEventCallback.calls.count()).toEqual(1);
                        expect(mockCallbacks[1].onEventCallback.calls.count()).toEqual(1);
                        done();
                    }, 50);
                } else {
                    callback(null, mockEmptyEventStream);
                }
            });
            mockStreamBuffer.getEventsInBufferAsStream.and.returnValue(mockEmptyEventStream);
            mockStreamBuffer.offerEvents.and.callThrough();
            const mockRevision = 0;

            bufferedEventSubscription = new BufferedEventSubscription(mockOptionsWithHighTimeout);
            bufferedEventSubscription.subscribe('mockToken', mockRevision, mockCallbacks[0].onEventCallback, mockCallbacks[0].onErrorCallback);
            bufferedEventSubscription.subscribe('mockToken2', mockRevision, mockCallbacks[1].onEventCallback, mockCallbacks[1].onErrorCallback);
        }, 100);
    });

    describe('activate', () => {
        it('should restart the internal bufferedEventSubscription if the bufferedEventSubscription was deactivated before, and there are active subscribers', (done) => {
            let numExecutions = 0;
            const mockOptionsWithHighTimeout = {
                es: mockES,
                distributedSignal: mockDistributedSignal,
                streamBuffer: mockStreamBuffer,
                query: {
                    aggregate: 'mockAggregate',
                    aggregateId: 'mockAggregateId',
                    context: 'mockContext'
                },
                channel: 'mockContext.mockAggregate.mockAggregateId',
                eventCallbackTimeout: 250,
                pollingTimeout: 1000,
                pollingMaxRevisions: 2
            };
            const mockEmptyEventStream = {
                events: []
            };
            mockES.getLastEvent.and.callFake((query, callback) => {
                callback(null, null);
            });
            const mockNewEventStreams = [
                {
                    events: [
                        {
                            streamRevision: 0,
                            payload: 'testNewEvent0'
                        }
                    ]
                }, {
                    events: [
                        {
                            streamRevision: 1,
                            payload: 'testNewEvent1'
                        }
                    ]
                }
            ];
            const mockPublishedEvents = [
                {
                    revision: 0,
                    payload: 'testNewEvent0'
                },
                {
                    revision: 1,
                    payload: 'testNewEvent1'
                }
            ];
            const mockCallbacks = [
                {
                    onEventCallback: function(error, event, callback) {
                        callback();
                    },
                    onErrorCallback: function() {}
                },
                {
                    onEventCallback: function(error, event, callback) {
                        callback();
                    },
                    onErrorCallback: function() {}
                }
            ];
            spyOn(mockCallbacks[0], 'onEventCallback').and.callThrough();
            spyOn(mockCallbacks[1], 'onEventCallback').and.callThrough();
            
            mockES.getEventStream.and.callFake((query, revMin, revMax, callback) => {
                numExecutions++;
                if (numExecutions === 2) {
                    callback(null, mockEmptyEventStream);
                    setTimeout(() => {
                        mockDistributedSignal.signal(mockOptionsWithHighTimeout.channel);
                    }, 25);
                } else if (numExecutions === 3) {
                    callback(null, mockNewEventStreams[0]);
                    setTimeout(() => {
                        bufferedEventSubscription.deactivate();
                        bufferedEventSubscription.activate();
                        mockDistributedSignal.signal(mockOptionsWithHighTimeout.channel);
                    }, 25);
                } else if (numExecutions === 4) {
                    callback(null, mockNewEventStreams[1]);

                    setTimeout(() => {
                        // Assert stream buffer and callbacks invoked
                        expect(mockStreamBuffer.offerEvents.calls.argsFor(0)).toEqual([mockNewEventStreams[0].events]);
                        expect(mockCallbacks[0].onEventCallback.calls.argsFor(0)).toEqual([null, mockNewEventStreams[0].events[0], jasmine.any(Function)]);
                        expect(mockCallbacks[1].onEventCallback.calls.argsFor(0)).toEqual([null, mockNewEventStreams[0].events[0], jasmine.any(Function)]);

                        // Assert stream buffer and callbacks invoked
                        expect(mockStreamBuffer.offerEvents.calls.argsFor(1)).toEqual([mockNewEventStreams[1].events]);
                        expect(mockCallbacks[0].onEventCallback.calls.argsFor(1)).toEqual([null, mockNewEventStreams[1].events[0], jasmine.any(Function)]);
                        expect(mockCallbacks[1].onEventCallback.calls.argsFor(1)).toEqual([null, mockNewEventStreams[1].events[0], jasmine.any(Function)]);
                        done();
                    }, 25);
                } else {
                    callback(null, mockEmptyEventStream);
                }
            });
            mockStreamBuffer.getEventsInBufferAsStream.and.returnValue(mockEmptyEventStream);
            mockStreamBuffer.offerEvents.and.callThrough();
            const mockRevision = 0;

            bufferedEventSubscription = new BufferedEventSubscription(mockOptionsWithHighTimeout);
            bufferedEventSubscription.subscribe('mockToken', mockRevision, mockCallbacks[0].onEventCallback, mockCallbacks[0].onErrorCallback);
            bufferedEventSubscription.subscribe('mockToken2', mockRevision, mockCallbacks[1].onEventCallback, mockCallbacks[1].onErrorCallback);
        }, 150);

        it('should not start a bufferedEventSubscription if the internal bufferedEventSubscription has not been started', (done) => {
            const mockEmptyEventStream = {
                events: []
            };
            mockES.getLastEvent.and.callFake((query, callback) => {
                callback(null, null);
            });
            
            mockES.getEventStream.and.returnValue(mockEmptyEventStream);
            mockStreamBuffer.getEventsInBufferAsStream.and.returnValue(mockEmptyEventStream);
            mockStreamBuffer.offerEvents.and.callThrough();

            bufferedEventSubscription = new BufferedEventSubscription(mockOptions);
            bufferedEventSubscription.activate();

            setTimeout(() => {
                expect(mockES.getEventStream).not.toHaveBeenCalled();
                expect(mockStreamBuffer.getEventsInBufferAsStream).not.toHaveBeenCalled();
                expect(mockStreamBuffer.offerEvents).not.toHaveBeenCalled();
                done();
            }, 50);
        }, 100);

        it('should not start a bufferedEventSubscription if there are no longer any active subscribers', (done) => {
            let numExecutions = 0;
            const mockOptionsWithHighTimeout = {
                es: mockES,
                distributedSignal: mockDistributedSignal,
                streamBuffer: mockStreamBuffer,
                query: {
                    aggregate: 'mockAggregate',
                    aggregateId: 'mockAggregateId',
                    context: 'mockContext'
                },
                channel: 'mockContext.mockAggregate.mockAggregateId',
                eventCallbackTimeout: 250,
                pollingTimeout: 1000,
                pollingMaxRevisions: 2
            };
            const mockEmptyEventStream = {
                events: []
            };
            mockES.getLastEvent.and.callFake((query, callback) => {
                callback(null, null);
            });
            const mockNewEventStreams = [
                {
                    events: [
                        {
                            streamRevision: 0,
                            payload: 'testNewEvent0'
                        }
                    ]
                }, {
                    events: [
                        {
                            streamRevision: 1,
                            payload: 'testNewEvent1'
                        }
                    ]
                }
            ];
            const mockPublishedEvents = [
                {
                    revision: 0,
                    payload: 'testNewEvent0'
                },
                {
                    revision: 1,
                    payload: 'testNewEvent1'
                }
            ];
            const mockCallbacks = [
                {
                    onEventCallback: function(error, event, callback) {
                        callback();
                    },
                    onErrorCallback: function() {}
                },
                {
                    onEventCallback: function(error, event, callback) {
                        callback();
                    },
                    onErrorCallback: function() {}
                }
            ];
            spyOn(mockCallbacks[0], 'onEventCallback').and.callThrough();
            spyOn(mockCallbacks[1], 'onEventCallback').and.callThrough();
            
            mockES.getEventStream.and.callFake((query, revMin, revMax, callback) => {
                numExecutions++;
                if (numExecutions === 2) {
                    callback(null, mockEmptyEventStream);
                    setTimeout(() => {
                        mockDistributedSignal.signal(mockOptionsWithHighTimeout.channel);
                    }, 25);
                } else if (numExecutions === 3) {
                    callback(null, mockNewEventStreams[0]);
                    setTimeout(() => {
                        bufferedEventSubscription.unsubscribe('mockToken');
                        bufferedEventSubscription.unsubscribe('mockToken2');
                        bufferedEventSubscription.activate();
                        mockDistributedSignal.signal(mockOptionsWithHighTimeout.channel);
                    }, 25);
                    setTimeout(() => {
                        // Assert stream buffer and callbacks invoked
                        expect(mockStreamBuffer.offerEvents.calls.argsFor(0)).toEqual([mockNewEventStreams[0].events]);
                        expect(mockCallbacks[0].onEventCallback.calls.argsFor(0)).toEqual([null, mockNewEventStreams[0].events[0], jasmine.any(Function)]);
                        expect(mockCallbacks[1].onEventCallback.calls.argsFor(0)).toEqual([null, mockNewEventStreams[0].events[0], jasmine.any(Function)]);

                        // Assert eventstore, stream buffer and callbacks not invoked
                        expect(mockES.getEventStream.calls.count()).toEqual(3);
                        expect(mockStreamBuffer.offerEvents.calls.count()).toEqual(1);
                        expect(mockCallbacks[0].onEventCallback.calls.count()).toEqual(1);
                        expect(mockCallbacks[1].onEventCallback.calls.count()).toEqual(1);
                        done();
                    }, 50);
                } else {
                    callback(null, mockEmptyEventStream);
                }
            });
            mockStreamBuffer.getEventsInBufferAsStream.and.returnValue(mockEmptyEventStream);
            mockStreamBuffer.offerEvents.and.callThrough();
            const mockRevision = 0;

            bufferedEventSubscription = new BufferedEventSubscription(mockOptionsWithHighTimeout);
            bufferedEventSubscription.subscribe('mockToken', mockRevision, mockCallbacks[0].onEventCallback, mockCallbacks[0].onErrorCallback);
            bufferedEventSubscription.subscribe('mockToken2', mockRevision, mockCallbacks[1].onEventCallback, mockCallbacks[1].onErrorCallback);
        }, 100);
    });

    describe('isActive', () => {
        it('should return true if the bufferedEventSubscription is active', (done) => {
            let numExecutions = 0;
            const mockOptionsWithHighTimeout = {
                es: mockES,
                distributedSignal: mockDistributedSignal,
                streamBuffer: mockStreamBuffer,
                query: {
                    aggregate: 'mockAggregate',
                    aggregateId: 'mockAggregateId',
                    context: 'mockContext'
                },
                channel: 'mockContext.mockAggregate.mockAggregateId',
                eventCallbackTimeout: 250,
                pollingTimeout: 1000,
                pollingMaxRevisions: 2
            };
            const mockEmptyEventStream = {
                events: []
            };
            mockES.getLastEvent.and.callFake((query, callback) => {
                callback(null, null);
            });
            const mockNewEventStreams = [
                {
                    events: [
                        {
                            streamRevision: 0,
                            payload: 'testNewEvent0'
                        }
                    ]
                }, {
                    events: [
                        {
                            streamRevision: 1,
                            payload: 'testNewEvent1'
                        }
                    ]
                }
            ];
            const mockPublishedEvents = [
                {
                    revision: 0,
                    payload: 'testNewEvent0'
                },
                {
                    revision: 1,
                    payload: 'testNewEvent1'
                }
            ];
            const mockCallbacks = [
                {
                    onEventCallback: function(error, event, callback) {
                        callback();
                    },
                    onErrorCallback: function() {}
                },
                {
                    onEventCallback: function(error, event, callback) {
                        callback();
                    },
                    onErrorCallback: function() {}
                }
            ];
            spyOn(mockCallbacks[0], 'onEventCallback').and.callThrough();
            spyOn(mockCallbacks[1], 'onEventCallback').and.callThrough();

            mockES.getEventStream.and.callFake((query, revMin, revMax, callback) => {
                numExecutions++;
                if (numExecutions === 2) {
                    callback(null, mockEmptyEventStream);
                    setTimeout(() => {
                        mockDistributedSignal.signal(mockOptionsWithHighTimeout.channel);
                    }, 25);
                } else if (numExecutions === 3) {
                    callback(null, mockNewEventStreams[0]);

                    setTimeout(() => {
                        // Assert bufferedEventSubscription is active
                        expect(bufferedEventSubscription.isActive()).toEqual(true);
                        done();
                    }, 25);
                } else {
                    callback(null, mockEmptyEventStream);
                }
            });
            mockStreamBuffer.getEventsInBufferAsStream.and.returnValue(mockEmptyEventStream);
            mockStreamBuffer.offerEvents.and.callThrough();
            const mockRevision = 0;

            bufferedEventSubscription = new BufferedEventSubscription(mockOptionsWithHighTimeout);
            bufferedEventSubscription.subscribe('mockToken', mockRevision, mockCallbacks[0].onEventCallback, mockCallbacks[0].onErrorCallback);
            bufferedEventSubscription.subscribe('mockToken2', mockRevision, mockCallbacks[1].onEventCallback, mockCallbacks[1].onErrorCallback);
        }, 100);

        it('should return false if the bufferedEventSubscription is deactivated', (done) => {
            let numExecutions = 0;
            const mockOptionsWithHighTimeout = {
                es: mockES,
                streamBuffer: mockStreamBuffer,
                distributedSignal: mockDistributedSignal,
                query: {
                    aggregate: 'mockAggregate',
                    aggregateId: 'mockAggregateId',
                    context: 'mockContext'
                },
                channel: 'mockContext.mockAggregate.mockAggregateId',
                eventCallbackTimeout: 250,
                pollingTimeout: 1000,
                pollingMaxRevisions: 2
            };
            const mockEmptyEventStream = {
                events: []
            };
            mockES.getLastEvent.and.callFake((query, callback) => {
                callback(null, null);
            });
            const mockNewEventStreams = [
                {
                    events: [
                        {
                            streamRevision: 0,
                            payload: 'testNewEvent0'
                        }
                    ]
                }, {
                    events: [
                        {
                            streamRevision: 1,
                            payload: 'testNewEvent1'
                        }
                    ]
                }
            ];
            const mockPublishedEvents = [
                {
                    revision: 0,
                    payload: 'testNewEvent0'
                },
                {
                    revision: 1,
                    payload: 'testNewEvent1'
                }
            ];
            const mockCallbacks = [
                {
                    onEventCallback: function(error, event, callback) {
                        callback();
                    },
                    onErrorCallback: function() {}
                },
                {
                    onEventCallback: function(error, event, callback) {
                        callback();
                    },
                    onErrorCallback: function() {}
                }
            ];
            spyOn(mockCallbacks[0], 'onEventCallback').and.callThrough();
            spyOn(mockCallbacks[1], 'onEventCallback').and.callThrough();

            mockES.getEventStream.and.callFake((query, revMin, revMax, callback) => {
                numExecutions++;
                if (numExecutions === 2) {
                    callback(null, mockEmptyEventStream);
                    setTimeout(() => {
                        mockDistributedSignal.signal(mockOptionsWithHighTimeout.channel);
                    }, 25);
                } else if (numExecutions === 3) {
                    callback(null, mockNewEventStreams[0]);
                    setTimeout(() => {
                        bufferedEventSubscription.deactivate();
                        mockDistributedSignal.signal(mockOptionsWithHighTimeout.channel);
                    }, 25);

                    setTimeout(() => {
                        // Assert bufferedEventSubscription is no longer active
                        expect(bufferedEventSubscription.isActive()).toEqual(false);
                        done();
                    }, 50);
                } else {
                    callback(null, mockEmptyEventStream);
                }
            });
            mockStreamBuffer.getEventsInBufferAsStream.and.returnValue(mockEmptyEventStream);
            mockStreamBuffer.offerEvents.and.callThrough();
            const mockRevision = 0;

            bufferedEventSubscription = new BufferedEventSubscription(mockOptionsWithHighTimeout);
            bufferedEventSubscription.subscribe('mockToken', mockRevision, mockCallbacks[0].onEventCallback, mockCallbacks[0].onErrorCallback);
            bufferedEventSubscription.subscribe('mockToken2', mockRevision, mockCallbacks[1].onEventCallback, mockCallbacks[1].onErrorCallback);
        }, 1000);

        it('should return false if the internal bufferedEventSubscription has not been started', (done) => {
            const mockEmptyEventStream = {
                events: []
            };
            mockES.getLastEvent.and.callFake((query, callback) => {
                callback(null, null);
            });
            
            mockES.getEventStream.and.returnValue(mockEmptyEventStream);
            mockStreamBuffer.getEventsInBufferAsStream.and.returnValue(mockEmptyEventStream);
            mockStreamBuffer.offerEvents.and.callThrough();

            bufferedEventSubscription = new BufferedEventSubscription(mockOptions);

            setTimeout(() => {
                expect(bufferedEventSubscription.isActive()).toEqual(false);
                done();
            }, 50);
        }, 100);
    });
});
