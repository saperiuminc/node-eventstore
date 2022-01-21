const SubscriptionEventStreamBuffer = require('../lib/subscriptionEventStreamBuffer');
const EventStream = require('../lib/eventStream');

xdescribe('eventStreamBuffer', () => {
    let mockOptions;

    beforeEach(() => {
        mockOptions = {
            es: { type: 'mockES', commit: function() {} },
            query: {
                aggregate: 'mockAggregate',
                aggregateId: 'mockAggregateId',
                context: 'mockContext'
            },
            channel: 'mockContext.mockAggregate.mockAggregateId',
            bucket: 'mockBucket',
            bufferCapacity: 10,
            poolCapacity: 5,
            ttl: 864000,
            onOfferEvent: function() {},
            onOfferEventError: function() {},
            onInactive: function() {},
            onClose: function() {}
        };
    });

    describe('initialization', () => {
        it('should set all provided options and instantiate the buffer and pool', () => {
            const now = new Date().getTime();
            const eventStreamBuffer = new SubscriptionEventStreamBuffer(mockOptions);

            expect(eventStreamBuffer._streamBuffer).toBeDefined();
            expect(eventStreamBuffer._offeredEventsPool).toBeDefined();
            expect(eventStreamBuffer._streamBuffer.length).toEqual(0);
            expect(eventStreamBuffer._offeredEventsPool.length).toEqual(0);

            expect(eventStreamBuffer.es).toEqual(mockOptions.es);
            expect(eventStreamBuffer.query).toEqual(mockOptions.query);
            expect(eventStreamBuffer.channel).toEqual(mockOptions.channel);
            expect(eventStreamBuffer.bucket).toEqual(mockOptions.bucket);
            expect(eventStreamBuffer.bufferCapacity).toEqual(mockOptions.bufferCapacity);
            expect(eventStreamBuffer.poolCapacity).toEqual(mockOptions.poolCapacity);
            expect(eventStreamBuffer.ttl).toEqual(mockOptions.ttl);
            expect(eventStreamBuffer.createdAt).toBeGreaterThanOrEqual(now);
            expect(eventStreamBuffer.updatedAt).toBeGreaterThanOrEqual(now);

            expect(eventStreamBuffer.onOfferEvent).toEqual(mockOptions.onOfferEvent);
            expect(eventStreamBuffer.onOfferEventError).toEqual(mockOptions.onOfferEventError);
            expect(eventStreamBuffer.onInactive).toEqual(mockOptions.onInactive);
            expect(eventStreamBuffer.onClose).toEqual(mockOptions.onClose);
        });
    });

    describe('offerEvent', () => {
        it('should add an event to the buffer, update the updatedAt timestamp, trigger onOfferEvent callback, and not add to the pool if the buffer and the pool are initially empty', () => {
            const mockEvent = {
                streamId: 'mockEventStreamId1',
                streamRevision: 0
            };
            const now = new Date().getTime();
            spyOn(mockOptions, 'onOfferEvent').and.callThrough();

            const eventStreamBuffer = new SubscriptionEventStreamBuffer(mockOptions);
            eventStreamBuffer.updatedAt = 0; // Force set the updatedAt to properly assert later

            eventStreamBuffer.offerEvent(mockEvent);

            expect(eventStreamBuffer._streamBuffer.length).toEqual(1);
            expect(eventStreamBuffer._streamBuffer[0]).toEqual(mockEvent);
            expect(eventStreamBuffer._offeredEventsPool.length).toEqual(0);
            expect(eventStreamBuffer.updatedAt).toBeGreaterThanOrEqual(now);
            expect(mockOptions.onOfferEvent).toHaveBeenCalledWith(mockOptions.bucket, mockOptions.channel);
        });

        it('should add an event to the buffer, update the updatedAt timestamp, trigger onOfferEvent callback, and move only the contiguous events from the pool to the buffer if the buffer is initially empty', () => {
            const mockEvents = [{
                streamId: 'mockEventStreamId1',
                streamRevision: 0
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 1
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 2
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 5
            }];
            const now = new Date().getTime();
            spyOn(mockOptions, 'onOfferEvent').and.callThrough();

            const eventStreamBuffer = new SubscriptionEventStreamBuffer(mockOptions);
            eventStreamBuffer.updatedAt = 0; // Force set the updatedAt to properly assert later
            eventStreamBuffer._offeredEventsPool.queue(mockEvents[2]);
            eventStreamBuffer._offeredEventsPool.queue(mockEvents[3]);
            eventStreamBuffer._offeredEventsPool.queue(mockEvents[1]);

            eventStreamBuffer.offerEvent(mockEvents[0]);

            expect(eventStreamBuffer._streamBuffer.length).toEqual(3);
            expect(eventStreamBuffer._streamBuffer[0]).toEqual(mockEvents[0]);
            expect(eventStreamBuffer._streamBuffer[1]).toEqual(mockEvents[1]);
            expect(eventStreamBuffer._streamBuffer[2]).toEqual(mockEvents[2]);

            expect(eventStreamBuffer._offeredEventsPool.length).toEqual(1);
            expect(eventStreamBuffer._offeredEventsPool.peek()).toEqual(mockEvents[3]);

            expect(eventStreamBuffer.updatedAt).toBeGreaterThanOrEqual(now);
            expect(mockOptions.onOfferEvent).toHaveBeenCalledWith(mockOptions.bucket, mockOptions.channel);
        });

        it('should add an event to the buffer, update the updatedAt timestamp, trigger onOfferEvent callback, and not move any events from the pool to the buffer if the buffer is initially empty and the pool contains events that are not contiguous to the input event', () => {
            const mockEvents = [{
                streamId: 'mockEventStreamId1',
                streamRevision: 0
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 4
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 3
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 5
            }];
            const now = new Date().getTime();
            spyOn(mockOptions, 'onOfferEvent').and.callThrough();

            const eventStreamBuffer = new SubscriptionEventStreamBuffer(mockOptions);
            eventStreamBuffer.updatedAt = 0; // Force set the updatedAt to properly assert later
            eventStreamBuffer._offeredEventsPool.queue(mockEvents[2]);
            eventStreamBuffer._offeredEventsPool.queue(mockEvents[3]);
            eventStreamBuffer._offeredEventsPool.queue(mockEvents[1]);

            eventStreamBuffer.offerEvent(mockEvents[0]);

            expect(eventStreamBuffer._streamBuffer.length).toEqual(1);
            expect(eventStreamBuffer._streamBuffer[0]).toEqual(mockEvents[0]);

            expect(eventStreamBuffer._offeredEventsPool.length).toEqual(3);
            expect(eventStreamBuffer._offeredEventsPool.peek()).toEqual(mockEvents[2]);

            expect(eventStreamBuffer.updatedAt).toBeGreaterThanOrEqual(now);
            expect(mockOptions.onOfferEvent).toHaveBeenCalledWith(mockOptions.bucket, mockOptions.channel);
        });

        it('should add an event to the buffer, update the updatedAt timestamp, trigger onOfferEvent callback, and not add to the pool if the pool is initially empty and the streamRevision of the input event is 1 higher than the buffer latest revision', () => {
            const mockEvents = [{
                streamId: 'mockEventStreamId1',
                streamRevision: 0
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 1
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 2
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 3
            }];
            const now = new Date().getTime();
            spyOn(mockOptions, 'onOfferEvent').and.callThrough();

            const eventStreamBuffer = new SubscriptionEventStreamBuffer(mockOptions);
            eventStreamBuffer.updatedAt = 0; // Force set the updatedAt to properly assert later
            eventStreamBuffer._streamBuffer.push(mockEvents[0]);
            eventStreamBuffer._streamBuffer.push(mockEvents[1]);
            eventStreamBuffer._streamBuffer.push(mockEvents[2]);
            eventStreamBuffer.offerEvent(mockEvents[3]);

            expect(eventStreamBuffer._streamBuffer.length).toEqual(4);
            expect(eventStreamBuffer._streamBuffer[3]).toEqual(mockEvents[3]);
            expect(eventStreamBuffer._offeredEventsPool.length).toEqual(0);

            expect(eventStreamBuffer.updatedAt).toBeGreaterThanOrEqual(now);
            expect(mockOptions.onOfferEvent).toHaveBeenCalledWith(mockOptions.bucket, mockOptions.channel);
        });

        it('should add an event to the buffer, update the updatedAt timestamp, trigger onOfferEvent callback, and move only the contiguous events from the pool to the buffer if the buffer is not empty and the input streamRevision of the event is 1 higher than the buffer latest revision', () => {
            const mockEvents = [{
                streamId: 'mockEventStreamId1',
                streamRevision: 0
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 1
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 2
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 3
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 5
            }];
            const now = new Date().getTime();
            spyOn(mockOptions, 'onOfferEvent').and.callThrough();

            const eventStreamBuffer = new SubscriptionEventStreamBuffer(mockOptions);
            eventStreamBuffer.updatedAt = 0; // Force set the updatedAt to properly assert later
            eventStreamBuffer._streamBuffer.push(mockEvents[0]);
            eventStreamBuffer._offeredEventsPool.queue(mockEvents[3]);
            eventStreamBuffer._offeredEventsPool.queue(mockEvents[4]);
            eventStreamBuffer._offeredEventsPool.queue(mockEvents[2]);

            eventStreamBuffer.offerEvent(mockEvents[1]);

            expect(eventStreamBuffer._streamBuffer.length).toEqual(4);
            expect(eventStreamBuffer._streamBuffer[0]).toEqual(mockEvents[0]);
            expect(eventStreamBuffer._streamBuffer[1]).toEqual(mockEvents[1]);
            expect(eventStreamBuffer._streamBuffer[2]).toEqual(mockEvents[2]);
            expect(eventStreamBuffer._streamBuffer[3]).toEqual(mockEvents[3]);

            expect(eventStreamBuffer._offeredEventsPool.length).toEqual(1);
            expect(eventStreamBuffer._offeredEventsPool.peek()).toEqual(mockEvents[4]);

            expect(eventStreamBuffer.updatedAt).toBeGreaterThanOrEqual(now);
            expect(mockOptions.onOfferEvent).toHaveBeenCalledWith(mockOptions.bucket, mockOptions.channel);
        });

        it('should add an event to the buffer, update the updatedAt timestamp, trigger onOfferEvent callback, and not move any events from the pool to the buffer if the buffer is not empty, the input streamRevision of the event is 1 higher than the buffer latest revision, and the pool contains events that are not contiguous to the input event', () => {
            const mockEvents = [{
                streamId: 'mockEventStreamId1',
                streamRevision: 0
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 1
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 4
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 3
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 5
            }];
            const now = new Date().getTime();
            spyOn(mockOptions, 'onOfferEvent').and.callThrough();

            const eventStreamBuffer = new SubscriptionEventStreamBuffer(mockOptions);
            eventStreamBuffer.updatedAt = 0; // Force set the updatedAt to properly assert later
            eventStreamBuffer._streamBuffer.push(mockEvents[0]);
            eventStreamBuffer._offeredEventsPool.queue(mockEvents[3]);
            eventStreamBuffer._offeredEventsPool.queue(mockEvents[4]);
            eventStreamBuffer._offeredEventsPool.queue(mockEvents[2]);

            eventStreamBuffer.offerEvent(mockEvents[1]);

            expect(eventStreamBuffer._streamBuffer.length).toEqual(2);
            expect(eventStreamBuffer._streamBuffer[0]).toEqual(mockEvents[0]);
            expect(eventStreamBuffer._streamBuffer[1]).toEqual(mockEvents[1]);

            expect(eventStreamBuffer._offeredEventsPool.length).toEqual(3);
            expect(eventStreamBuffer._offeredEventsPool.peek()).toEqual(mockEvents[3]);

            expect(eventStreamBuffer.updatedAt).toBeGreaterThanOrEqual(now);
            expect(mockOptions.onOfferEvent).toHaveBeenCalledWith(mockOptions.bucket, mockOptions.channel);
        });

        it('should add an event to the pool, trigger onOfferEvent callback, and not update the updatedAt timestamp if the buffer is not empty and the streamRevision of the input event is higher than the latest streamRevision on the buffer, but is not contiguous', () => {
            const mockEvents = [{
                streamId: 'mockEventStreamId1',
                streamRevision: 0
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 4
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 3
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 5
            }];
            spyOn(mockOptions, 'onOfferEvent').and.callThrough();

            const eventStreamBuffer = new SubscriptionEventStreamBuffer(mockOptions);
            eventStreamBuffer.updatedAt = 0; // Force set the updatedAt to properly assert later
            eventStreamBuffer._streamBuffer.push(mockEvents[0]);

            eventStreamBuffer.offerEvent(mockEvents[3]);
            expect(eventStreamBuffer._streamBuffer.length).toEqual(1);
            expect(eventStreamBuffer._offeredEventsPool.length).toEqual(1);
            expect(eventStreamBuffer._offeredEventsPool.peek()).toEqual(mockEvents[3]);
            
            eventStreamBuffer.offerEvent(mockEvents[2]);
            expect(eventStreamBuffer._streamBuffer.length).toEqual(1);
            expect(eventStreamBuffer._offeredEventsPool.length).toEqual(2);
            expect(eventStreamBuffer._offeredEventsPool.peek()).toEqual(mockEvents[2]);
            
            eventStreamBuffer.offerEvent(mockEvents[1]);
            expect(eventStreamBuffer._streamBuffer.length).toEqual(1);
            expect(eventStreamBuffer._offeredEventsPool.length).toEqual(3);
            expect(eventStreamBuffer._offeredEventsPool.peek()).toEqual(mockEvents[2]);

            expect(eventStreamBuffer.updatedAt).toEqual(0);
            expect(mockOptions.onOfferEvent).toHaveBeenCalledWith(mockOptions.bucket, mockOptions.channel);
        });

        it('should add an event to the buffer, update the updatedAt timestamp, trigger onOfferEvent callback, and shift the buffer if the buffer capacity is reached when adding', () => {
            const mockEvents = [{
                streamId: 'mockEventStreamId1',
                streamRevision: 0
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 1
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 2
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 3
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 4
            }];
            const now = new Date().getTime();

            const mockOptionsWithLowBufferCapacity = {
                es: { type: 'mockES' },
                query: {
                    aggregate: 'mockAggregate',
                    aggregateId: 'mockAggregateId',
                    context: 'mockContext'
                },
                channel: 'mockContext.mockAggregate.mockAggregateId',
                bucket: 'mockBucket',
                bufferCapacity: 3,
                poolCapacity: 5,
                ttl: 864000,
                onOfferEvent: function() {},
                onClose: function() {}
            };
            spyOn(mockOptionsWithLowBufferCapacity, 'onOfferEvent').and.callThrough();

            const eventStreamBuffer = new SubscriptionEventStreamBuffer(mockOptionsWithLowBufferCapacity);
            eventStreamBuffer.updatedAt = 0; // Force set the updatedAt to properly assert later
            eventStreamBuffer._streamBuffer.push(mockEvents[0]);
            eventStreamBuffer._streamBuffer.push(mockEvents[1]);
            eventStreamBuffer._streamBuffer.push(mockEvents[2]);

            eventStreamBuffer.offerEvent(mockEvents[3]);
            expect(eventStreamBuffer._streamBuffer.length).toEqual(3);
            expect(eventStreamBuffer._streamBuffer[0]).toEqual(mockEvents[1]);
            expect(eventStreamBuffer._streamBuffer[1]).toEqual(mockEvents[2]);
            expect(eventStreamBuffer._streamBuffer[2]).toEqual(mockEvents[3]);
            expect(eventStreamBuffer._offeredEventsPool.length).toEqual(0);

            eventStreamBuffer.offerEvent(mockEvents[4]);
            expect(eventStreamBuffer._streamBuffer[0]).toEqual(mockEvents[2]);
            expect(eventStreamBuffer._streamBuffer[1]).toEqual(mockEvents[3]);
            expect(eventStreamBuffer._streamBuffer[2]).toEqual(mockEvents[4]);
            expect(eventStreamBuffer._offeredEventsPool.length).toEqual(0);

            expect(eventStreamBuffer.updatedAt).toBeGreaterThanOrEqual(now);
            expect(mockOptionsWithLowBufferCapacity.onOfferEvent).toHaveBeenCalledWith(mockOptions.bucket, mockOptions.channel);
        });

        it('should reset the buffer and populate with the latest contiguous events from the pool, update the updatedAt timestamp, and trigger onOfferEvent callback if an event was offered to the pool and the pool reached its max capacity', () => {
            const mockEvents = [{
                streamId: 'mockEventStreamId1',
                streamRevision: 0
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 3
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 4
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 6
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 8
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 9
            }];
            const now = new Date().getTime();
            spyOn(mockOptions, 'onOfferEvent').and.callThrough();

            const eventStreamBuffer = new SubscriptionEventStreamBuffer(mockOptions);
            eventStreamBuffer.updatedAt = 0; // Force set the updatedAt to properly assert later
            eventStreamBuffer._streamBuffer.push(mockEvents[0]);
            eventStreamBuffer._offeredEventsPool.queue(mockEvents[1]);
            eventStreamBuffer._offeredEventsPool.queue(mockEvents[2]);
            eventStreamBuffer._offeredEventsPool.queue(mockEvents[3]);
            eventStreamBuffer._offeredEventsPool.queue(mockEvents[4]);

            eventStreamBuffer.offerEvent(mockEvents[5]);
            expect(eventStreamBuffer._streamBuffer.length).toEqual(2);
            expect(eventStreamBuffer._streamBuffer[0]).toEqual(mockEvents[4]);
            expect(eventStreamBuffer._streamBuffer[1]).toEqual(mockEvents[5]);
            expect(eventStreamBuffer._offeredEventsPool.length).toEqual(0);

            expect(eventStreamBuffer.updatedAt).toBeGreaterThanOrEqual(now);
            expect(mockOptions.onOfferEvent).toHaveBeenCalledWith(mockOptions.bucket, mockOptions.channel);
        });

        it('should not add an event on both the buffer and the pool, trigger onOfferEventError, and not trigger onOfferEvent callback if the streamRevision of the input event is less than the buffer latest revision', () => {
            const mockEvents = [{
                streamId: 'mockEventStreamId1',
                streamRevision: 0
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 1
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 2
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 3
            }];
            spyOn(mockOptions, 'onOfferEventError').and.callThrough();
            spyOn(mockOptions, 'onOfferEvent').and.callThrough();

            const eventStreamBuffer = new SubscriptionEventStreamBuffer(mockOptions);
            eventStreamBuffer.updatedAt = 0;
            eventStreamBuffer._streamBuffer.push(mockEvents[1]);
            eventStreamBuffer._streamBuffer.push(mockEvents[2]);
            eventStreamBuffer._streamBuffer.push(mockEvents[3]);

            eventStreamBuffer.offerEvent(mockEvents[0]);
            expect(eventStreamBuffer._streamBuffer.length).toEqual(3);
            expect(eventStreamBuffer._streamBuffer[0]).toEqual(mockEvents[1]);
            expect(eventStreamBuffer._streamBuffer[1]).toEqual(mockEvents[2]);
            expect(eventStreamBuffer._streamBuffer[2]).toEqual(mockEvents[3]);
            expect(eventStreamBuffer._offeredEventsPool.length).toEqual(0);

            expect(eventStreamBuffer.updatedAt).toEqual(0);
            expect(mockOptions.onOfferEventError).toHaveBeenCalledWith(mockOptions.bucket, mockOptions.channel);
            expect(mockOptions.onOfferEvent).not.toHaveBeenCalled();
        });

        it('should not add an event on both the buffer and the pool, trigger onOfferEventError, and not trigger onOfferEvent callback if the streamRevision of the input event is equal to the buffer latest revision', () => {
            const mockEvents = [{
                streamId: 'mockEventStreamId1',
                streamRevision: 0
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 1
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 2
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 3
            }];
            spyOn(mockOptions, 'onOfferEventError').and.callThrough();
            spyOn(mockOptions, 'onOfferEvent').and.callThrough();

            const eventStreamBuffer = new SubscriptionEventStreamBuffer(mockOptions);
            eventStreamBuffer.updatedAt = 0; // Force set the updatedAt to properly assert later
            eventStreamBuffer._streamBuffer.push(mockEvents[1]);
            eventStreamBuffer._streamBuffer.push(mockEvents[2]);
            eventStreamBuffer._streamBuffer.push(mockEvents[3]);

            eventStreamBuffer.offerEvent(mockEvents[3]);
            expect(eventStreamBuffer._streamBuffer.length).toEqual(3);
            expect(eventStreamBuffer._streamBuffer[0]).toEqual(mockEvents[1]);
            expect(eventStreamBuffer._streamBuffer[1]).toEqual(mockEvents[2]);
            expect(eventStreamBuffer._streamBuffer[2]).toEqual(mockEvents[3]);
            expect(eventStreamBuffer._offeredEventsPool.length).toEqual(0);

            expect(eventStreamBuffer.updatedAt).toEqual(0);
            expect(mockOptions.onOfferEventError).toHaveBeenCalledWith(mockOptions.bucket, mockOptions.channel);
            expect(mockOptions.onOfferEvent).not.toHaveBeenCalled();
        });

        it('should not add an event on both the buffer and the pool, trigger onOfferEventError, and not trigger onOfferEvent callback if the streamRevision of the input event is invalid', () => {
            const mockEvents = [{
                streamId: 'mockEventStreamId1',
                streamRevision: 0
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 1
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 2
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 3
            }];
            spyOn(mockOptions, 'onOfferEventError').and.callThrough();
            spyOn(mockOptions, 'onOfferEvent').and.callThrough();

            const eventStreamBuffer = new SubscriptionEventStreamBuffer(mockOptions);
            eventStreamBuffer.updatedAt = 0; // Force set the updatedAt to properly assert later
            eventStreamBuffer._streamBuffer.push(mockEvents[1]);
            eventStreamBuffer._streamBuffer.push(mockEvents[2]);
            eventStreamBuffer._streamBuffer.push(mockEvents[3]);

            eventStreamBuffer.offerEvent({
                invalidProperty: 'test'
            });
            expect(eventStreamBuffer._streamBuffer.length).toEqual(3);
            expect(eventStreamBuffer._streamBuffer[0]).toEqual(mockEvents[1]);
            expect(eventStreamBuffer._streamBuffer[1]).toEqual(mockEvents[2]);
            expect(eventStreamBuffer._streamBuffer[2]).toEqual(mockEvents[3]);
            expect(eventStreamBuffer._offeredEventsPool.length).toEqual(0);

            expect(eventStreamBuffer.updatedAt).toEqual(0);
            expect(mockOptions.onOfferEventError).toHaveBeenCalledWith(mockOptions.bucket, mockOptions.channel);
            expect(mockOptions.onOfferEvent).not.toHaveBeenCalled();
        });

        it('should trigger onInactive callback once the ttl threshold has been reached if an event was added to the buffer and no further changes were made', (done) => {
            const mockEvents = [{
                streamId: 'mockEventStreamId1',
                streamRevision: 0
            }];

            const mockOptionsWithTestTTL = {
                es: { type: 'mockES' },
                query: {
                    aggregate: 'mockAggregate',
                    aggregateId: 'mockAggregateId',
                    context: 'mockContext'
                },
                channel: 'mockContext.mockAggregate.mockAggregateId',
                bucket: 'mockBucket',
                bufferCapacity: 3,
                poolCapacity: 5,
                ttl: 50,
                onOfferEvent: function() {},
                onInactive: function() {},
                onClose: function() {}
            };
            spyOn(mockOptionsWithTestTTL, 'onInactive').and.callThrough();

            const eventStreamBuffer = new SubscriptionEventStreamBuffer(mockOptionsWithTestTTL);

            eventStreamBuffer.offerEvent(mockEvents[0]);

            expect(mockOptionsWithTestTTL.onInactive).not.toHaveBeenCalled();

            setTimeout(() => {
                expect(mockOptionsWithTestTTL.onInactive).toHaveBeenCalledTimes(1);
                expect(mockOptionsWithTestTTL.onInactive).toHaveBeenCalledWith(mockOptions.bucket, mockOptions.channel);
                done();
            }, 100);
        }, 150);
    });

    describe('getLatestRevision', () => {
        it('should return the latest revision in the buffer if the buffer has events', () => {
            const mockEvents = [{
                streamId: 'mockEventStreamId1',
                streamRevision: 0
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 1
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 2
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 3
            }];

            const eventStreamBuffer = new SubscriptionEventStreamBuffer(mockOptions);
            eventStreamBuffer._streamBuffer.push(mockEvents[0]);
            eventStreamBuffer._streamBuffer.push(mockEvents[1]);
            eventStreamBuffer._offeredEventsPool.queue(mockEvents[2]);
            eventStreamBuffer._offeredEventsPool.queue(mockEvents[3]);

            expect(eventStreamBuffer.getLatestRevision()).toEqual(1);
        });

        it('should return -1 if the buffer has no events', () => {
            const mockEvents = [{
                streamId: 'mockEventStreamId1',
                streamRevision: 0
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 1
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 2
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 3
            }];

            const eventStreamBuffer = new SubscriptionEventStreamBuffer(mockOptions);
            eventStreamBuffer._offeredEventsPool.queue(mockEvents[2]);
            eventStreamBuffer._offeredEventsPool.queue(mockEvents[3]);

            expect(eventStreamBuffer.getLatestRevision()).toEqual(-1);
        });

        it('should return -1 if the buffer has been cleaned up', () => {
            const eventStreamBuffer = new SubscriptionEventStreamBuffer(mockOptions);

            eventStreamBuffer.close();

            expect(eventStreamBuffer.getLatestRevision()).toEqual(-1);
        });
    });

    describe('getOldestRevision', () => {
        it('should return the oldest revision in the buffer', () => {
            const mockEvents = [{
                streamId: 'mockEventStreamId1',
                streamRevision: 0
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 1
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 2
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 3
            }];

            const eventStreamBuffer = new SubscriptionEventStreamBuffer(mockOptions);
            eventStreamBuffer._streamBuffer.push(mockEvents[0]);
            eventStreamBuffer._streamBuffer.push(mockEvents[1]);
            eventStreamBuffer._offeredEventsPool.queue(mockEvents[2]);
            eventStreamBuffer._offeredEventsPool.queue(mockEvents[3]);

            expect(eventStreamBuffer.getOldestRevision()).toEqual(0);
        });

        it('should return -1 if the buffer has no events', () => {
            const mockEvents = [{
                streamId: 'mockEventStreamId1',
                streamRevision: 0
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 1
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 2
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 3
            }];

            const eventStreamBuffer = new SubscriptionEventStreamBuffer(mockOptions);
            eventStreamBuffer._offeredEventsPool.queue(mockEvents[2]);
            eventStreamBuffer._offeredEventsPool.queue(mockEvents[3]);

            expect(eventStreamBuffer.getOldestRevision()).toEqual(-1);
        });

        it('should return -1 if the buffer has been cleaned up', () => {
            const eventStreamBuffer = new SubscriptionEventStreamBuffer(mockOptions);

            eventStreamBuffer.close();

            expect(eventStreamBuffer.getOldestRevision()).toEqual(-1);
        });
    });

    describe('getAllEventsInBuffer', () => {
        it('should return an array containing all the events in the buffer if the buffer has events', () => {
            const mockEvents = [{
                streamId: 'mockEventStreamId1',
                streamRevision: 0
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 1
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 2
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 3
            }];

            const eventStreamBuffer = new SubscriptionEventStreamBuffer(mockOptions);
            eventStreamBuffer._streamBuffer.push(mockEvents[0]);
            eventStreamBuffer._streamBuffer.push(mockEvents[1]);
            eventStreamBuffer._offeredEventsPool.queue(mockEvents[2]);
            eventStreamBuffer._offeredEventsPool.queue(mockEvents[3]);

            expect(eventStreamBuffer.getAllEventsInBuffer()).toEqual([mockEvents[0], mockEvents[1]]);
        });

        it('should return an empty array if the buffer has no events', () => {
            const eventStreamBuffer = new SubscriptionEventStreamBuffer(mockOptions);

            expect(eventStreamBuffer.getAllEventsInBuffer()).toEqual([]);
        });

        it('should return an empty array if the buffer has been cleaned up', () => {
            const eventStreamBuffer = new SubscriptionEventStreamBuffer(mockOptions);

            eventStreamBuffer.close();

            expect(eventStreamBuffer.getAllEventsInBuffer()).toEqual([]);
        });
    });

    describe('getEventsInBuffer', () => {
        it('should return the proper events array, given that both revMin and revMax are within the bounds of the buffer events', () => {
            const mockEvents = [{
                streamId: 'mockEventStreamId1',
                streamRevision: 0
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 1
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 2
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 3
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 4
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 5
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 6
            }];

            const eventStreamBuffer = new SubscriptionEventStreamBuffer(mockOptions);
            eventStreamBuffer._streamBuffer = mockEvents;

            const result = eventStreamBuffer.getEventsInBuffer(2, 5);

            expect(result).toEqual(mockEvents.slice(2, 5));
        });

        it('should return the proper events array, given that revMin is within the bounds of the buffer events, and revMax is newer than the buffer latest revision', () => {
            const mockEvents = [{
                streamId: 'mockEventStreamId1',
                streamRevision: 0
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 1
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 2
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 3
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 4
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 5
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 6
            }];

            const eventStreamBuffer = new SubscriptionEventStreamBuffer(mockOptions);
            eventStreamBuffer._streamBuffer = mockEvents;

            const result = eventStreamBuffer.getEventsInBuffer(4, 10);

            expect(result).toEqual(mockEvents.slice(4, 7));
        });

        it('should return an empty array, given that revMax is within the bounds of the buffer events, and revMin is older than the buffer oldest revision', () => {
            const mockEvents = [{
                streamId: 'mockEventStreamId1',
                streamRevision: 5
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 6
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 7
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 8
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 9
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 10
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 11
            }];

            const eventStreamBuffer = new SubscriptionEventStreamBuffer(mockOptions);
            eventStreamBuffer._streamBuffer = mockEvents;

            const result = eventStreamBuffer.getEventsInBuffer(3, 8);

            expect(result).toEqual([]);
        });

        it('should return an empty array, given that both revMin and revMax are older than the buffer oldest revision', () => {
            const mockEvents = [{
                streamId: 'mockEventStreamId1',
                streamRevision: 8
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 9
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 10
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 11
            }];

            const eventStreamBuffer = new SubscriptionEventStreamBuffer(mockOptions);
            eventStreamBuffer._streamBuffer = mockEvents;

            const result = eventStreamBuffer.getEventsInBuffer(3, 6);

            expect(result).toEqual([]);
        });

        it('should return an empty array, given that both revMin and revMax are newer than the buffer latest revision', () => {
            const mockEvents = [{
                streamId: 'mockEventStreamId1',
                streamRevision: 8
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 9
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 10
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 11
            }];

            const eventStreamBuffer = new SubscriptionEventStreamBuffer(mockOptions);
            eventStreamBuffer._streamBuffer = mockEvents;

            const result = eventStreamBuffer.getEventsInBuffer(13, 16);

            expect(result).toEqual([]);
        });
    });

    describe('getEventsInBufferAsStream', () => {
        it('should return an eventStream with the proper events array, given that both revMin and revMax are within the bounds of the buffer events', () => {
            const mockEvents = [{
                streamId: 'mockEventStreamId1',
                streamRevision: 0
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 1
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 2
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 3
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 4
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 5
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 6
            }];

            const eventStreamBuffer = new SubscriptionEventStreamBuffer(mockOptions);
            eventStreamBuffer._streamBuffer = mockEvents;
            const expectedEventStream = new EventStream(mockOptions.es, mockOptions.query, mockEvents.slice(2, 5));

            const result = eventStreamBuffer.getEventsInBufferAsStream(2, 5);

            expect(result).toEqual(expectedEventStream);
        });

        it('should return an eventStream with the proper events array, given that revMin is within the bounds of the buffer events, and revMax is newer than the buffer latest revision', () => {
            const mockEvents = [{
                streamId: 'mockEventStreamId1',
                streamRevision: 0
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 1
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 2
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 3
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 4
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 5
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 6
            }];

            const eventStreamBuffer = new SubscriptionEventStreamBuffer(mockOptions);
            eventStreamBuffer._streamBuffer = mockEvents;
            const expectedEventStream = new EventStream(mockOptions.es, mockOptions.query, mockEvents.slice(4, 7));

            const result = eventStreamBuffer.getEventsInBufferAsStream(4, 10);

            expect(result).toEqual(expectedEventStream);
        });

        it('should return an eventStream with an empty array, given that revMax is within the bounds of the buffer events, and revMin is older than the buffer oldest revision', () => {
            const mockEvents = [{
                streamId: 'mockEventStreamId1',
                streamRevision: 5
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 6
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 7
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 8
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 9
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 10
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 11
            }];

            const eventStreamBuffer = new SubscriptionEventStreamBuffer(mockOptions);
            eventStreamBuffer._streamBuffer = mockEvents;
            const expectedEventStream = new EventStream(mockOptions.es, mockOptions.query, []);

            const result = eventStreamBuffer.getEventsInBufferAsStream(3, 8);

            expect(result).toEqual(expectedEventStream);
        });

        it('should return an eventStream with an empty array, given that both revMin and revMax are older than the buffer oldest revision', () => {
            const mockEvents = [{
                streamId: 'mockEventStreamId1',
                streamRevision: 8
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 9
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 10
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 11
            }];

            const eventStreamBuffer = new SubscriptionEventStreamBuffer(mockOptions);
            eventStreamBuffer._streamBuffer = mockEvents;
            const expectedEventStream = new EventStream(mockOptions.es, mockOptions.query, []);

            const result = eventStreamBuffer.getEventsInBufferAsStream(3, 6);

            expect(result).toEqual(expectedEventStream);
        });

        it('should return an eventStream with an empty array, given that both revMin and revMax are newer than the buffer latest revision', () => {
            const mockEvents = [{
                streamId: 'mockEventStreamId1',
                streamRevision: 8
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 9
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 10
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 11
            }];

            const eventStreamBuffer = new SubscriptionEventStreamBuffer(mockOptions);
            eventStreamBuffer._streamBuffer = mockEvents;
            const expectedEventStream = new EventStream(mockOptions.es, mockOptions.query, []);

            const result = eventStreamBuffer.getEventsInBufferAsStream(13, 16);

            expect(result).toEqual(expectedEventStream);
        });
    });

    describe('close', () => {
        it('should clear the buffer and the pool', () => {
            const mockEvents = [{
                streamId: 'mockEventStreamId1',
                streamRevision: 0
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 1
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 2
            },{
                streamId: 'mockEventStreamId1',
                streamRevision: 3
            }];

            const eventStreamBuffer = new SubscriptionEventStreamBuffer(mockOptions);
            eventStreamBuffer._streamBuffer.push(mockEvents[0]);
            eventStreamBuffer._streamBuffer.push(mockEvents[1]);
            eventStreamBuffer._offeredEventsPool.queue(mockEvents[2]);
            eventStreamBuffer._offeredEventsPool.queue(mockEvents[3]);

            eventStreamBuffer.close();

            expect(eventStreamBuffer._streamBuffer).toEqual(null);
            expect(eventStreamBuffer._offeredEventsPool).toEqual(null);
        });

        it('should trigger onClose callback if it is defined in the options', () => {
            spyOn(mockOptions, 'onClose').and.callThrough();
            const eventStreamBuffer = new SubscriptionEventStreamBuffer(mockOptions);

            eventStreamBuffer.close();

            expect(mockOptions.onClose).toHaveBeenCalledWith(mockOptions.bucket, mockOptions.channel);
        });

        it('should clear the close ttl timer and trigger onClose callback only once if both onClose and ttl are defined in the options', (done) => {
            const mockEvents = [{
                streamId: 'mockEventStreamId1',
                streamRevision: 0
            }];

            const mockOptionsWithTestTTL = {
                es: { type: 'mockES' },
                query: {
                    aggregate: 'mockAggregate',
                    aggregateId: 'mockAggregateId',
                    context: 'mockContext'
                },
                channel: 'mockContext.mockAggregate.mockAggregateId',
                bucket: 'mockBucket',
                bufferCapacity: 3,
                poolCapacity: 5,
                ttl: 50,
                onOfferEvent: function() {},
                onClose: function() {}
            };
            spyOn(mockOptionsWithTestTTL, 'onClose').and.callThrough();

            const eventStreamBuffer = new SubscriptionEventStreamBuffer(mockOptionsWithTestTTL);

            eventStreamBuffer.offerEvent(mockEvents[0]);
            eventStreamBuffer.close();

            setTimeout(() => {
                expect(mockOptionsWithTestTTL.onClose).toHaveBeenCalledTimes(1);
                expect(mockOptionsWithTestTTL.onClose).toHaveBeenCalledWith(mockOptions.bucket, mockOptions.channel);
                done();
            }, 100);
        }, 150);
    });
});
