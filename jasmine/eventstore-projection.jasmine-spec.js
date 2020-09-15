let EventStoreWithProjection = require('../lib/eventstore-projections/eventstore-projection');
const BlueBird = require('bluebird');
const StreamBuffer = require('../lib/eventStreamBuffer');
const shortid = require('shortid');


describe('eventstore-projection tests', () => {
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


    beforeEach((done) => {
        const InMemoryStore = require('../lib/databases/inmemory');
        const JobsManager = require('./test-doubles/fakes/jobs-manager.fake');
        const EventstorePlaybackListInMemoryStore = require('./test-doubles/fakes/eventstore-playbacklist-inmemory-store.fake');
        const DistributedLock = require('./test-doubles/mocks/distributed-lock.mock');

        options = {
            pollingMaxRevisions: 10,
            pollingTimeout: 10, // so that polling is immediate
            eventCallbackTimeout: 0,
            projectionGroup: shortid.generate(), // NOTE: different projection group per test. still need to investigate why having the same projection group fails the test
            enableProjection: true,
            listStore: {
                host: 'host',
                port: 'port',
                database: 'database',
                user: 'user',
                password: 'password'
            },
            stateContextName: 'vehicle'
        };

        eventsDataStore = new InMemoryStore(options);
        jobsManager = new JobsManager();
        playbackListStore = new EventstorePlaybackListInMemoryStore();
        distributedLock = DistributedLock();
        esWithProjection = new EventStoreWithProjection(options, eventsDataStore, jobsManager, distributedLock, playbackListStore, playbackListViewStore);

        esWithProjection.pollingGetEventStreamAsync = async function(query, comparer, timeout) {
            if (!comparer) {
                comparer = () => false;
            }

            let delay = parseInt(timeout);
            if (isNaN(delay)) {
                delay = 1000;
            }

            const sleep = async function(timeout) {
                return new Promise((resolve) => {
                    setTimeout(resolve, timeout);
                })
            }

            const startTime = Date.now();
            let stream = null;
            do {
                stream = await esWithProjection.getEventStreamAsync(query);

                if (!comparer(stream)) {
                    // sleep every 10ms
                    await sleep(10);
                } else {
                    break;
                }

            } while (Date.now() - startTime < delay);

            return stream;
        };
        BlueBird.promisifyAll(esWithProjection);

        esWithProjection.initAsync();
        done();
    });

    describe('validating params', () => {
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
    });

    describe('non-validation actions', () => {
        describe('ensuring that only one projection event is created if multiple instances are created', () => {
            it('should call lock of the distributedLock', (done) => {
                const query = {
                    context: 'the_context'
                };

                const projectionId = 'the_projection_id';

                const lockKey = `projection-group:${options.projectionGroup}:projection:${projectionId}`;
                esWithProjection.project({
                    projectionId: projectionId,
                    query: query
                }, function(error) {
                    expect(error).toBeUndefined();
                    expect(distributedLock.lock).toHaveBeenCalledWith(lockKey);
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

                esWithProjection.project({
                    projectionId: projectionId,
                    query: query
                }, function(error) {
                    expect(error).toBeUndefined();
                    expect(distributedLock.unlock).toHaveBeenCalledWith(lockToken);
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

        describe('project', () => {
            let projection;
            describe('playbacklist operations', () => {
                beforeEach(async (done) => {
                    const projectionId = 'the_projection_id';
                    // arrange 
                    projection = {
                        projectionId: projectionId,
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
                            vehicle_reserve_price_updated: function(state, event, funcs, done) {
                                funcs.getPlaybackList('vehicle_list', function(err, playbackList) {
                                    playbackList.get(event.aggregateId, function(err, oldData) {
                                        const eventPayload = event.payload.payload;
                                        const newData = {
                                            reservePrice: eventPayload.reservePrice
                                        };

                                        playbackList.update(event.aggregateId, event.streamRevision, oldData.data, newData, {}, done);
                                    });
                                });
                            },
                            vehicle_deleted: function(state, event, funcs, done) {
                                funcs.getPlaybackList('vehicle_list', function(err, playbackList) {
                                    playbackList.delete(event.aggregateId, done);
                                });
                            },
                            vehicle_listed: function(state, event, funcs, done) {
                                const targetQuery = {
                                    context: 'auction',
                                    aggregate: 'vehicle',
                                    aggregateId: event.aggregateId
                                };

                                funcs.emit(targetQuery, event.payload, done);
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
                        outputState: 'true',
                        partitionBy: ''
                    };
                    await esWithProjection.projectAsync(projection);
                    await esWithProjection.startAllProjections();

                    done();
                })

                describe('playback list add', () => {
                    it('should add to item when an item is added to the playbacklist', async (done) => {
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
                        const data = await playbackListStore.pollingGet(projection.playbackList.name, 'vehicle_1', (item) => !!item);
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

                describe('playback list updates', () => {
                    let stream;
                    beforeEach(async (done) => {
                        stream = await esWithProjection.getEventStreamAsync({
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

                        const data = await playbackListStore.pollingGet(projection.playbackList.name, 'vehicle_1', (item) => !!item);

                        // get again
                        stream = await esWithProjection.getEventStreamAsync({
                            aggregateId: 'vehicle_1',
                            aggregate: 'vehicle', // optional
                            context: 'vehicle' // optional
                        });

                        done();
                    })

                    it('should update the item when an item is updated to the playbacklist', async (done) => {
                        // arrange 
                        // act
                        stream.addEvent({
                            name: 'vehicle_reserve_price_updated',
                            payload: {
                                vehicleId: 'vehicle_1',
                                reservePrice: 2233
                            }
                        });
                        stream.commit();

                        // assert
                        const data = await playbackListStore.pollingGet(projection.playbackList.name, 'vehicle_1', (item) => {
                            return item && item.data && item.data.reservePrice > 0;
                        });
                        expect(data).toEqual({
                            rowId: 'vehicle_1',
                            revision: 1,
                            data: {
                                vehicleId: 'vehicle_1',
                                year: 2012,
                                make: 'Honda',
                                model: 'Jazz',
                                mileage: 12345,
                                reservePrice: 2233
                            },
                            meta: {}
                        })
                        done();
                    });

                    it('should delete the item when an item is deleted from the playbacklist', async (done) => {
                        // arrange 
                        // act
                        stream.addEvent({
                            name: 'vehicle_deleted',
                            payload: {
                                vehicleId: 'vehicle_1'
                            }
                        });
                        stream.commit();

                        // assert
                        const data = await playbackListStore.pollingGet(projection.playbackList.name, 'vehicle_1', (item) => {
                            return !item;
                        });
                        expect(data).toBeUndefined();
                        done();
                    });
                })

            })

            describe('emit', () => {
                beforeEach(async (done) => {
                    const projectionId = 'the_projection_id';
                    // arrange 
                    projection = {
                        projectionId: projectionId,
                        query: {
                            context: 'vehicle'
                        },
                        playbackInterface: {
                            vehicle_listed: function(state, event, funcs, done) {
                                const targetQuery = {
                                    context: 'auction',
                                    aggregate: 'vehicle',
                                    aggregateId: event.aggregateId
                                };

                                funcs.emit(targetQuery, event.payload, done);
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
                        outputState: 'true',
                        partitionBy: ''
                    };
                    await esWithProjection.projectAsync(projection);
                    await esWithProjection.startAllProjections();

                    done();
                })

                it('should save to the target query', async (done) => {
                    const stream = await esWithProjection.getEventStreamAsync({
                        aggregateId: 'vehicle_1',
                        aggregate: 'vehicle', // optional
                        context: 'vehicle' // optional
                    });

                    stream.addEvent({
                        name: 'vehicle_listed',
                        payload: {
                            vehicleId: 'vehicle_1'
                        }
                    });
                    stream.commit();

                    const newStream = await esWithProjection.pollingGetEventStreamAsync({
                        aggregateId: 'vehicle_1',
                        aggregate: 'vehicle', // optional
                        context: 'auction' // optional
                    }, (stream) => stream.events.length > 0, 1000);

                    expect(newStream.events.length).toEqual(1);
                    done();
                });
            })

            describe('outputting states', () => {
                beforeEach(async (done) => {
                    const projectionId = 'the_projection_id';
                    // arrange 
                    projection = {
                        projectionId: projectionId,
                        query: {
                            context: 'vehicle'
                        },
                        playbackInterface: {
                            vehicle_created: function(state, event, funcs, done) {
                                const eventPayload = event.payload.payload;
                                state.year = eventPayload.year;
                                state.make = eventPayload.make;
                                state.model = eventPayload.model;
                                state.mileage = eventPayload.mileage;
                                state.vehicleId = eventPayload.vehicleId;
                                done();
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
                        outputState: 'true',
                        partitionBy: ''
                    };
                    done();
                })

                it('should create one state for the projection if partition is not defined', async (done) => {
                    projection.partitionBy = '';
                    await esWithProjection.projectAsync(projection);
                    await esWithProjection.startAllProjections();

                    const event = {
                        name: 'vehicle_created',
                        payload: {
                            vehicleId: 'vehicle_1',
                            year: 2012,
                            make: 'Honda',
                            model: 'Jazz',
                            mileage: 12345
                        }
                    };

                    const stream = await esWithProjection.getEventStreamAsync({
                        aggregateId: 'vehicle_1',
                        aggregate: 'vehicle', // optional
                        context: 'vehicle' // optional
                    });

                    stream.addEvent(event);
                    stream.commit();

                    const stateStream = await esWithProjection.pollingGetEventStreamAsync({
                        context: 'vehicle',
                        aggregate: 'states',
                        aggregateId: 'the_projection_id-result'

                    }, (stream) => stream.events.length > 0, 1000);

                    const expectedState = {
                        name: 'vehicle_created',
                        payload: {
                            vehicleId: stateStream.events[0].payload.vehicleId,
                            year: stateStream.events[0].payload.year,
                            make: stateStream.events[0].payload.make,
                            model: stateStream.events[0].payload.model,
                            mileage: stateStream.events[0].payload.mileage
                        }
                    }
                    expect(expectedState).toEqual(event);

                    done();
                })
                
                it('should create one state for each stream if partition is set to stream', async (done) => {
                    projection.partitionBy = 'stream';
                    await esWithProjection.projectAsync(projection);
                    await esWithProjection.startAllProjections();

                    const numberOfCarsToExpect = 2;
                    for (let index = 1; index <= numberOfCarsToExpect; index++) {
                        const event = {
                            name: 'vehicle_created',
                            payload: {
                                vehicleId: `vehicle_${index}`,
                                year: 2012,
                                make: 'Honda',
                                model: 'Jazz',
                                mileage: 12345
                            }
                        };
    
                        const stream = await esWithProjection.getEventStreamAsync({
                            aggregateId: `vehicle_${index}`,
                            aggregate: 'vehicle', // optional
                            context: 'vehicle' // optional
                        });
    
                        stream.addEvent(event);
                        stream.commit();
                    }
                    

                    for (let index = 1; index <= numberOfCarsToExpect; index++) {
                        const event = {
                            name: 'vehicle_created',
                            payload: {
                                vehicleId: `vehicle_${index}`,
                                year: 2012,
                                make: 'Honda',
                                model: 'Jazz',
                                mileage: 12345
                            }
                        };

                        const stateStream = await esWithProjection.pollingGetEventStreamAsync({
                            context: 'vehicle',
                            aggregate: 'states',
                            aggregateId: `the_projection_id-vehicle-vehicle-vehicle_${index}-result`
    
                        }, (stream) => stream.events.length > 0, 1000);
    
                        const expectedState = {
                            name: 'vehicle_created',
                            payload: {
                                vehicleId: stateStream.events[0].payload.vehicleId,
                                year: stateStream.events[0].payload.year,
                                make: stateStream.events[0].payload.make,
                                model: stateStream.events[0].payload.model,
                                mileage: stateStream.events[0].payload.mileage
                            }
                        }
                        expect(expectedState).toEqual(event);
                    }
                    

                    done();
                })
            
                it('should create a state for each unique id returned by partitionBy function', async (done) => {
                    projection.partitionBy = (event) => {
                        console.log(event);
                        return event.payload.payload.vehicleId;
                    };
                    await esWithProjection.projectAsync(projection);
                    await esWithProjection.startAllProjections();

                    const numberOfCarsToExpect = 7;
                    for (let index = 1; index <= numberOfCarsToExpect; index++) {
                        const event = {
                            name: 'vehicle_created',
                            payload: {
                                vehicleId: `vehicle_${index}`,
                                year: 2012,
                                make: 'Honda',
                                model: 'Jazz',
                                mileage: 12345
                            }
                        };
    
                        const stream = await esWithProjection.getEventStreamAsync({
                            aggregateId: `vehicle_${index}`,
                            aggregate: 'vehicle', // optional
                            context: 'vehicle' // optional
                        });

                        BlueBird.promisifyAll(stream);
    
                        stream.addEvent(event);
                        await stream.commitAsync();
                    }
                    

                    for (let index = 1; index <= numberOfCarsToExpect; index++) {
                        const event = {
                            name: 'vehicle_created',
                            payload: {
                                vehicleId: `vehicle_${index}`,
                                year: 2012,
                                make: 'Honda',
                                model: 'Jazz',
                                mileage: 12345
                            }
                        };

                        const stateStream = await esWithProjection.pollingGetEventStreamAsync({
                            context: 'vehicle',
                            aggregate: 'states',
                            aggregateId: `the_projection_id-vehicle_${index}-result`
    
                        }, (stream) => stream.events.length > 0, 1000);
    
                        const expectedState = {
                            name: 'vehicle_created',
                            payload: {
                                vehicleId: stateStream.events[0].payload.vehicleId,
                                year: stateStream.events[0].payload.year,
                                make: stateStream.events[0].payload.make,
                                model: stateStream.events[0].payload.model,
                                mileage: stateStream.events[0].payload.mileage
                            }
                        }
                        expect(expectedState).toEqual(event);
                    }
                    

                    done();
                })
            
            })
        });

        describe('subscribe', () => {
            it('should receive the event when an event is added', async (done) => {
                const expectedEvent = {
                    name: 'vehicle_created',
                    payload: {
                        vehicleId: 'vehicle_1',
                        year: 2012,
                        make: 'Honda',
                        model: 'Jazz',
                        mileage: 12345
                    }
                };

                const stream = await esWithProjection.getEventStreamAsync({
                    aggregateId: 'vehicle_1',
                    aggregate: 'vehicle', // optional
                    context: 'vehicle' // optional
                });

                stream.addEvent(expectedEvent);
                stream.commit();

                esWithProjection.subscribe({
                    aggregateId: 'vehicle_1',
                    aggregate: 'vehicle', // optional
                    context: 'vehicle' // optional
                }, 0, (err, event) => {
                    expect(event.payload).toEqual(expectedEvent);
                    done();
                });
            });

            it('should receive the correct number of events given a revision', async (done) => {
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
                stream.addEvent({
                    name: 'vehicle_updated',
                    payload: {
                        vehicleId: 'vehicle_1',
                        update: 'update1'
                    }
                });
                stream.addEvent({
                    name: 'vehicle_updated',
                    payload: {
                        vehicleId: 'vehicle_1',
                        update: 'update2'
                    }
                });
                stream.addEvent({
                    name: 'vehicle_updated',
                    payload: {
                        vehicleId: 'vehicle_1',
                        update: 'update3'
                    }
                });
                stream.commit();

                let expectedStreamRevision = 2;
                esWithProjection.subscribe({
                    aggregateId: 'vehicle_1',
                    aggregate: 'vehicle', // optional
                    context: 'vehicle' // optional
                }, 2, (err, event) => {
                    console.log('event.streamRevision', event.streamRevision);
                    expect(event.streamRevision == expectedStreamRevision);
                    expectedStreamRevision++;

                    if (expectedStreamRevision == 3) {
                        done();
                    }
                });
            });
        });
    })
})