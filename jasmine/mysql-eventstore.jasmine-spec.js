const MysqlEventStore = require('../lib/databases/mysql');
const EventStoreDuplicateError = require('../lib/databases/errors/EventStoreDuplicateError');
const _ = require('lodash');

// TODO: will change to a narrow integration test 
xdescribe('mysql-eventstore', () => {
    let mockOptions;
    const mockError = new Error('Mock Error');
    let mockMysql;
    let mockPool;
    let mockConnection;

    beforeEach(() => {
        mockMysql = jasmine.createSpyObj('mockMysql', ['createPool']);
        mockPool = jasmine.createSpyObj('mockPool', ['getConnection']);
        mockConnection = jasmine.createSpyObj('mockConnection', ['beginTransaction', 'query', 'commit', 'rollback', 'release']);

        mockOptions = {
            host: 'mockHost',
            port: 'mockPort',
            user: 'mockUser',
            password: 'mockPassword',
            database: 'eventstore',
            connectionPoolLimit: 10,
            usingEventDispatcher: true,
            mysql: mockMysql
        };
    });

    describe('connect', () => {
        it('should call mysql.createPool with proper params and assign pool to property and return self in callback', (done) => {
            const expectedOptions = {
                host: mockOptions.host,
                port: mockOptions.port,
                user: mockOptions.user,
                password: mockOptions.password,
                database: mockOptions.database,
                connectionLimit: mockOptions.connectionPoolLimit
            };
            mockMysql.createPool.and.returnValue(mockPool);

            const mysqlES = new MysqlEventStore(mockOptions);
            spyOn(mysqlES, '_initQuery').and.callFake(function(query, cb) { cb() });
            mysqlES.connect((err, es) => {
                expect(mysqlES.pool).toEqual(mockPool);
                expect(es).toEqual(mysqlES);
                expect(mockMysql.createPool).toHaveBeenCalledWith(expectedOptions);
                done();
            });
        }, 250);

        it('should emit connect event', (done) => {
            const connectSpy = jasmine.createSpy('connectSpy');
            mockMysql.createPool.and.callFake(function() {});

            const mysqlES = new MysqlEventStore(mockOptions);
            mysqlES.on('connect', connectSpy);
            spyOn(mysqlES, '_initQuery').and.callFake(function(query, cb) { cb() });
            mysqlES.connect(() => {
                expect(connectSpy).toHaveBeenCalled();
                done();
            });
        }, 250);

        it('should return error if mysql.createPool threw an error', (done) => {
            const expectedOptions = {
                host: mockOptions.host,
                port: mockOptions.port,
                user: mockOptions.user,
                password: mockOptions.password,
                database: mockOptions.database,
                connectionLimit: mockOptions.connectionPoolLimit
            };
            mockMysql.createPool.and.callFake(function() {
                throw mockError;
            });

            const mysqlES = new MysqlEventStore(mockOptions);
            spyOn(mysqlES, '_initQuery').and.callFake(function(query, cb) { cb() });

            mysqlES.connect((error) => {
                expect(mockMysql.createPool).toHaveBeenCalledWith(expectedOptions);
                expect(error).toEqual(mockError);
                done();
            });
            
        }, 250);

        // TODO: will create a different integration test for mysql
        xit('should initialize events, undispatched_events, and snapshots tables', (done) => {
            mockMysql.createPool.and.returnValue(mockPool);
            const mysqlES = new MysqlEventStore(mockOptions);
            mockPool.getConnection.and.callFake(function(callback) {
                callback(null, mockConnection);
            });
            mockConnection.release.and.callFake(function() {});

            mockConnection.query.and.callFake(function(query, payload, callback) {
                callback(null, 'mockResults', 'mockFields');
            });

            mysqlES.connect(() => {
                const createEventsTableQuery = `CREATE TABLE IF NOT EXISTS ${mysqlES._options.database}.${mysqlES._options.eventsTableName} (` +
                    `id VARCHAR(250) NOT NULL, ` +
                    `event JSON NOT NULL, ` +
                    `aggregate_id VARCHAR(250) GENERATED ALWAYS AS (json_unquote(json_extract(event,'$.aggregateId'))) VIRTUAL, ` +
                    `aggregate VARCHAR(45) GENERATED ALWAYS AS (json_unquote(json_extract(event,'$.aggregate'))) VIRTUAL, ` +
                    `context VARCHAR(45) GENERATED ALWAYS AS (json_unquote(json_extract(event,'$.context'))) VIRTUAL, ` +
                    `stream_revision INT(11) GENERATED ALWAYS AS (json_unquote(json_extract(event,'$.streamRevision'))) VIRTUAL, ` +
                    `commit_stamp BIGINT(20) GENERATED ALWAYS AS (json_unquote(json_extract(event,'$.commitStamp'))) VIRTUAL, ` +
                    `commit_sequence INT(11) GENERATED ALWAYS AS (json_unquote(json_extract(event,'$.commitSequence'))) VIRTUAL, ` +
                    `event_type VARCHAR(100) GENERATED ALWAYS AS (json_unquote(json_extract(event,'$.payload.name'))) VIRTUAL, ` +
                    `PRIMARY KEY (id), ` + 
                    `INDEX idx_get_events_aggregate_id_stream_revision (aggregate_id, stream_revision),` +
                    `INDEX idx_get_events_aggregate_context (aggregate, context, commit_stamp, stream_revision, commit_sequence),` +
                    `INDEX idx_get_events_context (context),` +
                    `INDEX idx_get_events_commit_stamp (commit_stamp),` +
                    `INDEX idx_get_last_events_aggregate_id (aggregate_id ASC, commit_stamp DESC, stream_revision DESC, commit_sequence DESC)` +
                `)`;

                const createUndispatchedEventsTableQuery = `CREATE TABLE IF NOT EXISTS ${mysqlES._options.database}.${mysqlES._options.undispatchedEventsTableName} (` +
                    `id VARCHAR(250) NOT NULL, ` +
                    `event JSON NOT NULL, ` +
                    `aggregate_id VARCHAR(250) GENERATED ALWAYS AS (json_unquote(json_extract(event,'$.aggregateId'))) VIRTUAL, ` +
                    `aggregate VARCHAR(45) GENERATED ALWAYS AS (json_unquote(json_extract(event,'$.aggregate'))) VIRTUAL, ` +
                    `context VARCHAR(45) GENERATED ALWAYS AS (json_unquote(json_extract(event,'$.context'))) VIRTUAL, ` +
                    `stream_revision INT(11) GENERATED ALWAYS AS (json_unquote(json_extract(event,'$.streamRevision'))) VIRTUAL, ` +
                    `commit_stamp BIGINT(20) GENERATED ALWAYS AS (json_unquote(json_extract(event,'$.commitStamp'))) VIRTUAL, ` +
                    `commit_sequence INT(11) GENERATED ALWAYS AS (json_unquote(json_extract(event,'$.commitSequence'))) VIRTUAL, ` +
                    `PRIMARY KEY (id), ` + 
                    `INDEX idx_get_events_aggregate_id (aggregate_id),` +
                    `INDEX idx_get_events_aggregate (aggregate),` +
                    `INDEX idx_get_events_context (context),` +
                    `INDEX idx_get_events_commit_stamp (commit_stamp)` +
                `)`;

                const createSnapshotsTableQuery = `CREATE TABLE IF NOT EXISTS ${mysqlES._options.database}.${mysqlES._options.snapshotsTableName} (` +
                    `id VARCHAR(250) NOT NULL, ` +
                    `snapshot JSON NOT NULL, ` +
                    `aggregate_id VARCHAR(250) GENERATED ALWAYS AS (json_unquote(json_extract(snapshot,'$.aggregateId'))) VIRTUAL, ` +
                    `aggregate VARCHAR(45) GENERATED ALWAYS AS (json_unquote(json_extract(snapshot,'$.aggregate'))) VIRTUAL, ` +
                    `context VARCHAR(45) GENERATED ALWAYS AS (json_unquote(json_extract(snapshot,'$.context'))) VIRTUAL, ` +
                    `commit_stamp BIGINT(20) GENERATED ALWAYS AS (json_unquote(json_extract(snapshot,'$.commitStamp'))) VIRTUAL, ` +
                    `PRIMARY KEY (id), ` + 
                    `INDEX idx_get_snapshot (context, aggregate, aggregate_id, commit_stamp)` +
                `)`;

                expect(mockConnection.query).toHaveBeenCalledWith(createEventsTableQuery, [], jasmine.any(Function));
                expect(mockConnection.query).toHaveBeenCalledWith(createUndispatchedEventsTableQuery, [], jasmine.any(Function));
                expect(mockConnection.query).toHaveBeenCalledWith(createSnapshotsTableQuery, [], jasmine.any(Function));

                done();
            });
            
        }, 250);

        it('should return an error if table creation fails on events table', (done) => {
            mockMysql.createPool.and.callFake(function() {});

            const mysqlES = new MysqlEventStore(mockOptions);
            spyOn(mysqlES, '_initQuery').and.callFake(function(query, cb) {
                cb(mockError);
            });
            mysqlES.connect((error) => {
                expect(error).toEqual(mockError);
                done();
            });
        }, 250);

        it('should return an error if table creation fails on undispatched events table', (done) => {
            mockMysql.createPool.and.callFake(function() {});

            let counter = 0;
            const mysqlES = new MysqlEventStore(mockOptions);
            spyOn(mysqlES, '_initQuery').and.callFake(function(query, cb) {
                if (counter === 1) {
                    counter++;
                    cb(mockError);
                } else {
                    counter++;
                    cb();
                }
            });
            mysqlES.connect((error) => {
                expect(error).toEqual(mockError);
                done();
            });
        }, 250);

        it('should return an error if table creation fails on snapshots table', (done) => {
            mockMysql.createPool.and.callFake(function() {});

            let counter = 0;
            const mysqlES = new MysqlEventStore(mockOptions);
            spyOn(mysqlES, '_initQuery').and.callFake(function(query, cb) {
                if (counter === 2) {
                    counter++;
                    cb(mockError);
                } else {
                    counter++;
                    cb();
                }
            });
            mysqlES.connect((error) => {
                expect(error).toEqual(mockError);
                done();
            });
        }, 250);
    });

    describe('disconnect', () => {
        it('should emit disconnect event', (done) => {
            const disconnectSpy = jasmine.createSpy('disconnectSpy');
            mockMysql.createPool.and.callFake(function() {});

            const mysqlES = new MysqlEventStore(mockOptions);
            mysqlES.on('disconnect', disconnectSpy);
            mysqlES.disconnect((error) => {
                expect(disconnectSpy).toHaveBeenCalled();
                expect(error).toBeNull();
                done();
            });
        }, 250);
    });

    describe('clear', () => {
        it('should call initQuery to truncate tables', (done) => {
            const mysqlES = new MysqlEventStore(mockOptions);
            spyOn(mysqlES, '_initQuery').and.callFake(function(query, cb) { cb() });
            mysqlES.clear(() => {
                const eventsQuery = `TRUNCATE TABLE ${mysqlES._options.database}.${mysqlES._options.eventsTableName}`;
                const undispatchedEventsQuery = `TRUNCATE TABLE ${mysqlES._options.database}.${mysqlES._options.undispatchedEventsTableName}`;
                const snapshotsQuery = `TRUNCATE TABLE ${mysqlES._options.database}.${mysqlES._options.snapshotsTableName}`;

                expect(mysqlES._initQuery).toHaveBeenCalledWith(eventsQuery, jasmine.any(Function));
                expect(mysqlES._initQuery).toHaveBeenCalledWith(undispatchedEventsQuery, jasmine.any(Function));
                expect(mysqlES._initQuery).toHaveBeenCalledWith(snapshotsQuery, jasmine.any(Function));

                done();
            })
        }, 250);

        it('should return an error if table creation fails on events table', (done) => {
            mockMysql.createPool.and.callFake(function() {});

            let counter = 0;
            const mysqlES = new MysqlEventStore(mockOptions);
            spyOn(mysqlES, '_initQuery').and.callFake(function(query, cb) {
                cb(mockError);
            });
            mysqlES.clear((error) => {
                expect(error).toEqual(mockError);
                done();
            });

        }, 250);
    });

    /* All Queries below require executing via a connection obtained from the pool. Tests assert each getConnection and releaseConnection */
    /* addEvents require executing queries in a transaction. Tests assert the transaction with commit and rollback */
    describe('addEvents', () => {
        const mockContext = 'mockContext';
        const mockAggregate = 'mockAggregate';
        const mockAggregateId = 'mockAggregateId';
        let mockEvent1;
        let mockEvent2;
        let mysqlES;

        beforeEach((done) => {
            mockEvent1 = {
                id: 'mockEventId0',
                aggregateId: mockAggregateId,
                aggregate: mockAggregate,
                context: mockContext,
                streamRevision: 5,
                commitId: 'mockEventId',
                commitStamp: new Date('2020-03-06T00:28:36.728Z'),
                commitSequence: 0,
                payload: {
                    name: 'mock_event_added',
                    payload: 'mockPayload',
                    aggregateId: mockAggregateId
                }
            };
            mockEvent2 = {
                id: 'mockEventId20',
                aggregateId: mockAggregateId + '2',
                aggregate: mockAggregate + '2',
                context: mockContext + '2',
                streamRevision: 3,
                commitId: 'mockEventId2',
                commitStamp: new Date('2020-04-06T00:28:36.728Z'),
                commitSequence: 0,
                payload: {
                    name: 'mock_event_added2',
                    payload: 'mockPayload2',
                    aggregateId: mockAggregateId + '2'
                }
            };
            mockMysql.createPool.and.returnValue(mockPool);
            done();
        });

        const initMysqlES = (cb) => {
            mysqlES = new MysqlEventStore(mockOptions);
            spyOn(mysqlES, '_initQuery').and.callFake(function(query, cb) { cb() });
            mysqlES.connect(cb);
        };

        const initMysqlESNoDispatcher = (cb) => {
            const mockOptionsWithoutDispatcher = _.cloneDeep(mockOptions);
            mockOptionsWithoutDispatcher.usingEventDispatcher = false;
            mysqlES = new MysqlEventStore(mockOptionsWithoutDispatcher);
            spyOn(mysqlES, '_initQuery').and.callFake(function(query, cb) { cb() });
            mysqlES.connect(cb);
        };

        describe('pool.getConnection', () => {
            beforeEach(() => {
                mockConnection.beginTransaction.and.callFake(function(callback) {
                    callback();
                });
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, 'mockResults', 'mockFields');
                });
                mockConnection.commit.and.callFake(function(callback) {
                    callback(null);
                });
                mockConnection.release.and.callFake(function() {});
            });

            it('should call pool.getConnection with a callback', (done) => {
                const mockEvents = [_.cloneDeep(mockEvent1)];
                mockPool.getConnection.and.callFake(function(callback) {
                    callback(null, mockConnection);
                });

                initMysqlES(() => {
                    mysqlES.addEvents(mockEvents, () => {
                        expect(mockPool.getConnection).toHaveBeenCalledWith(jasmine.any(Function));
                        done();
                    });
                });
            }, 250);

            it('should throw an error if pool.getConnection returned an error', (done) => {
                const mockEvents = [_.cloneDeep(mockEvent1)];
                mockPool.getConnection.and.callFake(function(callback) {
                    callback(mockError, null);
                });

                initMysqlES(() => {
                    mysqlES.addEvents(mockEvents, (error) => {
                        expect(mockPool.getConnection).toHaveBeenCalledWith(jasmine.any(Function));
                        expect(error).toEqual(mockError);
                        done();
                    });
                });
            }, 250);

            it('should throw an error if pool.getConnection threw an error', (done) => {
                const mockEvents = [_.cloneDeep(mockEvent1)];
                mockPool.getConnection.and.callFake(function() {
                    throw mockError;
                });

                initMysqlES(() => {
                    mysqlES.addEvents(mockEvents, (error) => {
                        expect(mockPool.getConnection).toHaveBeenCalledWith(jasmine.any(Function));
                        expect(error).toEqual(mockError);
                        done();
                    });
                });
            }, 250);
        });

        describe('conn.beginTransaction', () => {
            beforeEach(() => {
                mockPool.getConnection.and.callFake(function(callback) {
                    callback(null, mockConnection);
                });
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, 'mockResults', 'mockFields');
                });
                mockConnection.commit.and.callFake(function(callback) {
                    callback(null);
                });
                mockConnection.release.and.callFake(function() {});
            });

            it('should call conn.beginTransaction with a callback', (done) => {
                const mockEvents = [_.cloneDeep(mockEvent1)];
                mockConnection.beginTransaction.and.callFake(function(callback) {
                    callback();
                });

                initMysqlES(() => {
                    mysqlES.addEvents(mockEvents, () => {
                        expect(mockConnection.beginTransaction).toHaveBeenCalledWith(jasmine.any(Function));
                        done();
                    });
                });
            }, 250);

            it('should throw an error if pool.getConnection returned an error', (done) => {
                const mockEvents = [_.cloneDeep(mockEvent1)];
                mockConnection.beginTransaction.and.callFake(function(callback) {
                    callback(mockError);
                });

                initMysqlES(() => {
                    mysqlES.addEvents(mockEvents, (error) => {
                        expect(mockConnection.beginTransaction).toHaveBeenCalledWith(jasmine.any(Function));
                        expect(error).toEqual(mockError);
                        done();
                    });
                });
            }, 250);

            it('should throw an error if pool.getConnection threw an error', (done) => {
                const mockEvents = [_.cloneDeep(mockEvent1)];
                mockConnection.beginTransaction.and.callFake(function(callback) {
                    throw mockError;
                });

                initMysqlES(() => {
                    mysqlES.addEvents(mockEvents, (error) => {
                        expect(mockConnection.beginTransaction).toHaveBeenCalledWith(jasmine.any(Function));
                        expect(error).toEqual(mockError);
                        done();
                    });
                });
            }, 250);
        });

        describe('queries', () => {
            beforeEach(() => {
                mockPool.getConnection.and.callFake(function(callback) {
                    callback(null, mockConnection);
                });
                mockConnection.beginTransaction.and.callFake(function(callback) {
                    callback();
                });
                mockConnection.commit.and.callFake(function(callback) {
                    callback(null);
                });
                mockConnection.release.and.callFake(function() {});
            });

            it('should call an INSERT query on the proper database, events, and undispatchedEvents tables for each event with proper params if options.usingEventDispatcher is true', (done) => {
                const expectedMockEvent1 = _.cloneDeep(mockEvent1);
                const expectedMockEvent2 = _.cloneDeep(mockEvent2);
                const mockEvents = [mockEvent1, mockEvent2];

                expectedMockEvent1.commitStamp = expectedMockEvent1.commitStamp.getTime();
                expectedMockEvent2.commitStamp = expectedMockEvent2.commitStamp.getTime();
                const expectedEventQueryPayloads = [{
                        id: expectedMockEvent1.id,
                        event: JSON.stringify(expectedMockEvent1)
                    },
                    {
                        id: expectedMockEvent2.id,
                        event: JSON.stringify(expectedMockEvent2)
                    }
                ];

                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, 'mockResults', 'mockFields');
                });

                initMysqlES(() => {
                    mysqlES.addEvents(mockEvents, () => {
                        expect(mockConnection.query).toHaveBeenCalledTimes(4);
                        const calls = mockConnection.query.calls.all();
                        expect(calls[0].args).toEqual([
                            `INSERT INTO ${mockOptions.database}.events SET ?`,
                            expectedEventQueryPayloads[0],
                            jasmine.any(Function)
                        ]);
                        expect(calls[1].args).toEqual([
                            `INSERT INTO ${mockOptions.database}.undispatched_events SET ?`,
                            expectedEventQueryPayloads[0],
                            jasmine.any(Function)
                        ]);
                        expect(calls[2].args).toEqual([
                            `INSERT INTO ${mockOptions.database}.events SET ?`,
                            expectedEventQueryPayloads[1],
                            jasmine.any(Function)
                        ]);
                        expect(calls[3].args).toEqual([
                            `INSERT INTO ${mockOptions.database}.undispatched_events SET ?`,
                            expectedEventQueryPayloads[1],
                            jasmine.any(Function)
                        ]);
                        done();
                    });
                });
                
            }, 250);

            it('should only call an INSERT query on the proper database, and events tables for each event with proper params if options.usingEventDispatcher is false', (done) => {
                const expectedMockEvent1 = _.cloneDeep(mockEvent1);
                const expectedMockEvent2 = _.cloneDeep(mockEvent2);
                const mockEvents = [mockEvent1, mockEvent2];

                expectedMockEvent1.commitStamp = expectedMockEvent1.commitStamp.getTime();
                expectedMockEvent2.commitStamp = expectedMockEvent2.commitStamp.getTime();

                const expectedEventQueryPayloads = [{
                        id: expectedMockEvent1.id,
                        event: JSON.stringify(expectedMockEvent1)
                    },
                    {
                        id: expectedMockEvent2.id,
                        event: JSON.stringify(expectedMockEvent2)
                    }
                ];

                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, 'mockResults', 'mockFields');
                });

                initMysqlESNoDispatcher(() => {
                    mysqlES.addEvents(mockEvents, () => {
                        expect(mockConnection.query).toHaveBeenCalledTimes(2);
                        const calls = mockConnection.query.calls.all();
                        expect(calls[0].args).toEqual([
                            `INSERT INTO ${mockOptions.database}.events SET ?`,
                            expectedEventQueryPayloads[0],
                            jasmine.any(Function)
                        ]);
                        expect(calls[1].args).toEqual([
                            `INSERT INTO ${mockOptions.database}.events SET ?`,
                            expectedEventQueryPayloads[1],
                            jasmine.any(Function)
                        ]);
                        done();
                    });
                });
            }, 250);

            it('should call conn.rollback and throw the conn.query error if conn.query returned an error', (done) => {
                const mockEvents = [_.cloneDeep(mockEvent1), _.cloneDeep(mockEvent2)];
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(mockError);
                });
                mockConnection.rollback.and.callFake(function(callback) {
                    callback(null);
                });

                initMysqlES(() => {
                    mysqlES.addEvents(mockEvents, (error) => {
                        expect(error).toEqual(mockError);
                        expect(mockConnection.query).toHaveBeenCalled();
                        expect(mockConnection.rollback).toHaveBeenCalledWith(jasmine.any(Function));
                        done();
                    });
                });
            }, 250);

            it('should call conn.rollback and throw the an EventStoreDuplicateError error if conn.query threw a duplicate event error', (done) => {
                const mockEvents = [_.cloneDeep(mockEvent1), _.cloneDeep(mockEvent2)];
                const mockDuplicateError = new Error('Mock Duplicate Error');
                mockDuplicateError.code = 'ER_DUP_ENTRY';

                mockConnection.query.and.callFake(function(query, payload, callback) {
                    throw mockDuplicateError;
                });
                mockConnection.rollback.and.callFake(function(callback) {
                    callback(null);
                });

                initMysqlES(() => {
                    mysqlES.addEvents(mockEvents, (error) => {
                        expect(error).toEqual(new EventStoreDuplicateError(mockDuplicateError));
                        expect(mockConnection.query).toHaveBeenCalled();
                        expect(mockConnection.rollback).toHaveBeenCalledWith(jasmine.any(Function));
                        done();
                    });
                });
            }, 250);

            it('should call conn.rollback and throw the conn.query error if conn.query threw a non duplicate event error', (done) => {
                const mockEvents = [_.cloneDeep(mockEvent1), _.cloneDeep(mockEvent2)];
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    throw mockError;
                });
                mockConnection.rollback.and.callFake(function(callback) {
                    callback(null);
                });

                initMysqlES(() => {
                    mysqlES.addEvents(mockEvents, (error) => {
                        expect(error).toEqual(mockError);
                        expect(mockConnection.query).toHaveBeenCalled();
                        expect(mockConnection.rollback).toHaveBeenCalledWith(jasmine.any(Function));
                        done();
                    });
                });
            }, 250);

            it('should throw the conn.query error if conn.rollback returned an error', (done) => {
                const mockEvents = [_.cloneDeep(mockEvent1), _.cloneDeep(mockEvent2)];
                const mockRollbackError = new Error('rollback error');
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(mockError);
                });
                mockConnection.rollback.and.callFake(function(callback) {
                    callback(mockRollbackError);
                });

                initMysqlES(() => {
                    mysqlES.addEvents(mockEvents, (error) => {
                        expect(mockConnection.query).toHaveBeenCalled();
                        expect(mockConnection.rollback).toHaveBeenCalledWith(jasmine.any(Function));
                        expect(error).toEqual(mockError);
                        done();
                    });
                });
            }, 250);

            it('should throw the conn.query error if conn.rollback threw an error', (done) => {
                const mockEvents = [_.cloneDeep(mockEvent1), _.cloneDeep(mockEvent2)];
                const mockRollbackError = new Error('rollback error');
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    throw mockError;
                });
                mockConnection.rollback.and.callFake(function(callback) {
                    throw mockRollbackError;
                });

                initMysqlES(() => {
                    mysqlES.addEvents(mockEvents, (error) => {
                        expect(mockConnection.query).toHaveBeenCalled();
                        expect(mockConnection.rollback).toHaveBeenCalledWith(jasmine.any(Function));
                        expect(error).toEqual(mockError);
                        done();
                    });
                });
            }, 250);
        });

        describe('conn.commitTransaction', () => {
            beforeEach(() => {
                mockPool.getConnection.and.callFake(function(callback) {
                    callback(null, mockConnection);
                });
                mockConnection.beginTransaction.and.callFake(function(callback) {
                    callback();
                });
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, 'mockResults', 'mockFields');
                });
                mockConnection.release.and.callFake(function() {});
            });

            it('should call conn.commit with a callback', (done) => {
                const mockEvents = [_.cloneDeep(mockEvent1)];
                mockConnection.commit.and.callFake(function(callback) {
                    callback(null);
                });

                initMysqlES(() => {
                    mysqlES.addEvents(mockEvents, () => {
                        expect(mockConnection.commit).toHaveBeenCalledWith(jasmine.any(Function));
                        done();
                    });
                });
            }, 250);

            it('should call conn.rollback and throw the conn.commit error if conn.commit returned an error', (done) => {
                const mockEvents = [_.cloneDeep(mockEvent1)];
                mockConnection.commit.and.callFake(function(callback) {
                    callback(mockError, null);
                });
                mockConnection.rollback.and.callFake(function(callback) {
                    callback(null);
                });

                initMysqlES(() => {
                    mysqlES.addEvents(mockEvents, (error) => {
                        expect(mockConnection.commit).toHaveBeenCalledWith(jasmine.any(Function));
                        expect(mockConnection.rollback).toHaveBeenCalledWith(jasmine.any(Function));
                        expect(error).toEqual(mockError);
                        done();
                    });
                });
            }, 250);

            it('should call conn.rollback and throw the conn.commit error if conn.commit threw an error', (done) => {
                const mockEvents = [_.cloneDeep(mockEvent1)];
                mockConnection.commit.and.callFake(function(callback) {
                    throw mockError;
                });
                mockConnection.rollback.and.callFake(function(callback) {
                    callback(null);
                });

                initMysqlES(() => {
                    mysqlES.addEvents(mockEvents, (error) => {
                        expect(mockConnection.commit).toHaveBeenCalledWith(jasmine.any(Function));
                        expect(mockConnection.rollback).toHaveBeenCalledWith(jasmine.any(Function));
                        expect(error).toEqual(mockError);
                        done();
                    });
                });
            }, 250);

            it('should throw the conn.commit error if conn.rollback returned an error', (done) => {
                const mockEvents = [_.cloneDeep(mockEvent1)];
                const mockRollbackError = new Error('rollback error');
                mockConnection.commit.and.callFake(function(callback) {
                    callback(mockError, null);
                });
                mockConnection.rollback.and.callFake(function(callback) {
                    callback(mockRollbackError);
                });

                initMysqlES(() => {
                    mysqlES.addEvents(mockEvents, (error) => {
                        expect(mockConnection.commit).toHaveBeenCalledWith(jasmine.any(Function));
                        expect(mockConnection.rollback).toHaveBeenCalledWith(jasmine.any(Function));
                        expect(error).toEqual(mockError);
                        done();
                    });
                });
            }, 250);

            it('should throw the conn.commit error if conn.rollback threw an error', (done) => {
                const mockEvents = [_.cloneDeep(mockEvent1)];
                const mockRollbackError = new Error('rollback error');
                mockConnection.commit.and.callFake(function(callback) {
                    callback(mockError, null);
                });
                mockConnection.rollback.and.callFake(function(callback) {
                    throw mockRollbackError;
                });

                initMysqlES(() => {
                    mysqlES.addEvents(mockEvents, (error) => {
                        expect(mockConnection.commit).toHaveBeenCalledWith(jasmine.any(Function));
                        expect(mockConnection.rollback).toHaveBeenCalledWith(jasmine.any(Function));
                        expect(error).toEqual(mockError);
                        done();
                    });
                });
            }, 250);
        });

        describe('conn.release', () => {
            beforeEach(() => {
                mockPool.getConnection.and.callFake(function(callback) {
                    callback(null, mockConnection);
                });
            });

            it('should call conn.release and throw no error if all other connection calls are successful', (done) => {
                const mockEvents = [_.cloneDeep(mockEvent1)];
                mockConnection.beginTransaction.and.callFake(function(callback) {
                    callback();
                });
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, 'mockResults', 'mockFields');
                });
                mockConnection.commit.and.callFake(function(callback) {
                    callback(null);
                });
                mockConnection.release.and.callFake(function() {});

                initMysqlES(() => {
                    mysqlES.addEvents(mockEvents, () => {
                        expect(mockConnection.release).toHaveBeenCalled();
                    });
                });
                done();
            }, 250);

            it('should call conn.release and throw the conn.beginTransaction error if conn.beginTransaction returned an error', (done) => {
                const mockEvents = [_.cloneDeep(mockEvent1)];
                mockConnection.beginTransaction.and.callFake(function(callback) {
                    callback(mockError);
                });
                mockConnection.release.and.callFake(function() {});

                initMysqlES(() => {
                    mysqlES.addEvents(mockEvents, (error) => {
                        expect(mockConnection.release).toHaveBeenCalled();
                        expect(error).toEqual(mockError);
                        done();
                    });
                });
            }, 250);

            it('should call conn.release and throw the conn.beginTransaction error if conn.beginTransaction threw an error', (done) => {
                const mockEvents = [_.cloneDeep(mockEvent1)];
                mockConnection.beginTransaction.and.callFake(function(callback) {
                    throw mockError;
                });
                mockConnection.release.and.callFake(function() {});

                initMysqlES(() => {
                    mysqlES.addEvents(mockEvents, (error) => {
                        expect(mockConnection.release).toHaveBeenCalled();
                        expect(error).toEqual(mockError);
                        done();
                    });
                });
            }, 250);

            it('should call conn.release and throw the conn.query error if conn.query returned an error', (done) => {
                const mockEvents = [_.cloneDeep(mockEvent1)];
                mockConnection.beginTransaction.and.callFake(function(callback) {
                    callback();
                });
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(mockError);
                });
                mockConnection.rollback.and.callFake(function(callback) {
                    callback(null);
                });
                mockConnection.release.and.callFake(function() {});

                initMysqlES(() => {
                    mysqlES.addEvents(mockEvents, (error) => {
                        expect(mockConnection.release).toHaveBeenCalled();
                        expect(error).toEqual(mockError);
                        done();
                    });
                });
            }, 250);

            it('should call conn.release and throw the conn.beginTransaction error if conn.beginTransaction threw an error', (done) => {
                const mockEvents = [_.cloneDeep(mockEvent1)];
                mockConnection.beginTransaction.and.callFake(function(callback) {
                    callback();
                });
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    throw mockError;
                });
                mockConnection.rollback.and.callFake(function(callback) {
                    callback(null);
                });
                mockConnection.release.and.callFake(function() {});

                initMysqlES(() => {
                    mysqlES.addEvents(mockEvents, (error) => {
                        expect(mockConnection.release).toHaveBeenCalled();
                        expect(error).toEqual(mockError);
                        done();
                    });
                });
            }, 250);

            it('should call conn.release and throw the conn.commit error if conn.commit returned an error', (done) => {
                const mockEvents = [_.cloneDeep(mockEvent1)];
                mockConnection.beginTransaction.and.callFake(function(callback) {
                    callback();
                });
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, 'mockResults', 'mockFields');
                });
                mockConnection.commit.and.callFake(function(callback) {
                    callback(mockError, null);
                });
                mockConnection.rollback.and.callFake(function(callback) {
                    callback(null);
                });
                mockConnection.release.and.callFake(function() {});

                initMysqlES(() => {
                    mysqlES.addEvents(mockEvents, (error) => {
                        expect(mockConnection.release).toHaveBeenCalled();
                        expect(error).toEqual(mockError);
                        done();
                    });
                });
            }, 250);

            it('should call conn.release and throw the conn.commit error if conn.commit threw an error', (done) => {
                const mockEvents = [_.cloneDeep(mockEvent1)];
                mockConnection.beginTransaction.and.callFake(function(callback) {
                    callback();
                });
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, 'mockResults', 'mockFields');
                });
                mockConnection.commit.and.callFake(function(callback) {
                    callback(mockError, null);
                });
                mockConnection.rollback.and.callFake(function(callback) {
                    callback(null);
                });
                mockConnection.release.and.callFake(function() {});

                initMysqlES(() => {
                    mysqlES.addEvents(mockEvents, (error) => {
                        expect(mockConnection.release).toHaveBeenCalled();
                        expect(error).toEqual(mockError);
                        done();
                    });
                });
            }, 250);

            it('should throw no error if conn.release threw an error', (done) => {
                const mockEvents = [_.cloneDeep(mockEvent1)];
                mockConnection.beginTransaction.and.callFake(function(callback) {
                    callback();
                });
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, 'mockResults', 'mockFields');
                });
                mockConnection.commit.and.callFake(function(callback) {
                    callback(null);
                });
                mockConnection.release.and.callFake(function() {
                    throw mockError;
                });

                initMysqlES(() => {
                    mysqlES.addEvents(mockEvents, () => {
                        expect(mockConnection.release).toHaveBeenCalled();
                        done();
                    });
                });
            }, 250);
        });
    });

    describe('getEvents', () => {
        const mockContext = 'mockContext';
        const mockAggregate = 'mockAggregate';
        const mockAggregateId = 'mockAggregateId';
        const mockRevMin = 6;
        let mockQuery;
        let mysqlES;

        beforeEach((done) => {
            mockQuery = {
                aggregateId: mockAggregateId,
                aggregate: mockAggregate,
                context: mockContext
            };
            mockMysql.createPool.and.returnValue(mockPool);
            done();
        });

        const initMysqlES = (cb) => {
            mysqlES = new MysqlEventStore(mockOptions);
            spyOn(mysqlES, '_initQuery').and.callFake(function(query, cb) { cb() });
            mysqlES.connect(cb);
        };

        describe('pool.getConnection', () => {
            beforeEach(() => {
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, [{ payload: '{}' }], 'mockFields');
                });
                mockConnection.release.and.callFake(function() {});
            });

            it('should call pool.getConnection with a callback', (done) => {
                mockPool.getConnection.and.callFake(function(callback) {
                    callback(null, mockConnection);
                });

                initMysqlES(() => {
                    mysqlES.getEvents(mockQuery, undefined, undefined, () => {
                        expect(mockPool.getConnection).toHaveBeenCalledWith(jasmine.any(Function));
                        done();
                    });
                });
            }, 250);

            it('should throw an error if pool.getConnection returned an error', (done) => {
                mockPool.getConnection.and.callFake(function(callback) {
                    callback(mockError, null);
                });

                initMysqlES(() => {
                    mysqlES.getEvents(mockQuery, undefined, undefined, (error) => {
                        expect(mockPool.getConnection).toHaveBeenCalledWith(jasmine.any(Function));
                        expect(error).toEqual(mockError);
                        done();
                    });
                });
            }, 250);

            it('should throw an error if pool.getConnection threw an error', (done) => {
                mockPool.getConnection.and.callFake(function() {
                    throw mockError;
                });

                initMysqlES(() => {
                    mysqlES.getEvents(mockQuery, undefined, undefined, (error) => {
                        expect(mockPool.getConnection).toHaveBeenCalledWith(jasmine.any(Function));
                        expect(error).toEqual(mockError);
                        done();
                    });
                });
            }, 250);
        });

        describe('queries', () => {
            beforeEach(() => {
                mockPool.getConnection.and.callFake(function(callback) {
                    callback(null, mockConnection);
                });
                mockConnection.release.and.callFake(function() {});
            });

            it('should call a SELECT query on the proper database (with late row lookup), and events table with proper params if limit and skip are undefined', (done) => {
                const expectedQueryPayloads = [mockQuery.aggregate, mockQuery.context, mockQuery.aggregateId, 0, 0];
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, [{ payload: '{}' }], 'mockFields');
                });

                initMysqlES(() => {
                    mysqlES.getEvents(mockQuery, undefined, undefined, () => {
                        expect(mockConnection.query).toHaveBeenCalledWith(
                            `SELECT e.id, event FROM ( SELECT id FROM ${mockOptions.database}.events WHERE aggregate = ? AND context = ? AND aggregate_id = ? ORDER BY commit_stamp ASC, stream_revision ASC, commit_sequence ASC LIMIT ?, ? ) o JOIN ${mockOptions.database}.events e ON o.id = e.id ORDER BY commit_stamp ASC, stream_revision ASC, commit_sequence`,
                            expectedQueryPayloads,
                            jasmine.any(Function)
                        );
                        done();
                    });
                });
            }, 250);

            it('should call a SELECT query on the proper database (with late row lookup), and events table with proper params if limit and skip are null', (done) => {
                const expectedQueryPayloads = [mockQuery.aggregate, mockQuery.context, mockQuery.aggregateId, 0, 0];
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, [{ payload: '{}' }], 'mockFields');
                });

                initMysqlES(() => {
                    mysqlES.getEvents(mockQuery, null, null, () => {
                        expect(mockConnection.query).toHaveBeenCalledWith(
                            `SELECT e.id, event FROM ( SELECT id FROM ${mockOptions.database}.events WHERE aggregate = ? AND context = ? AND aggregate_id = ? ORDER BY commit_stamp ASC, stream_revision ASC, commit_sequence ASC LIMIT ?, ? ) o JOIN ${mockOptions.database}.events e ON o.id = e.id ORDER BY commit_stamp ASC, stream_revision ASC, commit_sequence`,
                            expectedQueryPayloads,
                            jasmine.any(Function)
                        );
                        done();
                    });
                });
            }, 250);

            it('should call a SELECT query on the proper database (with late row lookup), and events table with proper params if limit and skip are NaN', (done) => {
                const expectedQueryPayloads = [mockQuery.aggregate, mockQuery.context, mockQuery.aggregateId, 0, 0];
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, [{ payload: '{}' }], 'mockFields');
                });

                initMysqlES(() => {
                    mysqlES.getEvents(mockQuery, 'skip', 'limit', () => {
                        expect(mockConnection.query).toHaveBeenCalledWith(
                            `SELECT e.id, event FROM ( SELECT id FROM ${mockOptions.database}.events WHERE aggregate = ? AND context = ? AND aggregate_id = ? ORDER BY commit_stamp ASC, stream_revision ASC, commit_sequence ASC LIMIT ?, ? ) o JOIN ${mockOptions.database}.events e ON o.id = e.id ORDER BY commit_stamp ASC, stream_revision ASC, commit_sequence`,
                            expectedQueryPayloads,
                            jasmine.any(Function)
                        );
                        done();
                    });
                });
            }, 250);

            it('should call a SELECT query on the proper database (with late row lookup), and events table with proper params if query is empty', (done) => {
                const expectedQueryPayloads = [0, 0];
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, [{ payload: '{}' }], 'mockFields');
                });

                initMysqlES(() => {
                    mysqlES.getEvents({}, 0, 0, () => {
                        expect(mockConnection.query).toHaveBeenCalledWith(
                            `SELECT e.id, event FROM ( SELECT id FROM ${mockOptions.database}.events ORDER BY commit_stamp ASC, stream_revision ASC, commit_sequence ASC LIMIT ?, ? ) o JOIN ${mockOptions.database}.events e ON o.id = e.id ORDER BY commit_stamp ASC, stream_revision ASC, commit_sequence`,
                            expectedQueryPayloads,
                            jasmine.any(Function)
                        );
                        done();
                    });
                });
            }, 250);

            it('should call a SELECT query on the proper database (with late row lookup), and events table with proper params if query is undefined', (done) => {
                const expectedQueryPayloads = [0, 0];
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, [{ payload: '{}' }], 'mockFields');
                });

                initMysqlES(() => {
                    mysqlES.getEvents(undefined, 0, 0, () => {
                        expect(mockConnection.query).toHaveBeenCalledWith(
                            `SELECT e.id, event FROM ( SELECT id FROM ${mockOptions.database}.events ORDER BY commit_stamp ASC, stream_revision ASC, commit_sequence ASC LIMIT ?, ? ) o JOIN ${mockOptions.database}.events e ON o.id = e.id ORDER BY commit_stamp ASC, stream_revision ASC, commit_sequence`,
                            expectedQueryPayloads,
                            jasmine.any(Function)
                        );
                        done();
                    });
                });
            }, 250);

            it('should call a SELECT query on the proper database (with late row lookup), and events table with proper params if query is null', (done) => {
                const expectedQueryPayloads = [0, 0];
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, [{ payload: '{}' }], 'mockFields');
                });

                initMysqlES(() => {
                    mysqlES.getEvents(null, 0, 0, () => {
                        expect(mockConnection.query).toHaveBeenCalledWith(
                            `SELECT e.id, event FROM ( SELECT id FROM ${mockOptions.database}.events ORDER BY commit_stamp ASC, stream_revision ASC, commit_sequence ASC LIMIT ?, ? ) o JOIN ${mockOptions.database}.events e ON o.id = e.id ORDER BY commit_stamp ASC, stream_revision ASC, commit_sequence`,
                            expectedQueryPayloads,
                            jasmine.any(Function)
                        );
                        done();
                    });
                });
            }, 250);

            it('should call a SELECT query on the proper database (with late row lookup), and events table with proper params if aggregate is missing in query', (done) => {
                const expectedQueryPayloads = [mockQuery.context, mockQuery.aggregateId, 0, 0];
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, [{ payload: '{}' }], 'mockFields');
                });

                initMysqlES(() => {
                    mysqlES.getEvents({ context: mockQuery.context, aggregateId: mockQuery.aggregateId }, 0, 0, () => {
                        expect(mockConnection.query).toHaveBeenCalledWith(
                            `SELECT e.id, event FROM ( SELECT id FROM ${mockOptions.database}.events WHERE context = ? AND aggregate_id = ? ORDER BY commit_stamp ASC, stream_revision ASC, commit_sequence ASC LIMIT ?, ? ) o JOIN ${mockOptions.database}.events e ON o.id = e.id ORDER BY commit_stamp ASC, stream_revision ASC, commit_sequence`,
                            expectedQueryPayloads,
                            jasmine.any(Function)
                        );
                        done();
                    });
                });
            }, 250);

            it('should call a SELECT query on the proper database (with late row lookup), and events table with proper params if aggregate and context are missing in query', (done) => {
                const expectedQueryPayloads = [mockQuery.aggregateId, 0, 0];
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, [{ payload: '{}' }], 'mockFields');
                });

                initMysqlES(() => {
                    mysqlES.getEvents({ aggregateId: mockQuery.aggregateId }, 0, 0, () => {
                        expect(mockConnection.query).toHaveBeenCalledWith(
                            `SELECT e.id, event FROM ( SELECT id FROM ${mockOptions.database}.events WHERE aggregate_id = ? ORDER BY commit_stamp ASC, stream_revision ASC, commit_sequence ASC LIMIT ?, ? ) o JOIN ${mockOptions.database}.events e ON o.id = e.id ORDER BY commit_stamp ASC, stream_revision ASC, commit_sequence`,
                            expectedQueryPayloads,
                            jasmine.any(Function)
                        );
                        done();
                    });
                });
            }, 250);

            it('should call a SELECT query on the proper database (with late row lookup), and events table with proper params if limit and skip are defined ', (done) => {
                const expectedQueryPayloads = [mockQuery.aggregate, mockQuery.context, mockQuery.aggregateId, 10, 100];
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, [{ payload: '{}' }], 'mockFields');
                });

                initMysqlES(() => {
                    mysqlES.getEvents(mockQuery, 10, 100, () => {
                        expect(mockConnection.query).toHaveBeenCalledWith(
                            `SELECT e.id, event FROM ( SELECT id FROM ${mockOptions.database}.events WHERE aggregate = ? AND context = ? AND aggregate_id = ? ORDER BY commit_stamp ASC, stream_revision ASC, commit_sequence ASC LIMIT ?, ? ) o JOIN ${mockOptions.database}.events e ON o.id = e.id ORDER BY commit_stamp ASC, stream_revision ASC, commit_sequence`,
                            expectedQueryPayloads,
                            jasmine.any(Function)
                        );
                        done();
                    });
                });
            }, 250);

            it('should return the proper results if there are events', (done) => {
                const expectedQueryPayloads = [mockQuery.aggregate, mockQuery.context, mockQuery.aggregateId, 10, 100];
                const mockEvent = {
                    id: 'mockEventId',
                    aggregateId: mockAggregateId,
                    aggregate: mockAggregate,
                    context: mockContext,
                    streamRevision: 5,
                    commitId: 'mockEventId',
                    commitStamp: new Date('2020-03-06T00:28:36.728Z'),
                    commitSequence: 0,
                    payload: {
                        name: 'mock_event_added',
                        payload: 'mockPayload',
                        aggregateId: mockAggregateId
                    }
                };
                const mockEvt = _.cloneDeep(mockEvent);
                mockEvt.commitStamp = mockEvt.commitStamp.getTime();
                const mockResults = [{
                    id: mockEvt.id,
                    event: JSON.stringify(mockEvt)
                }];
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, mockResults, 'mockFields');
                });
                initMysqlES(() => {
                    mysqlES.getEvents(mockQuery, 10, 100, (error, results) => {
                        expect(error).toBeFalsy();
                        expect(mockConnection.query).toHaveBeenCalledWith(
                            `SELECT e.id, event FROM ( SELECT id FROM ${mockOptions.database}.events WHERE aggregate = ? AND context = ? AND aggregate_id = ? ORDER BY commit_stamp ASC, stream_revision ASC, commit_sequence ASC LIMIT ?, ? ) o JOIN ${mockOptions.database}.events e ON o.id = e.id ORDER BY commit_stamp ASC, stream_revision ASC, commit_sequence`,
                            expectedQueryPayloads,
                            jasmine.any(Function)
                        );
                        expect(results).toEqual([mockEvent]);
                        done();
                    });
                });
            }, 250);

            it('should return the proper results if there are no events', (done) => {
                const expectedQueryPayloads = [mockQuery.aggregate, mockQuery.context, mockQuery.aggregateId, 10, 100];
                const mockResults = [];
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, mockResults, 'mockFields');
                });
                initMysqlES(() => {
                    mysqlES.getEvents(mockQuery, 10, 100, (error, results) => {
                        expect(error).toBeFalsy();
                        expect(mockConnection.query).toHaveBeenCalledWith(
                            `SELECT e.id, event FROM ( SELECT id FROM ${mockOptions.database}.events WHERE aggregate = ? AND context = ? AND aggregate_id = ? ORDER BY commit_stamp ASC, stream_revision ASC, commit_sequence ASC LIMIT ?, ? ) o JOIN ${mockOptions.database}.events e ON o.id = e.id ORDER BY commit_stamp ASC, stream_revision ASC, commit_sequence`,
                            expectedQueryPayloads,
                            jasmine.any(Function)
                        );
                        expect(results).toEqual([]);
                        done();
                    });
                });
            }, 250);

            it('should convert int to string when an aggregateId of type int is passed to the query', (done) => {
                const mockIntAggregateId = 90210;
                const mockQueryWithIntAggregateId = {
                    aggregateId: mockIntAggregateId,
                    aggregate: mockAggregate,
                    context: mockContext
                };

                const expectedQueryPayloads = [mockQueryWithIntAggregateId.aggregate, mockQueryWithIntAggregateId.context, `${mockQueryWithIntAggregateId.aggregateId}`, 10, 100];
                const mockEvent = {
                    id: 'mockEventId0',
                    aggregateId: mockIntAggregateId,
                    aggregate: mockAggregate,
                    context: mockContext,
                    streamRevision: 5,
                    commitId: 'mockEventId',
                    commitStamp: new Date('2020-03-06T00:28:36.728Z'),
                    commitSequence: 0,
                    payload: {
                        name: 'mock_event_added',
                        payload: 'mockPayload',
                        aggregateId: mockAggregateId
                    }
                };
                const mockEvt = _.cloneDeep(mockEvent);
                mockEvt.commitStamp = mockEvt.commitStamp.getTime();
                const mockResults = [{
                    id: mockEvt.id,
                    event: JSON.stringify(mockEvt)
                }];
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, mockResults, 'mockFields');
                });

                initMysqlES(() => {
                    mysqlES.getEvents(mockQueryWithIntAggregateId, 10, 100, (error, results) => {
                        expect(error).toBeFalsy();
                        expect(mockConnection.query).toHaveBeenCalledWith(
                            `SELECT e.id, event FROM ( SELECT id FROM ${mockOptions.database}.events WHERE aggregate = ? AND context = ? AND aggregate_id = ? ORDER BY commit_stamp ASC, stream_revision ASC, commit_sequence ASC LIMIT ?, ? ) o JOIN ${mockOptions.database}.events e ON o.id = e.id ORDER BY commit_stamp ASC, stream_revision ASC, commit_sequence`,
                            expectedQueryPayloads,
                            jasmine.any(Function)
                        );
                        expect(results).toEqual([mockEvent]);
                        done();
                    });
                });
            }, 250);
        });

        describe('conn.release', () => {
            beforeEach(() => {
                mockPool.getConnection.and.callFake(function(callback) {
                    callback(null, mockConnection);
                });
            });

            it('should call conn.release and throw no error if all other connection calls are successful', (done) => {
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, [{ payload: '{}' }], 'mockFields');
                });
                mockConnection.release.and.callFake(function() {});

                initMysqlES(() => {
                    mysqlES.getEvents(mockQuery, undefined, undefined, () => {
                        expect(mockConnection.release).toHaveBeenCalled();
                        done();
                    });
                });
            }, 250);

            it('should call conn.release and throw the conn.query error if conn.query returned an error', (done) => {
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(mockError);
                });
                mockConnection.release.and.callFake(function() {});

                initMysqlES(() => {
                    mysqlES.getEvents(mockQuery, undefined, undefined, (error) => {
                        expect(mockConnection.release).toHaveBeenCalled();
                        expect(error).toEqual(mockError);
                        done();
                    });
                });
            }, 250);

            it('should call conn.release and throw the conn.query error if conn.query threw an error', (done) => {
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    throw mockError;
                });
                mockConnection.release.and.callFake(function() {});

                initMysqlES(() => {
                    mysqlES.getEvents(mockQuery, undefined, undefined, (error) => {
                        expect(mockConnection.release).toHaveBeenCalled();
                        expect(error).toEqual(mockError);
                        done();
                    });
                });
            }, 250);

            it('should throw no error if conn.release threw an error', (done) => {
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, [{ payload: '{}' }], 'mockFields');
                });
                mockConnection.release.and.callFake(function() {
                    throw mockError;
                });

                initMysqlES(() => {
                    mysqlES.getEvents(mockQuery, undefined, undefined, () => {
                        expect(mockConnection.release).toHaveBeenCalled();
                        done();
                    });
                });
            }, 250);
        });
    });

    describe('getEventsSince', () => {
        const mockContext = 'mockContext';
        const mockAggregate = 'mockAggregate';
        const mockAggregateId = 'mockAggregateId';
        const mockRevMin = 6;
        let mockDate;
        let mysqlES;

        beforeEach((done) => {
            mockDate = new Date('2020-03-06T00:28:36.728Z');
            mockMysql.createPool.and.returnValue(mockPool);
            done();
        });

        const initMysqlES = (cb) => {
            mysqlES = new MysqlEventStore(mockOptions);
            spyOn(mysqlES, '_initQuery').and.callFake(function(query, cb) { cb() });
            mysqlES.connect(cb);
        };

        describe('pool.getConnection', () => {
            beforeEach(() => {
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, [{ payload: '{}' }], 'mockFields');
                });
                mockConnection.release.and.callFake(function() {});
            });

            it('should call pool.getConnection with a callback', (done) => {
                mockPool.getConnection.and.callFake(function(callback) {
                    callback(null, mockConnection);
                });

                initMysqlES(() => {
                    mysqlES.getEventsSince(mockDate, undefined, undefined, () => {
                        expect(mockPool.getConnection).toHaveBeenCalledWith(jasmine.any(Function));
                        done();
                    });
                });
            }, 250);

            it('should throw an error if pool.getConnection returned an error', (done) => {
                mockPool.getConnection.and.callFake(function(callback) {
                    callback(mockError, null);
                });

                initMysqlES(() => {
                    mysqlES.getEventsSince(mockDate, undefined, undefined, (error) => {
                        expect(mockPool.getConnection).toHaveBeenCalledWith(jasmine.any(Function));
                        expect(error).toEqual(mockError);
                        done();
                    });
                });
            }, 250);

            it('should throw an error if pool.getConnection threw an error', (done) => {
                mockPool.getConnection.and.callFake(function() {
                    throw mockError;
                });

                initMysqlES(() => {
                    mysqlES.getEventsSince(mockDate, undefined, undefined, (error) => {
                        expect(mockPool.getConnection).toHaveBeenCalledWith(jasmine.any(Function));
                        expect(error).toEqual(mockError);
                        done();
                    });
                });
            }, 250);
        });

        describe('queries', () => {
            beforeEach(() => {
                mockPool.getConnection.and.callFake(function(callback) {
                    callback(null, mockConnection);
                });
                mockConnection.release.and.callFake(function() {});
            });

            it('should call a SELECT query on the proper database, and events table with proper params if limit and skip are undefined', (done) => {
                const expectedQueryPayloads = [mockDate, 0, 0];
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, [{ payload: '{}' }], 'mockFields');
                });

                initMysqlES(() => {
                    mysqlES.getEventsSince(mockDate, undefined, undefined, () => {
                        expect(mockConnection.query).toHaveBeenCalledWith(
                            `SELECT id, event FROM ${mockOptions.database}.events WHERE commit_stamp >= ? ORDER BY commit_stamp ASC, stream_revision ASC, commit_sequence ASC LIMIT ? OFFSET ?`,
                            expectedQueryPayloads,
                            jasmine.any(Function)
                        );
                        done();
                    });
                });
            }, 250);

            it('should call a SELECT query on the proper database, and events table with proper params if limit and skip are null', (done) => {
                const expectedQueryPayloads = [mockDate, 0, 0];
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, [{ payload: '{}' }], 'mockFields');
                });

                initMysqlES(() => {
                    mysqlES.getEventsSince(mockDate, undefined, undefined, () => {
                        expect(mockConnection.query).toHaveBeenCalledWith(
                            `SELECT id, event FROM ${mockOptions.database}.events WHERE commit_stamp >= ? ORDER BY commit_stamp ASC, stream_revision ASC, commit_sequence ASC LIMIT ? OFFSET ?`,
                            expectedQueryPayloads,
                            jasmine.any(Function)
                        );
                        done();
                    });
                });
            }, 250);

            it('should call a SELECT query on the proper database, and events table with proper params if limit and skip are NaN', (done) => {
                const expectedQueryPayloads = [mockDate, 0, 0];
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, [{ payload: '{}' }], 'mockFields');
                });

                initMysqlES(() => {
                    mysqlES.getEventsSince(mockDate, 'skip', 'limit', () => {
                        expect(mockConnection.query).toHaveBeenCalledWith(
                            `SELECT id, event FROM ${mockOptions.database}.events WHERE commit_stamp >= ? ORDER BY commit_stamp ASC, stream_revision ASC, commit_sequence ASC LIMIT ? OFFSET ?`,
                            expectedQueryPayloads,
                            jasmine.any(Function)
                        );
                        done();
                    });
                });
            }, 250);

            it('should call a SELECT query on the proper database, and events table with proper params if date is undefined', (done) => {
                const expectedQueryPayloads = [0, 0, 0];
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, [{ payload: '{}' }], 'mockFields');
                });

                initMysqlES(() => {
                    mysqlES.getEventsSince(undefined, 'skip', 'limit', () => {
                        expect(mockConnection.query).toHaveBeenCalledWith(
                            `SELECT id, event FROM ${mockOptions.database}.events WHERE commit_stamp >= ? ORDER BY commit_stamp ASC, stream_revision ASC, commit_sequence ASC LIMIT ? OFFSET ?`,
                            expectedQueryPayloads,
                            jasmine.any(Function)
                        );
                        done();
                    });
                });
            }, 250);

            it('should call a SELECT query on the proper database, and events table with proper params if date is null', (done) => {
                const expectedQueryPayloads = [0, 0, 0];
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, [{ payload: '{}' }], 'mockFields');
                });

                initMysqlES(() => {
                    mysqlES.getEventsSince(null, 'skip', 'limit', () => {
                        expect(mockConnection.query).toHaveBeenCalledWith(
                            `SELECT id, event FROM ${mockOptions.database}.events WHERE commit_stamp >= ? ORDER BY commit_stamp ASC, stream_revision ASC, commit_sequence ASC LIMIT ? OFFSET ?`,
                            expectedQueryPayloads,
                            jasmine.any(Function)
                        );
                        done();
                    });
                });
            }, 250);

            it('should return the proper results if there are events', (done) => {
                const expectedQueryPayloads = [mockDate, 100, 10];
                const mockEvent = {
                    id: 'mockEventId',
                    aggregateId: mockAggregateId,
                    aggregate: mockAggregate,
                    context: mockContext,
                    streamRevision: 5,
                    commitId: 'mockEventId',
                    commitStamp: new Date('2020-03-06T00:28:36.728Z'),
                    commitSequence: 0,
                    payload: {
                        name: 'mock_event_added',
                        payload: 'mockPayload',
                        aggregateId: mockAggregateId
                    }
                };
                const mockEvt = _.cloneDeep(mockEvent);
                mockEvt.commitStamp = mockEvt.commitStamp.getTime();
                const mockResults = [{
                    id: mockEvt.id,
                    event: JSON.stringify(mockEvt)
                }];
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, mockResults, 'mockFields');
                });
                initMysqlES(() => {
                    mysqlES.getEventsSince(mockDate, 10, 100, (error, results) => {
                        expect(error).toBeFalsy();
                        expect(mockConnection.query).toHaveBeenCalledWith(
                            `SELECT id, event FROM ${mockOptions.database}.events WHERE commit_stamp >= ? ORDER BY commit_stamp ASC, stream_revision ASC, commit_sequence ASC LIMIT ? OFFSET ?`,
                            expectedQueryPayloads,
                            jasmine.any(Function)
                        );
                        expect(results).toEqual([mockEvent]);
                        done();
                    });
                });
            }, 250);

            it('should return the proper results if there are no events', (done) => {
                const expectedQueryPayloads = [mockDate, 100, 10];
                const mockResults = [];
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, mockResults, 'mockFields');
                });
                initMysqlES(() => {
                    mysqlES.getEventsSince(mockDate, 10, 100, (error, results) => {
                        expect(error).toBeFalsy();
                        expect(mockConnection.query).toHaveBeenCalledWith(
                            `SELECT id, event FROM ${mockOptions.database}.events WHERE commit_stamp >= ? ORDER BY commit_stamp ASC, stream_revision ASC, commit_sequence ASC LIMIT ? OFFSET ?`,
                            expectedQueryPayloads,
                            jasmine.any(Function)
                        );
                        expect(results).toEqual([]);
                        done();
                    });
                });
            }, 250);
        });

        describe('conn.release', () => {
            beforeEach(() => {
                mockPool.getConnection.and.callFake(function(callback) {
                    callback(null, mockConnection);
                });
            });

            it('should call conn.release and throw no error if all other connection calls are successful', (done) => {
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, [{ payload: '{}' }], 'mockFields');
                });
                mockConnection.release.and.callFake(function() {});

                initMysqlES(() => {
                    mysqlES.getEventsSince(mockDate, undefined, undefined, () => {
                        expect(mockConnection.release).toHaveBeenCalled();
                        done();
                    });
                });
            }, 250);

            it('should call conn.release and throw the conn.query error if conn.query returned an error', (done) => {
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(mockError);
                });
                mockConnection.release.and.callFake(function() {});

                initMysqlES(() => {
                    mysqlES.getEventsSince(mockDate, undefined, undefined, (error) => {
                        expect(mockConnection.release).toHaveBeenCalled();
                        expect(error).toEqual(mockError);
                        done();
                    });
                });
            }, 250);

            it('should call conn.release and throw the conn.query error if conn.query threw an error', (done) => {
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    throw mockError;
                });
                mockConnection.release.and.callFake(function() {});

                initMysqlES(() => {
                    mysqlES.getEventsSince(mockDate, undefined, undefined, (error) => {
                        expect(mockConnection.release).toHaveBeenCalled();
                        expect(error).toEqual(mockError);
                        done();
                    });
                });
            }, 250);

            it('should throw no error if conn.release threw an error', (done) => {
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, [{ payload: '{}' }], 'mockFields');
                });
                mockConnection.release.and.callFake(function() {
                    throw mockError;
                });

                initMysqlES(() => {
                    mysqlES.getEventsSince(mockDate, undefined, undefined, () => {
                        expect(mockConnection.release).toHaveBeenCalled();
                        done();
                    });
                });
            }, 250);
        });

    });

    describe('getEventsByRevision', () => {
        const mockContext = 'mockContext';
        const mockAggregate = 'mockAggregate';
        const mockAggregateId = 'mockAggregateId';
        const mockRevMin = 6;
        let mockQuery;
        let mysqlES;

        beforeEach((done) => {
            mockQuery = {
                aggregateId: mockAggregateId,
                aggregate: mockAggregate,
                context: mockContext
            };
            mockMysql.createPool.and.returnValue(mockPool);
            done();
        });

        const initMysqlES = (cb) => {
            mysqlES = new MysqlEventStore(mockOptions);
            spyOn(mysqlES, '_initQuery').and.callFake(function(query, cb) { cb() });
            mysqlES.connect(cb);
        };

        describe('pool.getConnection', () => {
            beforeEach(() => {
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, [{ payload: '{}' }], 'mockFields');
                });
                mockConnection.release.and.callFake(function() {});
            });

            it('should call pool.getConnection with a callback', (done) => {
                mockPool.getConnection.and.callFake(function(callback) {
                    callback(null, mockConnection);
                });

                initMysqlES(() => {
                    mysqlES.getEventsByRevision(mockQuery, mockRevMin, -1, () => {
                        expect(mockPool.getConnection).toHaveBeenCalledWith(jasmine.any(Function));
                        done();
                    });
                });
            }, 250);

            it('should throw an error if pool.getConnection returned an error', (done) => {
                mockPool.getConnection.and.callFake(function(callback) {
                    callback(mockError, null);
                });

                initMysqlES(() => {
                    mysqlES.getEventsByRevision(mockQuery, mockRevMin, -1, (error) => {
                        expect(mockPool.getConnection).toHaveBeenCalledWith(jasmine.any(Function));
                        expect(error).toEqual(mockError);
                        done();
                    });
                });
            }, 250);

            it('should throw an error if pool.getConnection threw an error', (done) => {
                mockPool.getConnection.and.callFake(function() {
                    throw mockError;
                });

                initMysqlES(() => {
                    mysqlES.getEventsByRevision(mockQuery, mockRevMin, -1, (error) => {
                        expect(mockPool.getConnection).toHaveBeenCalledWith(jasmine.any(Function));
                        expect(error).toEqual(mockError);
                        done();
                    });
                });
            }, 250);
        });

        describe('queries', () => {
            beforeEach(() => {
                mockPool.getConnection.and.callFake(function(callback) {
                    callback(null, mockConnection);
                });
                mockConnection.release.and.callFake(function() {});
            });

            it('should call a SELECT query on the proper database, and events table with proper params', (done) => {
                const expectedQueryPayloads = [mockQuery.aggregate, mockQuery.context, mockQuery.aggregateId, mockRevMin];
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, [{ payload: '{}' }], 'mockFields');
                });

                initMysqlES(() => {
                    mysqlES.getEventsByRevision(mockQuery, mockRevMin, -1, () => {
                        expect(mockConnection.query).toHaveBeenCalledWith(
                            `SELECT id, event FROM ${mockOptions.database}.events WHERE aggregate = ? AND context = ? AND aggregate_id = ? AND stream_revision >= ? ORDER BY commit_stamp ASC, stream_revision ASC, commit_sequence ASC`,
                            expectedQueryPayloads,
                            jasmine.any(Function)
                        );
                        done();
                    });
                });
            }, 250);

            it('should call a SELECT query on the proper database, and events table with proper params if revMin is undefined', (done) => {
                const expectedQueryPayloads = [mockQuery.aggregate, mockQuery.context, mockQuery.aggregateId];
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, [{ payload: '{}' }], 'mockFields');
                });

                initMysqlES(() => {
                    mysqlES.getEventsByRevision(mockQuery, undefined, -1, () => {
                        expect(mockConnection.query).toHaveBeenCalledWith(
                            `SELECT id, event FROM ${mockOptions.database}.events WHERE aggregate = ? AND context = ? AND aggregate_id = ? ORDER BY commit_stamp ASC, stream_revision ASC, commit_sequence ASC`,
                            expectedQueryPayloads,
                            jasmine.any(Function)
                        );
                        done();
                    });
                });
            }, 250);

            it('should call a SELECT query on the proper database, and events table with proper params if revMin is null', (done) => {
                const expectedQueryPayloads = [mockQuery.aggregate, mockQuery.context, mockQuery.aggregateId];
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, [{ payload: '{}' }], 'mockFields');
                });

                initMysqlES(() => {
                    mysqlES.getEventsByRevision(mockQuery, null, -1, () => {
                        expect(mockConnection.query).toHaveBeenCalledWith(
                            `SELECT id, event FROM ${mockOptions.database}.events WHERE aggregate = ? AND context = ? AND aggregate_id = ? ORDER BY commit_stamp ASC, stream_revision ASC, commit_sequence ASC`,
                            expectedQueryPayloads,
                            jasmine.any(Function)
                        );
                        done();
                    });
                });
                
                
            }, 250);

            it('should call a SELECT query on the proper database, and events table with proper params if revMin is not a number', (done) => {
                const expectedQueryPayloads = [mockQuery.aggregate, mockQuery.context, mockQuery.aggregateId];
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, [{ payload: '{}' }], 'mockFields');
                });

                initMysqlES(() => {
                    mysqlES.getEventsByRevision(mockQuery, 'not a number', -1, () => {
                        expect(mockConnection.query).toHaveBeenCalledWith(
                            `SELECT id, event FROM ${mockOptions.database}.events WHERE aggregate = ? AND context = ? AND aggregate_id = ? ORDER BY commit_stamp ASC, stream_revision ASC, commit_sequence ASC`,
                            expectedQueryPayloads,
                            jasmine.any(Function)
                        );
                        done();
                    });
                });
            }, 250);

            it('should call a SELECT query on the proper database, and events table with proper params if query is empty', (done) => {
                const expectedQueryPayloads = [];
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, [{ payload: '{}' }], 'mockFields');
                });

                initMysqlES(() => {
                    mysqlES.getEventsByRevision({}, 0, -1, () => {
                        expect(mockConnection.query).toHaveBeenCalledWith(
                            `SELECT id, event FROM ${mockOptions.database}.events ORDER BY commit_stamp ASC, stream_revision ASC, commit_sequence ASC`,
                            expectedQueryPayloads,
                            jasmine.any(Function)
                        );
                        done();
                    });
                });
            }, 250);

            it('should call a SELECT query on the proper database, and events table with proper params if query is undefined', (done) => {
                const expectedQueryPayloads = [];
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, [{ payload: '{}' }], 'mockFields');
                });

                initMysqlES(() => {
                    mysqlES.getEventsByRevision(undefined, 0, -1, () => {
                        expect(mockConnection.query).toHaveBeenCalledWith(
                            `SELECT id, event FROM ${mockOptions.database}.events ORDER BY commit_stamp ASC, stream_revision ASC, commit_sequence ASC`,
                            expectedQueryPayloads,
                            jasmine.any(Function)
                        );
                        done();
                    });
                });
            }, 250);

            it('should call a SELECT query on the proper database, and events table with proper params if query is null', (done) => {
                const expectedQueryPayloads = [];
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, [{ payload: '{}' }], 'mockFields');
                });

                initMysqlES(() => {
                    mysqlES.getEventsByRevision(null, 0, -1, () => {
                        expect(mockConnection.query).toHaveBeenCalledWith(
                            `SELECT id, event FROM ${mockOptions.database}.events ORDER BY commit_stamp ASC, stream_revision ASC, commit_sequence ASC`,
                            expectedQueryPayloads,
                            jasmine.any(Function)
                        );
                        done();
                    });
                });
            }, 250);

            it('should call a SELECT query on the proper database, and events table with proper params if aggregate is missing in query', (done) => {
                const expectedQueryPayloads = [mockQuery.context, mockQuery.aggregateId];
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, [{ payload: '{}' }], 'mockFields');
                });

                initMysqlES(() => {
                    mysqlES.getEventsByRevision({ context: mockQuery.context, aggregateId: mockQuery.aggregateId }, 0, -1, () => {
                        expect(mockConnection.query).toHaveBeenCalledWith(
                            `SELECT id, event FROM ${mockOptions.database}.events WHERE context = ? AND aggregate_id = ? ORDER BY commit_stamp ASC, stream_revision ASC, commit_sequence ASC`,
                            expectedQueryPayloads,
                            jasmine.any(Function)
                        );
                        done();
                    });
                });
            }, 250);

            it('should call a SELECT query on the proper database, and events table with proper params if aggregate and context are missing in query', (done) => {
                const expectedQueryPayloads = [mockQuery.aggregateId];
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, [{ payload: '{}' }], 'mockFields');
                });

                initMysqlES(() => {
                    mysqlES.getEventsByRevision({ aggregateId: mockQuery.aggregateId }, 0, -1, () => {
                        expect(mockConnection.query).toHaveBeenCalledWith(
                            `SELECT id, event FROM ${mockOptions.database}.events WHERE aggregate_id = ? ORDER BY commit_stamp ASC, stream_revision ASC, commit_sequence ASC`,
                            expectedQueryPayloads,
                            jasmine.any(Function)
                        );
                        done();
                    });
                });
            }, 250);

            it('should call a SELECT query on the proper database, and events table with proper params if query is null but revMin is defined', (done) => {
                const expectedQueryPayloads = [10];
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, [{ payload: '{}' }], 'mockFields');
                });

                initMysqlES(() => {
                    mysqlES.getEventsByRevision(null, 10, -1, () => {
                        expect(mockConnection.query).toHaveBeenCalledWith(
                            `SELECT id, event FROM ${mockOptions.database}.events WHERE stream_revision >= ? ORDER BY commit_stamp ASC, stream_revision ASC, commit_sequence ASC`,
                            expectedQueryPayloads,
                            jasmine.any(Function)
                        );
                        done();
                    });
                });
            }, 250);

            it('should call a SELECT query on the proper database, and events table with proper params if revMax is defined', (done) => {
                const mockRevMax = 100;
                const expectedQueryPayloads = [mockQuery.aggregate, mockQuery.context, mockQuery.aggregateId, mockRevMin, mockRevMax];
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, [{ payload: '{}' }], 'mockFields');
                });

                initMysqlES(() => {
                    mysqlES.getEventsByRevision(mockQuery, mockRevMin, mockRevMax, () => {
                        expect(mockConnection.query).toHaveBeenCalledWith(
                            `SELECT id, event FROM ${mockOptions.database}.events WHERE aggregate = ? AND context = ? AND aggregate_id = ? AND stream_revision >= ? AND stream_revision < ? ORDER BY commit_stamp ASC, stream_revision ASC, commit_sequence ASC`,
                            expectedQueryPayloads,
                            jasmine.any(Function)
                        );
                        done();
                    });
                });
            }, 250);

            it('should call a SELECT query on the proper database, and events table with proper params if only revMax is defined', (done) => {
                const mockRevMax = 100;
                const expectedQueryPayloads = [mockRevMax];
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, [{ payload: '{}' }], 'mockFields');
                });

                initMysqlES(() => {
                    mysqlES.getEventsByRevision({}, 0, mockRevMax, () => {
                        expect(mockConnection.query).toHaveBeenCalledWith(
                            `SELECT id, event FROM ${mockOptions.database}.events WHERE stream_revision < ? ORDER BY commit_stamp ASC, stream_revision ASC, commit_sequence ASC`,
                            expectedQueryPayloads,
                            jasmine.any(Function)
                        );
                        done();
                    });
                });
            }, 250);

            it('should return the proper results if there are events', (done) => {
                const expectedQueryPayloads = [mockQuery.aggregate, mockQuery.context, mockQuery.aggregateId, mockRevMin];
                const mockEvent = {
                    id: 'mockEventId',
                    aggregateId: mockAggregateId,
                    aggregate: mockAggregate,
                    context: mockContext,
                    streamRevision: 5,
                    commitId: 'mockEventId',
                    commitStamp: new Date('2020-03-06T00:28:36.728Z'),
                    commitSequence: 0,
                    payload: {
                        name: 'mock_event_added',
                        payload: 'mockPayload',
                        aggregateId: mockAggregateId
                    }
                };
                const mockEvt = _.cloneDeep(mockEvent);
                mockEvt.commitStamp = mockEvt.commitStamp.getTime();
                const mockResults = [{
                    id: mockEvt.id,
                    event: JSON.stringify(mockEvt)
                }];
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, mockResults, 'mockFields');
                });
                initMysqlES(() => {
                    mysqlES.getEventsByRevision(mockQuery, mockRevMin, -1, (error, results) => {
                        expect(error).toBeFalsy();
                        expect(mockConnection.query).toHaveBeenCalledWith(
                            `SELECT id, event FROM ${mockOptions.database}.events WHERE aggregate = ? AND context = ? AND aggregate_id = ? AND stream_revision >= ? ORDER BY commit_stamp ASC, stream_revision ASC, commit_sequence ASC`,
                            expectedQueryPayloads,
                            jasmine.any(Function)
                        );
                        expect(results).toEqual([mockEvent]);
                        done();
                    });
                });
            }, 250);

            it('should return the proper results if there are no events', (done) => {
                const expectedQueryPayloads = [mockQuery.aggregate, mockQuery.context, mockQuery.aggregateId, mockRevMin];
                const mockResults = [];
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, mockResults, 'mockFields');
                });

                initMysqlES(() => {
                    mysqlES.getEventsByRevision(mockQuery, mockRevMin, -1, (error, results) => {
                        expect(error).toBeFalsy();
                        expect(mockConnection.query).toHaveBeenCalledWith(
                            `SELECT id, event FROM ${mockOptions.database}.events WHERE aggregate = ? AND context = ? AND aggregate_id = ? AND stream_revision >= ? ORDER BY commit_stamp ASC, stream_revision ASC, commit_sequence ASC`,
                            expectedQueryPayloads,
                            jasmine.any(Function)
                        );
                        expect(results).toEqual([]);
                        done();
                    });
                });
            }, 250);

            it('should convert int to string when an aggregateId of type int is passed to the query', (done) => {
                const mockIntAggregateId = 90210;
                const mockQueryWithIntAggregateId = {
                    aggregateId: mockIntAggregateId,
                    aggregate: mockAggregate,
                    context: mockContext
                };

                const expectedQueryPayloads = [mockQueryWithIntAggregateId.aggregate, mockQueryWithIntAggregateId.context, `${mockQueryWithIntAggregateId.aggregateId}`, mockRevMin];
                const mockEvent = {
                    id: 'mockEventId0',
                    aggregateId: mockIntAggregateId,
                    aggregate: mockAggregate,
                    context: mockContext,
                    streamRevision: 5,
                    commitId: 'mockEventId',
                    commitStamp: new Date('2020-03-06T00:28:36.728Z'),
                    commitSequence: 0,
                    payload: {
                        name: 'mock_event_added',
                        payload: 'mockPayload',
                        aggregateId: mockAggregateId
                    }
                };
                const mockEvt = _.cloneDeep(mockEvent);
                mockEvt.commitStamp = mockEvt.commitStamp.getTime();
                const mockResults = [{
                    id: mockEvt.id,
                    event: JSON.stringify(mockEvt)
                }];
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, mockResults, 'mockFields');
                });

                initMysqlES(() => {
                    mysqlES.getEventsByRevision(mockQueryWithIntAggregateId, mockRevMin, -1, (error, results) => {
                        expect(error).toBeFalsy();
                        expect(mockConnection.query).toHaveBeenCalledWith(
                            `SELECT id, event FROM ${mockOptions.database}.events WHERE aggregate = ? AND context = ? AND aggregate_id = ? AND stream_revision >= ? ORDER BY commit_stamp ASC, stream_revision ASC, commit_sequence ASC`,
                            expectedQueryPayloads,
                            jasmine.any(Function)
                        );
                        expect(results).toEqual([mockEvent]);
                        done();
                    });
                });
            }, 250);
        });

        describe('conn.release', () => {
            beforeEach(() => {
                mockPool.getConnection.and.callFake(function(callback) {
                    callback(null, mockConnection);
                });
            });

            it('should call conn.release and throw no error if all other connection calls are successful', (done) => {
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, [{ payload: '{}' }], 'mockFields');
                });
                mockConnection.release.and.callFake(function() {});

                initMysqlES(() => {
                    mysqlES.getEventsByRevision(mockQuery, mockRevMin, -1, () => {
                        expect(mockConnection.release).toHaveBeenCalled();
                        done();
                    });
                });
            }, 250);

            it('should call conn.release and throw the conn.query error if conn.query returned an error', (done) => {
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(mockError);
                });
                mockConnection.release.and.callFake(function() {});

                initMysqlES(() => {
                    mysqlES.getEventsByRevision(mockQuery, mockRevMin, -1, (error) => {
                        expect(mockConnection.release).toHaveBeenCalled();
                        expect(error).toEqual(mockError);
                        done();
                    });
                });
            }, 250);

            it('should call conn.release and throw the conn.query error if conn.query threw an error', (done) => {
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    throw mockError;
                });
                mockConnection.release.and.callFake(function() {});

                initMysqlES(() => {
                    mysqlES.getEventsByRevision(mockQuery, mockRevMin, -1, (error) => {
                        expect(mockConnection.release).toHaveBeenCalled();
                        expect(error).toEqual(mockError);
                        done();
                    });
                });
            }, 250);

            it('should throw no error if conn.release threw an error', (done) => {
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, [{ payload: '{}' }], 'mockFields');
                });
                mockConnection.release.and.callFake(function() {
                    throw mockError;
                });

                initMysqlES(() => {
                    mysqlES.getEventsByRevision(mockQuery, mockRevMin, -1, () => {
                        expect(mockConnection.release).toHaveBeenCalled();
                        done();
                    });
                });
            }, 250);
        });

    });

    describe('getUndispatchedEvents', () => {
        const mockContext = 'mockContext';
        const mockAggregate = 'mockAggregate';
        const mockAggregateId = 'mockAggregateId';
        let mockQuery;
        let mysqlES;

        beforeEach((done) => {
            mockQuery = {
                aggregate: mockAggregate,
                context: mockContext,
                aggregateId: mockAggregateId
            };
            mockMysql.createPool.and.returnValue(mockPool);
            done();
        });

        const initMysqlES = (cb) => {
            mysqlES = new MysqlEventStore(mockOptions);
            spyOn(mysqlES, '_initQuery').and.callFake(function(query, cb) { cb() });
            mysqlES.connect(cb);
        };

        describe('pool.getConnection', () => {
            beforeEach(() => {
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, [{ payload: '{}' }], 'mockFields');
                });
                mockConnection.release.and.callFake(function() {});
            });

            it('should call pool.getConnection with a callback', (done) => {
                mockPool.getConnection.and.callFake(function(callback) {
                    callback(null, mockConnection);
                });

                initMysqlES(() => {
                    mysqlES.getUndispatchedEvents(mockQuery, () => {
                        expect(mockPool.getConnection).toHaveBeenCalledWith(jasmine.any(Function));
                        done();
                    });
                });
                
            }, 250);

            it('should throw an error if pool.getConnection returned an error', (done) => {
                mockPool.getConnection.and.callFake(function(callback) {
                    callback(mockError, null);
                });

                initMysqlES(() => {
                    mysqlES.getUndispatchedEvents(mockQuery, (error) => {
                        expect(mockPool.getConnection).toHaveBeenCalledWith(jasmine.any(Function));
                        expect(error).toEqual(mockError);
                        done();
                    });
                });
            }, 250);

            it('should throw an error if pool.getConnection threw an error', (done) => {
                mockPool.getConnection.and.callFake(function() {
                    throw mockError;
                });

                initMysqlES(() => {
                    mysqlES.getUndispatchedEvents(mockQuery, (error) => {
                        expect(mockPool.getConnection).toHaveBeenCalledWith(jasmine.any(Function));
                        expect(error).toEqual(mockError);
                        done();
                    });
                });
            }, 250);
        });

        describe('queries', () => {
            beforeEach(() => {
                mockPool.getConnection.and.callFake(function(callback) {
                    callback(null, mockConnection);
                });
                mockConnection.release.and.callFake(function() {});
            });

            it('should call a SELECT query on the proper database, and undispatched_events table with proper params if query is empty', (done) => {
                const expectedQueryPayloads = [];
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, [{ payload: '{}' }], 'mockFields');
                });

                initMysqlES(() => {
                    mysqlES.getUndispatchedEvents({}, () => {
                        expect(mockConnection.query).toHaveBeenCalledWith(
                            `SELECT id, event FROM ${mockOptions.database}.undispatched_events ORDER BY commit_stamp ASC, stream_revision ASC, commit_sequence ASC`,
                            expectedQueryPayloads,
                            jasmine.any(Function)
                        );
                        done();
                    });
                });
            }, 250);

            it('should call a SELECT query on the proper database, and undispatched_events table with proper params if query is undefined', (done) => {
                const expectedQueryPayloads = [];
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, [{ payload: '{}' }], 'mockFields');
                });

                initMysqlES(() => {
                    mysqlES.getUndispatchedEvents(undefined, () => {
                        expect(mockConnection.query).toHaveBeenCalledWith(
                            `SELECT id, event FROM ${mockOptions.database}.undispatched_events ORDER BY commit_stamp ASC, stream_revision ASC, commit_sequence ASC`,
                            expectedQueryPayloads,
                            jasmine.any(Function)
                        );
                        done();
                    });
                });
            }, 250);

            it('should call a SELECT query on the proper database, and undispatched_events table with proper params if query is null', (done) => {
                const expectedQueryPayloads = [];
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, [{ payload: '{}' }], 'mockFields');
                });

                initMysqlES(() => {
                    mysqlES.getUndispatchedEvents(null, () => {
                        expect(mockConnection.query).toHaveBeenCalledWith(
                            `SELECT id, event FROM ${mockOptions.database}.undispatched_events ORDER BY commit_stamp ASC, stream_revision ASC, commit_sequence ASC`,
                            expectedQueryPayloads,
                            jasmine.any(Function)
                        );
                        done();
                    });
                });
            }, 250);
         

            it('should call a SELECT query on the proper database, and undispatched_events table with proper params if aggregate is missing in query', (done) => {
                const expectedQueryPayloads = [mockQuery.context, mockQuery.aggregateId];
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, [{ payload: '{}' }], 'mockFields');
                });

                initMysqlES(() => {
                    mysqlES.getUndispatchedEvents({ context: mockQuery.context, aggregateId: mockQuery.aggregateId }, () => {
                        expect(mockConnection.query).toHaveBeenCalledWith(
                            `SELECT id, event FROM ${mockOptions.database}.undispatched_events WHERE context = ? AND aggregate_id = ? ORDER BY commit_stamp ASC, stream_revision ASC, commit_sequence ASC`,
                            expectedQueryPayloads,
                            jasmine.any(Function)
                        );
                        done();
                    });
                });
            }, 250);

            it('should call a SELECT query on the proper database, and undispatched_events table with proper params if aggregate and context are missing in query', (done) => {
                const expectedQueryPayloads = [mockQuery.aggregateId];
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, [{ payload: '{}' }], 'mockFields');
                });

                initMysqlES(() => {
                    mysqlES.getUndispatchedEvents({ aggregateId: mockQuery.aggregateId }, () => {
                        expect(mockConnection.query).toHaveBeenCalledWith(
                            `SELECT id, event FROM ${mockOptions.database}.undispatched_events WHERE aggregate_id = ? ORDER BY commit_stamp ASC, stream_revision ASC, commit_sequence ASC`,
                            expectedQueryPayloads,
                            jasmine.any(Function)
                        );
                        done();
                    });
                });
            }, 250);

            it('should call a SELECT query on the proper database, and undispatched_events table with proper params', (done) => {
                mockQuery = {
                    aggregateId: 'mockAggregateId',
                    aggregate: 'mockAggregate',
                    context: 'mockContext'
                };

                const expectedQueryPayloads = [mockQuery.aggregate, mockQuery.context, mockQuery.aggregateId];
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, [{ payload: '{}' }], 'mockFields');
                });

                initMysqlES(() => {
                    mysqlES.getUndispatchedEvents(mockQuery, () => {
                        expect(mockConnection.query).toHaveBeenCalledWith(
                            `SELECT id, event FROM ${mockOptions.database}.undispatched_events WHERE aggregate = ? AND context = ? AND aggregate_id = ? ORDER BY commit_stamp ASC, stream_revision ASC, commit_sequence ASC`,
                            expectedQueryPayloads,
                            jasmine.any(Function)
                        );
                        done();
                    });
                   
                });
            }, 250);

            it('should return the properly mapped results', (done) => {
                const expectedQueryPayloads = [mockQuery.aggregate, mockQuery.context, mockQuery.aggregateId];
                const mockUndispatchedEvent = {
                    id: `mockEventId0`,
                    aggregateId: mockAggregateId,
                    aggregate: mockAggregate,
                    context: mockContext,
                    streamRevision: 5,
                    commitId: 'mockEventId',
                    commitStamp: new Date('2020-03-06T00:28:36.728Z'),
                    commitSequence: 0,
                    payload: {
                        name: 'mock_event_added',
                        payload: 'mockPayload',
                        aggregateId: mockAggregateId
                    }
                };
                const mockResults = [{
                    id: mockUndispatchedEvent.eventId,
                    event: JSON.stringify(mockUndispatchedEvent)
                }];

                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, mockResults, 'mockFields');
                });

                initMysqlES(() => {
                    mysqlES.getUndispatchedEvents(mockQuery, (err, results) => {
                        expect(mockConnection.query).toHaveBeenCalledWith(
                            `SELECT id, event FROM ${mockOptions.database}.undispatched_events WHERE aggregate = ? AND context = ? AND aggregate_id = ? ORDER BY commit_stamp ASC, stream_revision ASC, commit_sequence ASC`,
                            expectedQueryPayloads,
                            jasmine.any(Function)
                        );
                        expect(results).toEqual([mockUndispatchedEvent]);
                        done();
                    });
                });
            }, 250);
        });

        describe('conn.release', () => {
            beforeEach(() => {
                mockPool.getConnection.and.callFake(function(callback) {
                    callback(null, mockConnection);
                });
            });

            it('should call conn.release and throw no error if all other connection calls are successful', (done) => {
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, [{ payload: '{}' }], 'mockFields');
                });
                mockConnection.release.and.callFake(function() {});

                initMysqlES(() => {
                    mysqlES.getUndispatchedEvents(mockQuery, () => {
                        expect(mockConnection.release).toHaveBeenCalled();
                        done();
                    });
                });
            }, 250);

            it('should call conn.release and throw the conn.query error if conn.query returned an error', (done) => {
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(mockError);
                });
                mockConnection.release.and.callFake(function() {});

                initMysqlES(() => {
                    mysqlES.getUndispatchedEvents(mockQuery, (error) => {
                        expect(mockConnection.release).toHaveBeenCalled();
                        expect(error).toEqual(mockError);
                        done();
                    });
                });
            }, 250);

            it('should call conn.release and throw the conn.query error if conn.query threw an error', (done) => {
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    throw mockError;
                });
                mockConnection.release.and.callFake(function() {});

                initMysqlES(() => {
                    mysqlES.getUndispatchedEvents(mockQuery, (error) => {
                        expect(mockConnection.release).toHaveBeenCalled();
                        expect(error).toEqual(mockError);
                        done();
                    });
                });
            }, 250);

            it('should throw no error if conn.release threw an error', (done) => {
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, [{ payload: '{}' }], 'mockFields');
                });
                mockConnection.release.and.callFake(function() {
                    throw mockError;
                });

                initMysqlES(() => {
                    mysqlES.getUndispatchedEvents(mockQuery, () => {
                        expect(mockConnection.release).toHaveBeenCalled();
                        done();
                    });
                });
            }, 250);
        });
    });

    describe('getLastEvent', () => {
        const mockContext = 'mockContext';
        const mockAggregate = 'mockAggregate';
        const mockAggregateId = 'mockAggregateId';
        let mockQuery;
        let mysqlES;

        beforeEach((done) => {
            mockQuery = {
                aggregateId: mockAggregateId,
                aggregate: mockAggregate,
                context: mockContext
            };
            mockMysql.createPool.and.returnValue(mockPool);
            done();
        });

        const initMysqlES = (cb) => {
            mysqlES = new MysqlEventStore(mockOptions);
            spyOn(mysqlES, '_initQuery').and.callFake(function(query, cb) { cb() });
            mysqlES.connect(cb);
        };

        describe('pool.getConnection', () => {
            beforeEach(() => {
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, [{ payload: '{}' }], 'mockFields');
                });
                mockConnection.release.and.callFake(function() {});
            });

            it('should call pool.getConnection with a callback', (done) => {
                mockPool.getConnection.and.callFake(function(callback) {
                    callback(null, mockConnection);
                });

                initMysqlES(() => {
                    mysqlES.getLastEvent(mockQuery, () => {
                        expect(mockPool.getConnection).toHaveBeenCalledWith(jasmine.any(Function));
                        done();
                    });
                });
            }, 250);

            it('should throw an error if pool.getConnection returned an error', (done) => {
                mockPool.getConnection.and.callFake(function(callback) {
                    callback(mockError, null);
                });

                initMysqlES(() => {
                    mysqlES.getLastEvent(mockQuery, (error) => {
                        expect(mockPool.getConnection).toHaveBeenCalledWith(jasmine.any(Function));
                        expect(error).toEqual(mockError);
                        done();
                    });
                });
            }, 250);

            it('should throw an error if pool.getConnection threw an error', (done) => {
                mockPool.getConnection.and.callFake(function() {
                    throw mockError;
                });

                initMysqlES(() => {
                    mysqlES.getLastEvent(mockQuery, (error) => {
                        expect(mockPool.getConnection).toHaveBeenCalledWith(jasmine.any(Function));
                        expect(error).toEqual(mockError);
                        done();
                    });
                });
            }, 250);
        });

        describe('queries', () => {
            beforeEach(() => {
                mockPool.getConnection.and.callFake(function(callback) {
                    callback(null, mockConnection);
                });
                mockConnection.release.and.callFake(function() {});
            });

            it('should call a SELECT query on the proper database, and events table with proper params', (done) => {
                const expectedQueryPayloads = [mockQuery.aggregate, mockQuery.context, mockQuery.aggregateId];
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, [{ payload: '{}' }], 'mockFields');
                });

                initMysqlES(() => {
                    mysqlES.getLastEvent(mockQuery, () => {
                        expect(mockConnection.query).toHaveBeenCalledWith(
                            `SELECT id, event FROM ${mockOptions.database}.events WHERE aggregate = ? AND context = ? AND aggregate_id = ? ORDER BY commit_stamp DESC, stream_revision DESC, commit_sequence DESC LIMIT 1`,
                            expectedQueryPayloads,
                            jasmine.any(Function)
                        );
                        done();
                    });
                });
            }, 250);

            it('should call a SELECT query on the proper database, and events table with proper params if query is empty', (done) => {
                const expectedQueryPayloads = [];
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, [{ payload: '{}' }], 'mockFields');
                });

                initMysqlES(() => {
                    mysqlES.getLastEvent({}, () => {
                        expect(mockConnection.query).toHaveBeenCalledWith(
                            `SELECT id, event FROM ${mockOptions.database}.events ORDER BY commit_stamp DESC, stream_revision DESC, commit_sequence DESC LIMIT 1`,
                            expectedQueryPayloads,
                            jasmine.any(Function)
                        );
                        done();
                    });
                });
            }, 250);

            it('should call a SELECT query on the proper database, and events table with proper params if query is undefined', (done) => {
                const expectedQueryPayloads = [];
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, [{ payload: '{}' }], 'mockFields');
                });

                initMysqlES(() => {
                    mysqlES.getLastEvent(undefined, () => {
                        expect(mockConnection.query).toHaveBeenCalledWith(
                            `SELECT id, event FROM ${mockOptions.database}.events ORDER BY commit_stamp DESC, stream_revision DESC, commit_sequence DESC LIMIT 1`,
                            expectedQueryPayloads,
                            jasmine.any(Function)
                        );
                        done();
                    });
                });
            }, 250);

            it('should call a SELECT query on the proper database, and events table with proper params if query is null', (done) => {
                const expectedQueryPayloads = [];
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, [{ payload: '{}' }], 'mockFields');
                });

                initMysqlES(() => {
                    mysqlES.getLastEvent(null, () => {
                        expect(mockConnection.query).toHaveBeenCalledWith(
                            `SELECT id, event FROM ${mockOptions.database}.events ORDER BY commit_stamp DESC, stream_revision DESC, commit_sequence DESC LIMIT 1`,
                            expectedQueryPayloads,
                            jasmine.any(Function)
                        );
                        done();
                    });
                });
            }, 250);
            
            it('should call a SELECT query on the proper database, and events table with proper params if aggregate is not defined', (done) => {
                const expectedQueryPayloads = [mockQuery.context, mockQuery.aggregateId];
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, [{ payload: '{}' }], 'mockFields');
                });

                initMysqlES(() => {
                    mysqlES.getLastEvent({ context: mockQuery.context, aggregateId: mockQuery.aggregateId }, () => {
                        expect(mockConnection.query).toHaveBeenCalledWith(
                            `SELECT id, event FROM ${mockOptions.database}.events WHERE context = ? AND aggregate_id = ? ORDER BY commit_stamp DESC, stream_revision DESC, commit_sequence DESC LIMIT 1`,
                            expectedQueryPayloads,
                            jasmine.any(Function)
                        );
                        done();
                    });
                });
            }, 250);

            it('should call a SELECT query on the proper database, and events table with proper params if aggregate and context are not defined', (done) => {
                const expectedQueryPayloads = [mockQuery.aggregateId];
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, [{ payload: '{}' }], 'mockFields');
                });

                initMysqlES(() => {
                    mysqlES.getLastEvent({ aggregateId: mockQuery.aggregateId }, () => {
                        expect(mockConnection.query).toHaveBeenCalledWith(
                            `SELECT id, event FROM ${mockOptions.database}.events WHERE aggregate_id = ? ORDER BY commit_stamp DESC, stream_revision DESC, commit_sequence DESC LIMIT 1`,
                            expectedQueryPayloads,
                            jasmine.any(Function)
                        );
                        done();
                    });
                });
            }, 250);

            it('should convert an int aggregateId to string', (done) => {
                const mockIntAggregateId = 90210;
                const mockQueryWithIntAggregateId = {
                    aggregateId: mockIntAggregateId,
                    aggregate: mockAggregate,
                    context: mockContext
                };
                const expectedQueryPayloads = [mockQueryWithIntAggregateId.aggregate, mockQueryWithIntAggregateId.context, `${mockQueryWithIntAggregateId.aggregateId}`];
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, [{ payload: '{}' }], 'mockFields');
                });

                initMysqlES(() => {
                    mysqlES.getLastEvent(mockQueryWithIntAggregateId, () => {
                        expect(mockConnection.query).toHaveBeenCalledWith(
                            `SELECT id, event FROM ${mockOptions.database}.events WHERE aggregate = ? AND context = ? AND aggregate_id = ? ORDER BY commit_stamp DESC, stream_revision DESC, commit_sequence DESC LIMIT 1`,
                            expectedQueryPayloads,
                            jasmine.any(Function)
                        );
                        done();
                    });
                });
            }, 250);

            it('should return the proper last event if there is a last event', (done) => {
                const expectedQueryPayloads = [mockQuery.aggregate, mockQuery.context, mockQuery.aggregateId];
                const mockLastEvent = {
                    id: 'mockLastEventId0',
                    aggregateId: mockAggregateId,
                    aggregate: mockAggregate,
                    context: mockContext,
                    streamRevision: 3,
                    commitId: 'mockLastEventId',
                    commitStamp: new Date('2020-04-06T00:28:36.728Z'),
                    commitSequence: 0,
                    payload: {
                        name: 'mock_event_added',
                        payload: 'mockPayload',
                        aggregateId: mockAggregateId
                    }
                };
                const mockEvt = _.cloneDeep(mockLastEvent);
                mockEvt.commitStamp = mockEvt.commitStamp.getTime();
                const mockResults = [{
                    id: mockEvt.id,
                    event: JSON.stringify(mockEvt)
                }];
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, mockResults, 'mockFields');
                });

                initMysqlES(() => {
                    mysqlES.getLastEvent(mockQuery, (error, results) => {
                        expect(error).toBeFalsy();
                        expect(mockConnection.query).toHaveBeenCalledWith(
                            `SELECT id, event FROM ${mockOptions.database}.events WHERE aggregate = ? AND context = ? AND aggregate_id = ? ORDER BY commit_stamp DESC, stream_revision DESC, commit_sequence DESC LIMIT 1`,
                            expectedQueryPayloads,
                            jasmine.any(Function)
                        );
                        expect(results).toEqual(mockLastEvent);
                        done();
                    });
                });
            }, 250);

            it('should return null if there is no last event', (done) => {
                const expectedQueryPayloads = [mockQuery.aggregate, mockQuery.context, mockQuery.aggregateId];
                const mockResults = [];
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, mockResults, 'mockFields');
                });

                initMysqlES(() => {
                    mysqlES.getLastEvent(mockQuery, (error, results) => {
                        expect(error).toBeFalsy();
                        expect(mockConnection.query).toHaveBeenCalledWith(
                            `SELECT id, event FROM ${mockOptions.database}.events WHERE aggregate = ? AND context = ? AND aggregate_id = ? ORDER BY commit_stamp DESC, stream_revision DESC, commit_sequence DESC LIMIT 1`,
                            expectedQueryPayloads,
                            jasmine.any(Function)
                        );
                        expect(results).toEqual(null);
                        done();
                    });
                });
            }, 250);
        });

        describe('conn.release', () => {
            beforeEach(() => {
                mockPool.getConnection.and.callFake(function(callback) {
                    callback(null, mockConnection);
                });
            });

            it('should call conn.release and throw no error if all other connection calls are successful', (done) => {
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, [{ payload: '{}' }], 'mockFields');
                });
                mockConnection.release.and.callFake(function() {});

                initMysqlES(() => {
                    mysqlES.getLastEvent(mockQuery, () => {
                        expect(mockConnection.release).toHaveBeenCalled();
                        done();
                    });
                });
            }, 250);

            it('should call conn.release and throw the conn.query error if conn.query returned an error', (done) => {
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(mockError);
                });
                mockConnection.release.and.callFake(function() {});

                initMysqlES(() => {
                    mysqlES.getLastEvent(mockQuery, (error) => {
                        expect(mockConnection.release).toHaveBeenCalled();
                        expect(error).toEqual(mockError);
                        done();
                    });
                });
            }, 250);

            it('should call conn.release and throw the conn.query error if conn.query threw an error', (done) => {
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    throw mockError;
                });
                mockConnection.release.and.callFake(function() {});

                initMysqlES(() => {
                    mysqlES.getLastEvent(mockQuery, (error) => {
                        expect(mockConnection.release).toHaveBeenCalled();
                        expect(error).toEqual(mockError);
                        done();
                    });
                });
            }, 250);

            it('should throw no error if conn.release threw an error', (done) => {
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, [{ payload: '{}' }], 'mockFields');
                });
                mockConnection.release.and.callFake(function() {
                    throw mockError;
                });

                initMysqlES(() => {
                    mysqlES.getLastEvent(mockQuery, () => {
                        expect(mockConnection.release).toHaveBeenCalled();
                        done();
                    });
                });
            }, 250);
        });
    });

    describe('setEventToDispatched', () => {
        const mockContext = 'mockContext';
        const mockAggregate = 'mockAggregate';
        const mockAggregateId = 'mockAggregateId';
        let mockEvent;
        let mysqlES;

        beforeEach((done) => {
            mockEvent = {
                id: `mockEventId0`,
                aggregateId: mockAggregateId,
                aggregate: mockAggregate,
                context: mockContext,
                streamRevision: 5,
                commitId: 'mockEventId',
                commitStamp: new Date('2020-03-06T00:28:36.728Z'),
                commitSequence: 0,
                payload: {
                    name: 'mock_event_added',
                    payload: 'mockPayload',
                    aggregateId: mockAggregateId
                }
            };
            mockMysql.createPool.and.returnValue(mockPool);
            done();
        });

        const initMysqlES = (cb) => {
            mysqlES = new MysqlEventStore(mockOptions);
            spyOn(mysqlES, '_initQuery').and.callFake(function(query, cb) { cb() });
            mysqlES.connect(cb);
        };

        describe('pool.getConnection', () => {
            beforeEach(() => {
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, [{ payload: '{}' }], 'mockFields');
                });
                mockConnection.release.and.callFake(function() {});
            });

            it('should call pool.getConnection with a callback', (done) => {
                mockPool.getConnection.and.callFake(function(callback) {
                    callback(null, mockConnection);
                });

                initMysqlES(() => {
                    mysqlES.setEventToDispatched(mockEvent, () => {
                        expect(mockPool.getConnection).toHaveBeenCalledWith(jasmine.any(Function));
                        done();
                    });
                });
            }, 250);

            it('should throw an error if pool.getConnection returned an error', (done) => {
                mockPool.getConnection.and.callFake(function(callback) {
                    callback(mockError, null);
                });

                initMysqlES(() => {
                    mysqlES.setEventToDispatched(mockEvent, (error) => {
                        expect(mockPool.getConnection).toHaveBeenCalledWith(jasmine.any(Function));
                        expect(error).toEqual(mockError);
                        done();
                    });
                });
            }, 250);

            it('should throw an error if pool.getConnection threw an error', (done) => {
                mockPool.getConnection.and.callFake(function() {
                    throw mockError;
                });

                initMysqlES(() => {
                    mysqlES.setEventToDispatched(mockEvent, (error) => {
                            expect(mockPool.getConnection).toHaveBeenCalledWith(jasmine.any(Function));
                        expect(error).toEqual(mockError);
                        done();
                    });
                });
            }, 250);
        });

        describe('queries', () => {
            beforeEach(() => {
                mockPool.getConnection.and.callFake(function(callback) {
                    callback(null, mockConnection);
                });
                mockConnection.release.and.callFake(function() {});
            });

            it('should call a DELETE query on the undispatched_events table with proper params', (done) => {
                const expectedQueryPayloads = [mockEvent.id];
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, 'mockResults', 'mockFields');
                });

                initMysqlES(() => {
                    mysqlES.setEventToDispatched(mockEvent.id, () => {
                        expect(mockConnection.query).toHaveBeenCalledWith(
                            `DELETE FROM ${mockOptions.database}.undispatched_events WHERE id = ?`,
                            expectedQueryPayloads,
                            jasmine.any(Function)
                        );
                        done();
                    });
                });
                
            }, 250);

            it('should convert event.aggregateId from int to string', (done) => {
                const mockAggregateIntId = 90210;
                const mockEventIntId = {
                    id: `mockEventId0`,
                    aggregateId: mockAggregateIntId,
                    aggregate: mockAggregate,
                    context: mockContext,
                    streamRevision: 5,
                    commitId: 'mockEventId',
                    commitStamp: new Date('2020-03-06T00:28:36.728Z'),
                    commitSequence: 0,
                    payload: {
                        name: 'mock_event_added',
                        payload: 'mockPayload',
                        aggregateId: mockAggregateIntId
                    }
                };

                const expectedQueryPayloads = [mockEventIntId.id];
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, 'mockResults', 'mockFields');
                });

                initMysqlES(() => {
                    mysqlES.setEventToDispatched(mockEventIntId.id, () => {
                        expect(mockConnection.query).toHaveBeenCalledWith(
                            `DELETE FROM ${mockOptions.database}.undispatched_events WHERE id = ?`,
                            expectedQueryPayloads,
                            jasmine.any(Function)
                        );
                    });
                });
                done();
            }, 250);
        });

        describe('conn.release', () => {
            beforeEach(() => {
                mockPool.getConnection.and.callFake(function(callback) {
                    callback(null, mockConnection);
                });
            });

            it('should call conn.release and throw no error if all other connection calls are successful', (done) => {
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, 'mockResults', 'mockFields');
                });
                mockConnection.release.and.callFake(function() {});

                initMysqlES(() => {
                    mysqlES.setEventToDispatched(mockEvent.id, () => {
                        expect(mockConnection.release).toHaveBeenCalled();
                        done();
                    });
                });
                
            }, 250);

            it('should call conn.release and throw the conn.query error if conn.query returned an error', (done) => {
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(mockError);
                });
                mockConnection.release.and.callFake(function() {});

                initMysqlES(() => {
                    mysqlES.setEventToDispatched(mockEvent.id, (error) => {
                        expect(mockConnection.release).toHaveBeenCalled();
                        expect(error).toEqual(mockError);
                        done();
                    });
                });
            }, 250);

            it('should call conn.release and throw the conn.query error if conn.query threw an error', (done) => {
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    throw mockError;
                });
                mockConnection.release.and.callFake(function() {});

                initMysqlES(() => {
                    mysqlES.setEventToDispatched(mockEvent.id, (error) => {
                        expect(mockConnection.release).toHaveBeenCalled();
                        expect(error).toEqual(mockError);
                        done();
                    });
                });
            }, 250);

            it('should throw no error if conn.release threw an error', (done) => {
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, 'mockResults', 'mockFields');
                });
                mockConnection.release.and.callFake(function() {
                    throw mockError;
                });

                initMysqlES(() => {
                    mysqlES.setEventToDispatched(mockEvent, () => {
                        expect(mockConnection.release).toHaveBeenCalled();
                        done();
                    });
                });
            }, 250);
        });
    });

    describe('addSnapshot', () => {
        const mockContext = 'mockContext';
        const mockAggregate = 'mockAggregate';
        const mockAggregateId = 'mockAggregateId';
        let mockSnapshot;
        let mysqlES;

        beforeEach((done) => {
            mockSnapshot = {
                id: 'mockSnapshotId',
                aggregateId: mockAggregateId,
                aggregate: mockAggregate,
                context: mockContext,
                revision: 3,
                commitStamp: '2020-04-06T00:28:36.728Z',
                data: {
                    state: 'created',
                    aggregateId: mockAggregateId
                }
            };
            mockMysql.createPool.and.returnValue(mockPool);
            done();
        });

        const initMysqlES = (cb) => {
            mysqlES = new MysqlEventStore(mockOptions);
            spyOn(mysqlES, '_initQuery').and.callFake(function(query, cb) { cb() });
            mysqlES.connect(cb);
        };

        describe('pool.getConnection', () => {
            beforeEach(() => {
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, [{ payload: '{}' }], 'mockFields');
                });
                mockConnection.release.and.callFake(function() {});
            });

            it('should call pool.getConnection with a callback', (done) => {
                mockPool.getConnection.and.callFake(function(callback) {
                    callback(null, mockConnection);
                });

                initMysqlES(() => {
                    mysqlES.addSnapshot(mockSnapshot, () => {
                        expect(mockPool.getConnection).toHaveBeenCalledWith(jasmine.any(Function));
                        done();
                    });
                });
            }, 250);

            it('should throw an error if pool.getConnection returned an error', (done) => {
                mockPool.getConnection.and.callFake(function(callback) {
                    callback(mockError, null);
                });

                initMysqlES(() => {
                    mysqlES.addSnapshot(mockSnapshot, (error) => {
                        expect(mockPool.getConnection).toHaveBeenCalledWith(jasmine.any(Function));
                        expect(error).toEqual(mockError);
                        done();
                    });
                });
            }, 250);

            it('should throw an error if pool.getConnection threw an error', (done) => {
                mockPool.getConnection.and.callFake(function() {
                    throw mockError;
                });

                initMysqlES(() => {
                    mysqlES.addSnapshot(mockSnapshot, (error) => {
                        expect(mockPool.getConnection).toHaveBeenCalledWith(jasmine.any(Function));
                        expect(error).toEqual(mockError);
                        done();
                    });
                });
            }, 250);
        });

        describe('queries', () => {
            beforeEach(() => {
                mockPool.getConnection.and.callFake(function(callback) {
                    callback(null, mockConnection);
                });
                mockConnection.release.and.callFake(function() {});
            });

            it('should call an INSERT query on the proper database, and snapshots table with proper params', (done) => {
                const mocksnap = _.cloneDeep(mockSnapshot);
                mocksnap.commitStamp = new Date(mockSnapshot.commitStamp).getTime();
                const expectedQueryPayloads = [{
                    id: mockSnapshot.id,
                    snapshot: JSON.stringify(mocksnap)
                }];
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, 'mockResults', 'mockFields');
                });

                initMysqlES(() => {
                    mysqlES.addSnapshot(mockSnapshot, () => {
                        expect(mockConnection.query).toHaveBeenCalledWith(
                            `INSERT INTO ${mockOptions.database}.snapshots SET ?`,
                            expectedQueryPayloads,
                            jasmine.any(Function)
                        );
                        done();
                    });
                });
            }, 250);
        });

        describe('conn.release', () => {
            beforeEach(() => {
                mockPool.getConnection.and.callFake(function(callback) {
                    callback(null, mockConnection);
                });
            });

            it('should call conn.release and throw no error if all other connection calls are successful', (done) => {
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, 'mockResults', 'mockFields');
                });
                mockConnection.release.and.callFake(function() {});

                initMysqlES(() => {
                    mysqlES.addSnapshot(mockSnapshot, () => {
                        expect(mockConnection.release).toHaveBeenCalled();
                        done();
                    });
                });
            }, 250);

            it('should call conn.release and throw the conn.query error if conn.query returned an error', (done) => {
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(mockError);
                });
                mockConnection.release.and.callFake(function() {});

                initMysqlES(() => {
                    mysqlES.addSnapshot(mockSnapshot, (error) => {
                        expect(mockConnection.release).toHaveBeenCalled();
                        expect(error).toEqual(mockError);
                        done();
                    });
                });
            }, 250);

            it('should call conn.release and throw the conn.query error if conn.query threw an error', (done) => {
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    throw mockError;
                });
                mockConnection.release.and.callFake(function() {});

                initMysqlES(() => {
                    mysqlES.addSnapshot(mockSnapshot, (error) => {
                        expect(mockConnection.release).toHaveBeenCalled();
                        expect(error).toEqual(mockError);
                        done();
                    });
                });
            }, 250);

            it('should throw no error if conn.release threw an error', (done) => {
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, 'mockResults', 'mockFields');
                });
                mockConnection.release.and.callFake(function() {
                    throw mockError;
                });

                initMysqlES(() => {
                    mysqlES.addSnapshot(mockSnapshot, () => {
                        expect(mockConnection.release).toHaveBeenCalled();
                        done();
                    });
                });
            }, 250);
        });
    });

    describe('getSnapshot', () => {
        const mockContext = 'mockContext';
        const mockAggregate = 'mockAggregate';
        const mockAggregateId = 'mockAggregateId';
        let mockQuery;
        let mysqlES;

        beforeEach((done) => {
            mockQuery = {
                aggregateId: mockAggregateId,
                aggregate: mockAggregate,
                context: mockContext
            };
            mockMysql.createPool.and.returnValue(mockPool);
            done();
        });

        const initMysqlES = (cb) => {
            mysqlES = new MysqlEventStore(mockOptions);
            spyOn(mysqlES, '_initQuery').and.callFake(function(query, cb) { cb() });
            mysqlES.connect(cb);
        };

        describe('pool.getConnection', () => {
            beforeEach(() => {
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, [{ data: '{}' }], 'mockFields');
                });
                mockConnection.release.and.callFake(function() {});
            });

            it('should call pool.getConnection with a callback', (done) => {
                mockPool.getConnection.and.callFake(function(callback) {
                    callback(null, mockConnection);
                });

                initMysqlES(() => {
                    mysqlES.getSnapshot(mockQuery, 0, () => {
                        expect(mockPool.getConnection).toHaveBeenCalledWith(jasmine.any(Function));
                        done();
                    });
                });
            }, 250);

            it('should throw an error if pool.getConnection returned an error', (done) => {
                mockPool.getConnection.and.callFake(function(callback) {
                    callback(mockError, null);
                });

                initMysqlES(() => {
                    mysqlES.getSnapshot(mockQuery, 0, (error) => {
                        expect(mockPool.getConnection).toHaveBeenCalledWith(jasmine.any(Function));
                        expect(error).toEqual(mockError);
                        done();
                    });
                });
            }, 250);

            it('should throw an error if pool.getConnection threw an error', (done) => {
                mockPool.getConnection.and.callFake(function() {
                    throw mockError;
                });

                initMysqlES(() => {
                    mysqlES.getSnapshot(mockQuery, 0, (error) => {
                        expect(mockPool.getConnection).toHaveBeenCalledWith(jasmine.any(Function));
                        expect(error).toEqual(mockError);
                        done();
                    });
                });
            }, 250);
        });

        describe('queries', () => {
            beforeEach(() => {
                mockPool.getConnection.and.callFake(function(callback) {
                    callback(null, mockConnection);
                });
                mockConnection.release.and.callFake(function() {});
            });

            it('should call a SELECT query on the proper database, and snapshots table with proper params', (done) => {
                const expectedQueryPayloads = [mockQuery.aggregateId, mockQuery.aggregate, mockQuery.context];
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, [{ data: '{}' }], 'mockFields');
                });

                initMysqlES(() => {
                    mysqlES.getSnapshot(mockQuery, 0, () => {
                        expect(mockConnection.query).toHaveBeenCalledWith(
                            `SELECT id, snapshot FROM ${mockOptions.database}.snapshots WHERE aggregate_id = ? AND aggregate = ? AND context = ? ORDER BY commit_stamp DESC LIMIT 1`,
                            expectedQueryPayloads,
                            jasmine.any(Function)
                        );
                    });
                });
                done();
            }, 250);

            it('should call a SELECT query on the proper database, and snapshots table with proper params if context and aggregate are undefined', (done) => {
                delete mockQuery.aggregate;
                delete mockQuery.context;
                const expectedQueryPayloads = [mockQuery.aggregateId, null, null];
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, [{ data: '{}' }], 'mockFields');
                });

                initMysqlES(() => {
                    mysqlES.getSnapshot(mockQuery, 0, () => {
                        expect(mockConnection.query).toHaveBeenCalledWith(
                            `SELECT id, snapshot FROM ${mockOptions.database}.snapshots WHERE aggregate_id = ? AND aggregate = ? AND context = ? ORDER BY commit_stamp DESC LIMIT 1`,
                            expectedQueryPayloads,
                            jasmine.any(Function)
                        );
                    });
                });
                done();
            }, 250);

            it('should convert int aggregateId to string', (done) => {
                const mockAggregateIdInt = 90210;
                const mockQueryIntId = {
                    aggregateId: mockAggregateIdInt,
                    aggregate: mockAggregate,
                    context: mockContext
                };
                const expectedQueryPayloads = [`${mockQueryIntId.aggregateId}`, mockQueryIntId.aggregate, mockQueryIntId.context];
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, [{ data: '{}' }], 'mockFields');
                });

                initMysqlES(() => {
                    mysqlES.getSnapshot(mockQueryIntId, 0, () => {
                        expect(mockConnection.query).toHaveBeenCalledWith(
                            `SELECT id, snapshot FROM ${mockOptions.database}.snapshots WHERE aggregate_id = ? AND aggregate = ? AND context = ? ORDER BY commit_stamp DESC LIMIT 1`,
                            expectedQueryPayloads,
                            jasmine.any(Function)
                        );
                        done();
                    });
                });
            }, 250);

            it('should return the proper snapshot if there is a snapshot', (done) => {
                const expectedQueryPayloads = [mockQuery.aggregateId, mockQuery.aggregate, mockQuery.context];
                const mockSnapshot = {
                    id: 'mockSnapshotId',
                    aggregateId: mockAggregateId,
                    aggregate: mockAggregate,
                    context: mockContext,
                    revision: 3,
                    commitStamp: new Date('2020-04-06T00:28:36.728Z'),
                    data: {
                        state: 'created',
                        aggregateId: mockAggregateId
                    }
                };
                const mocksnap = _.cloneDeep(mockSnapshot);
                mocksnap.commitStamp = mocksnap.commitStamp.getTime();
                const mockResults = [{
                    id: mockSnapshot.id,
                    snapshot: JSON.stringify(mocksnap)
                }];
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, mockResults, 'mockFields');
                });

                initMysqlES(() => {
                    mysqlES.getSnapshot(mockQuery, 0, (error, results) => {
                        expect(error).toBeFalsy();
                        expect(mockConnection.query).toHaveBeenCalledWith(
                            `SELECT id, snapshot FROM ${mockOptions.database}.snapshots WHERE aggregate_id = ? AND aggregate = ? AND context = ? ORDER BY commit_stamp DESC LIMIT 1`,
                            expectedQueryPayloads,
                            jasmine.any(Function)
                        );
                        expect(results).toEqual(mockSnapshot);
                        done();
                    });
                });
            }, 250);

            it('should return null if there are no snapshots', (done) => {
                const expectedQueryPayloads = [mockQuery.aggregateId, mockQuery.aggregate, mockQuery.context];
                const mockResults = [];
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, mockResults, 'mockFields');
                });

                initMysqlES(() => {
                    mysqlES.getSnapshot(mockQuery, 0, (error, results) => {
                        expect(error).toBeFalsy();
                        expect(mockConnection.query).toHaveBeenCalledWith(
                            `SELECT id, snapshot FROM ${mockOptions.database}.snapshots WHERE aggregate_id = ? AND aggregate = ? AND context = ? ORDER BY commit_stamp DESC LIMIT 1`,
                            expectedQueryPayloads,
                            jasmine.any(Function)
                        );
                        expect(results).toEqual(null);
                        done();
                    });
                });
                
            }, 250);
        });

        describe('conn.release', () => {
            beforeEach(() => {
                mockPool.getConnection.and.callFake(function(callback) {
                    callback(null, mockConnection);
                });
            });

            it('should call conn.release and throw no error if all other connection calls are successful', (done) => {
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, [{ data: '{}' }], 'mockFields');
                });
                mockConnection.release.and.callFake(function() {});

                initMysqlES(() => {
                    mysqlES.getSnapshot(mockQuery, 0, () => {
                        expect(mockConnection.release).toHaveBeenCalled();
                        done();
                    });
                });
            }, 250);

            it('should call conn.release and throw the conn.query error if conn.query returned an error', (done) => {
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(mockError);
                });
                mockConnection.release.and.callFake(function() {});

                initMysqlES(() => {
                    mysqlES.getSnapshot(mockQuery, 0, (error) => {
                        expect(mockConnection.release).toHaveBeenCalled();
                        expect(error).toEqual(mockError);
                        done();
                    });
                });
            }, 250);

            it('should call conn.release and throw the conn.query error if conn.query threw an error', (done) => {
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    throw mockError;
                });
                mockConnection.release.and.callFake(function() {});

                initMysqlES(() => {
                    mysqlES.getSnapshot(mockQuery, 0, (error) => {
                        expect(mockConnection.release).toHaveBeenCalled();
                        expect(error).toEqual(mockError);
                        done();
                    });
                });
            }, 250);

            it('should throw no error if conn.release threw an error', (done) => {
                mockConnection.query.and.callFake(function(query, payload, callback) {
                    callback(null, [{ data: '{}' }], 'mockFields');
                });
                mockConnection.release.and.callFake(function() {
                    throw mockError;
                });

                initMysqlES(() => {
                    mysqlES.getSnapshot(mockQuery, 0, () => {
                        expect(mockConnection.release).toHaveBeenCalled();
                        done();
                    });
                });
            }, 250);
        });
    });
});