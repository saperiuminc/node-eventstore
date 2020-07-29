
const mockery = require('mockery');
mockery.enable();
const _ = require('lodash');

const EventstorePlaybackList = require('../lib/eventstore-projections/eventstore-playback-list');

describe('eventstore-playback-list tests', () => {
    let eventstorePlaybackList;
    let options;
    let mysql;
    let mysqlConnection;
    beforeEach(() => {

        mysql = jasmine.createSpyObj('mysql', ['createConnection']);
        mysqlConnection = jasmine.createSpyObj('mysqlConnection', ['query']);
        mysql.createConnection.and.returnValue(mysqlConnection);
        mysqlConnection.query.and.callFake(function(queryText, queryParams, cb) {
            cb();
        });
        mockery.registerMock('mysql', mysql);

        options = {
            host: 'host',
            port: 10,
            user: 'user',
            password: 'password',
            database: 'database'
        };

        eventstorePlaybackList = new EventstorePlaybackList(options);
    });

    describe('init', () => {
        describe('validating the options', () => {
            it('should validate for required parameter host', async (done) => {
                try {
                    options.host = null;
                    eventstorePlaybackList = new EventstorePlaybackList(options);
                    await eventstorePlaybackList.init();
                } catch (error) {
                    expect(error.message).toEqual('host is required to be passed as part of the options');
                    done();
                }
            });

            it('should validate for required parameter port', async (done) => {
                try {
                    options.port = null;
                    eventstorePlaybackList = new EventstorePlaybackList(options);
                    await eventstorePlaybackList.init();
                } catch (error) {
                    expect(error.message).toEqual('port is required to be passed as part of the options');
                    done();
                }
            });

            it('should validate for required parameter user', async (done) => {
                try {
                    options.user = null;
                    eventstorePlaybackList = new EventstorePlaybackList(options);
                    await eventstorePlaybackList.init();
                } catch (error) {
                    expect(error.message).toEqual('user is required to be passed as part of the options');
                    done();
                }
            });

            it('should validate for required parameter password', async (done) => {
                try {
                    options.password = null;
                    eventstorePlaybackList = new EventstorePlaybackList(options);
                    await eventstorePlaybackList.init();
                } catch (error) {
                    expect(error.message).toEqual('password is required to be passed as part of the options');
                    done();
                }
            });

            it('should validate for required parameter database', async (done) => {
                try {
                    options.database = null;
                    eventstorePlaybackList = new EventstorePlaybackList(options);
                    await eventstorePlaybackList.init();
                } catch (error) {
                    expect(error.message).toEqual('database is required to be passed as part of the options');
                    done();
                }
            });
        })

        describe('connecting to the database', () => {
            it('should pass the correct parameters to mysql', async (done) => {
                await eventstorePlaybackList.init();
                expect(mysql.createConnection).toHaveBeenCalledWith({
                    host: options.host,
                    port: options.port,
                    user: options.user,
                    password: options.password,
                    database: options.database
                });
                done();
            })

            it('should handle and just rethrow error if an error in creating a connection in mysql', async (done) => {
                const expectedError = new Error('error in creating a connection');
                mysql.createConnection.and.callFake(() => {
                    throw expectedError;
                })

                try {
                    await eventstorePlaybackList.init();
                } catch (error) {
                    expect(error).toEqual(expectedError);
                    done();
                }

            });
        })

        describe('creating the table', () => {
            it('should add the virtual columns/fields', async (done) => {
                options.fields = [{
                    name: 'field_1',
                    type: 'string'
                }];

                eventstorePlaybackList = new EventstorePlaybackList(options);
                await eventstorePlaybackList.init();
                // TODO: research on the best way to check if the create table call is called with the correct generated columns
                expect(mysqlConnection.query).toHaveBeenCalledWith(jasmine.stringMatching(/`field_1`/), undefined, jasmine.any(Function));
                done();
            });

            it('should add the correct index', async (done) => {
                options.secondaryKeys = {
                    idx_field_1: [{
                        name: 'field_1',
                        sort: 'asc'
                    }]
                }

                eventstorePlaybackList = new EventstorePlaybackList(options);
                await eventstorePlaybackList.init();
                // TODO: research on the best way to check if the create table call is called with the correct generated indices
                expect(mysqlConnection.query).toHaveBeenCalledWith(jasmine.stringMatching(/`idx_field_1`/), undefined, jasmine.any(Function));
                done();
            });

            it('should handle the error in creating the table and just rethrow', async (done) => {
                const expectedError = new Error('error in creating the table');;
                mysqlConnection.query.and.callFake(function(queryText, queryParams, cb) {
                    throw expectedError;
                });
                try {
                    await eventstorePlaybackList.init();
                } catch (error) {
                    expect(error).toEqual(expectedError);
                    done();
                }
            });
        })
    });

    describe('query', () => {
        describe('should do paging', () => {
            it('should pass the correct paging parameters to mysql', async (done) => {
                // start, limit, filters, sort
                const start = 1;
                const limit = 10;
                const filters = null;
                const sort = null;
                await eventstorePlaybackList.init();
                eventstorePlaybackList.query(start, limit, filters, sort, function() {
                    expect(mysqlConnection.query).toHaveBeenCalledWith('SELECT COUNT(1) as total_count FROM list_name', undefined, jasmine.any(Function));
                    expect(mysqlConnection.query).toHaveBeenCalledWith('SELECT * FROM list_name WHERE 1 = 1   LIMIT ?,?', [
                        start,
                        limit
                    ], jasmine.any(Function));
                    done();
                });
            })

            it('should handle error and rethrow if any errors in getting the count', async (done) => {
                const expectedError = new Error('error in mysql');

                const start = 1;
                const limit = 10;
                const filters = [];
                const sort = [];
                await eventstorePlaybackList.init();
                mysqlConnection.query.and.callFake(function(query, queryParams, cb) {
                    if (_.includes(query, 'COUNT')) {
                        throw expectedError;
                    } else {
                        cb();
                    }
                });

                
                eventstorePlaybackList.query(start, limit, filters, sort, function(err) {
                    expect(err).toEqual(expectedError);
                    done();
                });
            });

            it('should handle error and rethrow if any errors in getting the list', async (done) => {
                const expectedError = new Error('error in mysql');

                const start = 1;
                const limit = 10;
                const filters = [];
                const sort = [];
                await eventstorePlaybackList.init();
                mysqlConnection.query.and.callFake(function(query, queryParams, cb) {
                    if (_.includes(query, 'COUNT')) {
                        cb(null, [{
                            total_count: 1
                        }]);
                    } else if (_.includes(query, 'SELECT * FROM list_name')) {
                        throw expectedError;
                    } else {
                        cb();
                    }
                });
                
                eventstorePlaybackList.query(start, limit, filters, sort, function(err) {
                    expect(err).toEqual(expectedError);
                    done();
                });
            });
        });

        describe('should do filtering', () => {
          it('should filter using "is" filter type', async (done) => {
              // start, limit, filters, sort
              const start = 1;
              const limit = 10;
              const filters = [{
                  field: 'field_1',
                  operator: 'is',
                  value: 'value_1'
              }];
              const sort = null;
              await eventstorePlaybackList.init();
              eventstorePlaybackList.query(start, limit, filters, sort, function() {
                  expect(mysqlConnection.query).toHaveBeenCalledWith('SELECT COUNT(1) as total_count FROM list_name', undefined, jasmine.any(Function));
                  expect(mysqlConnection.query).toHaveBeenCalledWith(`SELECT * FROM list_name WHERE 1 = 1  AND  ( field_1 = 'value_1' )   LIMIT ?,?`, [
                      start,
                      limit
                  ], jasmine.any(Function));
                  done();
              });
          });

          it('should filter using "any" filter type', async (done) => {
              // start, limit, filters, sort
              const start = 1;
              const limit = 10;
              const filters = [{
                  field: 'field_1',
                  operator: 'any',
                  value: ['value_1', 'value_2']
              }];
              const sort = null;
              await eventstorePlaybackList.init();
              eventstorePlaybackList.query(start, limit, filters, sort, function() {
                  expect(mysqlConnection.query).toHaveBeenCalledWith('SELECT COUNT(1) as total_count FROM list_name', undefined, jasmine.any(Function));
                  expect(mysqlConnection.query).toHaveBeenCalledWith(`SELECT * FROM list_name WHERE 1 = 1  AND  ( field_1 IN ('value_1','value_2') )  LIMIT ?,?`, [
                      start,
                      limit
                  ], jasmine.any(Function));
                  done();
              });
          });

          it('should filter using "range" filter type', async (done) => {
              // start, limit, filters, sort
              const start = 1;
              const limit = 10;
              const filters = [{
                  field: 'field_1',
                  operator: 'range',
                  from: 'value_1',
                  to: 'value_1'
              }];
              const sort = null;
              await eventstorePlaybackList.init();
              eventstorePlaybackList.query(start, limit, filters, sort, function() {
                  expect(mysqlConnection.query).toHaveBeenCalledWith('SELECT COUNT(1) as total_count FROM list_name', undefined, jasmine.any(Function));
                  expect(mysqlConnection.query).toHaveBeenCalledWith(`SELECT * FROM list_name WHERE 1 = 1  AND  ( field_1 >= value_1  AND field_1 <= value_1 )   LIMIT ?,?`, [
                      start,
                      limit
                  ], jasmine.any(Function));
                  done();
              });
          });

          it('should filter using "contains" filter type', async (done) => {
            // start, limit, filters, sort
            const start = 1;
            const limit = 10;
            const filters = [{
                field: 'field_1',
                operator: 'contains',
                value: 'Fols'
            }];
            const sort = null;
            await eventstorePlaybackList.init();
            eventstorePlaybackList.query(start, limit, filters, sort, function() {
                expect(mysqlConnection.query).toHaveBeenCalledWith('SELECT COUNT(1) as total_count FROM list_name', undefined, jasmine.any(Function));
                expect(mysqlConnection.query).toHaveBeenCalledWith(`SELECT * FROM list_name WHERE 1 = 1  AND  ( field_1 LIKE '%Fols%' )   LIMIT ?,?`, [
                    start,
                    limit
                ], jasmine.any(Function));
                done();
            });
          });

          it('should filter using "arrayContains" filter type - using non-array value', async (done) => {
            // start, limit, filters, sort
            const start = 1;
            const limit = 10;
            const filters = [{
                field: 'field_1',
                group: 'group',
                groupBooleanOperator: 'or',
                operator: 'arrayContains',
                value: 'Fols'
            }];
            const sort = null;
            await eventstorePlaybackList.init();
            eventstorePlaybackList.query(start, limit, filters, sort, function() {
                expect(mysqlConnection.query).toHaveBeenCalledWith('SELECT COUNT(1) as total_count FROM list_name', undefined, jasmine.any(Function));
                expect(mysqlConnection.query).toHaveBeenCalledWith(`SELECT * FROM list_name WHERE 1 = 1  AND  ( JSON_CONTAINS(field_1, '"Fols"') )   LIMIT ?,?`, [
                    start,
                    limit
                ], jasmine.any(Function));
                done();
            });
          });

          it('should filter using "arrayContains" filter type - using array value', async (done) => {
            // start, limit, filters, sort
            const start = 1;
            const limit = 10;
            const filters = [
              {
                field: 'field_1',
                group: 'group',
                groupBooleanOperator: 'or',
                operator: 'arrayContains',
                value: [1, 2, 3]
              }
            ];
            const sort = null;
            await eventstorePlaybackList.init();
            eventstorePlaybackList.query(start, limit, filters, sort, function() {
                expect(mysqlConnection.query).toHaveBeenCalledWith('SELECT COUNT(1) as total_count FROM list_name', undefined, jasmine.any(Function));
                expect(mysqlConnection.query).toHaveBeenCalledWith(`SELECT * FROM list_name WHERE 1 = 1  AND  ( JSON_CONTAINS(field_1, '"1"') )  OR  ( JSON_CONTAINS(field_1, '"2"') )  OR  ( JSON_CONTAINS(field_1, '"3"') )   LIMIT ?,?`, [
                    start,
                    limit
                ], jasmine.any(Function));
                done();
            });
          });

          it('should filter using filter group', async(done) => {
            // start, limit, filters, sort
            const start = 1;
            const limit = 10;
            const filters = [
              {
                field: 'field_1',
                operator: 'is',
                group: 'group_1',
                groupBooleanOperator: 'or',
                value: 'First'
              },
              {
                field: 'field_2',
                operator: 'is',
                group: 'group_1',
                groupBooleanOperator: 'or',
                value: 'Second'
              },
              {
                field: 'field_3',
                operator: 'is',
                group: 'group_2',
                groupBooleanOperator: 'and',
                value: 'Third'
              },
              {
                field: 'field_4',
                operator: 'is',
                group: 'group_2',
                groupBooleanOperator: 'and',
                value: 'Fourth'
              },
            ];
            const sort = null;
            await eventstorePlaybackList.init();
            eventstorePlaybackList.query(start, limit, filters, sort, function() {
                expect(mysqlConnection.query).toHaveBeenCalledWith('SELECT COUNT(1) as total_count FROM list_name', undefined, jasmine.any(Function));
                expect(mysqlConnection.query).toHaveBeenCalledWith(`SELECT * FROM list_name WHERE 1 = 1  AND  ( field_1 = 'First'   OR field_2 = 'Second' )  AND  ( field_3 = 'Third'   AND field_4 = 'Fourth' )   LIMIT ?,?`, [
                    start,
                    limit
                ], jasmine.any(Function));
                done();
            });
          });
      })

        describe('should do sorting', () => {
            it('should do asc sorting', async (done) => {
                // start, limit, filters, sort
                const start = 1;
                const limit = 10;
                const filters = [];
                const sort = [{
                    field: 'field_1',
                    sort: 'asc'
                }];
                await eventstorePlaybackList.init();
                eventstorePlaybackList.query(start, limit, filters, sort, function() {
                    expect(mysqlConnection.query).toHaveBeenCalledWith('SELECT COUNT(1) as total_count FROM list_name', undefined, jasmine.any(Function));
                    expect(mysqlConnection.query).toHaveBeenCalledWith(`SELECT * FROM list_name WHERE 1 = 1  ORDER BY field_1 asc LIMIT ?,?`, [
                        start,
                        limit
                    ], jasmine.any(Function));
                    done();
                });
            });

            it('should do desc sorting', async (done) => {
                // start, limit, filters, sort
                const start = 1;
                const limit = 10;
                const filters = [];
                const sort = [{
                    field: 'field_1',
                    sort: 'desc'
                }];
                await eventstorePlaybackList.init();
                eventstorePlaybackList.query(start, limit, filters, sort, function() {
                    expect(mysqlConnection.query).toHaveBeenCalledWith('SELECT COUNT(1) as total_count FROM list_name', undefined, jasmine.any(Function));
                    expect(mysqlConnection.query).toHaveBeenCalledWith(`SELECT * FROM list_name WHERE 1 = 1  ORDER BY field_1 desc LIMIT ?,?`, [
                        start,
                        limit
                    ], jasmine.any(Function));
                    done();
                });
            });
        })

        describe('should return data', () => {
            it('should return the correct data structure', async (done) => {
                const expectedTotalCount = 1;
                const expectedResult = {
                    count: expectedTotalCount,
                    rows: [{
                        data: {
                            field_1: 'field_1_value'
                        },
                        meta: {
                            field_1: 'field_1_value'
                        },
                        rowId: 1,
                        revision: 1
                    }]
                }

                const start = 1;
                const limit = 10;
                const filters = [];
                const sort = [{
                    field: 'field_1',
                    sort: 'asc'
                }];
                await eventstorePlaybackList.init();
                mysqlConnection.query.and.callFake(function(query, queryParams, cb) {
                    if (_.includes(query, 'COUNT')) {
                        cb(null, [{
                            total_count: expectedTotalCount
                        }]);
                    } else if (_.includes(query, 'SELECT * FROM list_name')) {
                        cb(null, [{
                            row_id: expectedResult.rows[0].rowId,
                            row_revision: expectedResult.rows[0].revision,
                            row_json: JSON.stringify(expectedResult.rows[0].data),
                            meta_json: JSON.stringify(expectedResult.rows[0].meta)
                        }]);
                    } else {
                        cb();
                    }
                });

                
                eventstorePlaybackList.query(start, limit, filters, sort, function(err, results) {
                    expect(results).toEqual(expectedResult);
                    done();
                });
            });
        })
    })
})
