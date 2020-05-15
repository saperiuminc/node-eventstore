const EventStoreWithProjection = require('../lib/eventstore-projections/eventstore-projection');

describe('eventstore-projection tests', () => {
    let esWithProjection;
    beforeEach(() => {

        esWithProjection = new EventStoreWithProjection();

        esWithProjection.getLastEventAsStream = spyOn(esWithProjection, 'getLastEventAsStream');
        esWithProjection.getLastEventAsStream.and.callFake((query, cb) => {
            cb();
        })
    });

    describe('project', () => {
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
})