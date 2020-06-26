const eventstore = require('@saperiuminc/eventstore')({
    type: 'mysql',
    host: process.env.EVENTSTORE_MYSQL_HOST,
    port: process.env.EVENTSTORE_MYSQL_PORT,
    user: process.env.EVENTSTORE_MYSQL_USERNAME,
    password: process.env.EVENTSTORE_MYSQL_PASSWORD,
    database: process.env.EVENTSTORE_MYSQL_DATABASE,
    redisConfig: {
        host: process.env.REDIS_HOST,
        port: process.env.REDIS_PORT
    },
    pollingTimeout: 10000
});

eventstore.init(function(err) {
    if (err) {
        console.error(err);
        console.error('error in init');
    } else {
        console.log('es initialized');

        // some dummy calls for testing
        eventstore.subscribe('dummy_stream_id', 0, (err, event) => {
            console.log('received event');
        });

        eventstore.project({
            projectionId: 'dummy-projection-id-1',
            playbackInterface: {
                $init: function() {
                    return {
                        count: 0
                    }
                },
                DUMMY_CREATED: function(state, event, funcs, done) {
                    state.count++;
                    done();
                }
            },
            query: {
                context: 'dummy_context',
                aggregate: 'dummy_aggregate'
            },
            partitionBy: '',
            outputState: 'true'
        });

        eventstore.startAllProjections((err) => {
            if (err) {
                console.error('error in startAllProjections');
            } else {
                console.log('startAllProjections done');
            }
        })
    }
});

module.exports = eventstore;