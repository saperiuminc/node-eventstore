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
    pollingTimeout: 60000
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
            projectionId: 'dummy-projection-id',
            query: {
                context: 'dummy_context',
                aggregate: 'dummy_aggregate'
            },
            function(err, event) {
                console.log('got event', event);
            },
            partitionBy: 'instance'
        });
    }
});

module.exports = eventstore;

const abc =require('../../../index')({
});
