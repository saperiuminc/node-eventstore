const eventstore = require('../index')({
    type: 'mysql',
    host: '127.0.0.1',
    port: 3306,
    user: 'root',
    password: 'root',
    database: 'eventstore',
    redisConfig: {
        host: 'localhost',
        port: 6379
    }
});

setTimeout(() => {
    eventstore.init(function(err) {
        if (err) {
            console.error(err);
            console.error('error in init');
        } else {
            console.log('es initialized');
        }
    }); 
}, 5000);
