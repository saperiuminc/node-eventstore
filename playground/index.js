const Redis = require('ioredis');
const redisFactory = function() {
    const options = {
        port: 6379,
        host: 'localhost'
    };
    const redisClient = new Redis(options);
    const redisSubscriber = new Redis(options);

    return {
        createClient: function(type) {
            switch (type) {
                case 'client':
                    return redisClient;
                case 'bclient':
                    return new Redis(options); // always create a new one
                case 'subscriber':
                    return redisSubscriber;
                default:
                    return new Redis(options);
            }
        }
    };
};

const eventstore = require('../index')({
    type: 'mysql',
    host: '127.0.0.1',
    port: 3306,
    user: 'root',
    password: 'root',
    database: 'eventstore',
    redisCreateClient: redisFactory.createClient
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
