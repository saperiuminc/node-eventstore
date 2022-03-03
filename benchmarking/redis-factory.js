const Redis = require('ioredis');

const createRedisFactory = function() {
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

module.exports = createRedisFactory();