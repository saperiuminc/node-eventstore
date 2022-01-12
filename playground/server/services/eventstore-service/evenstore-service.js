class EventstoreService {
    #eventstore;

    constructor() {
        this.#eventstore = undefined;
    }

    async init() {
        const Bluebird = require('bluebird');
        const mysqlConfig = {
            host: 'db',
            port: 3306,
            database: 'eventstore',
            user: 'root',
            password: 'root'
        }

        const redisConfig = {
            host: 'redis',
            port: 6379
        }
        
        const Redis = require('ioredis');
        const redisClient = new Redis(redisConfig.port, redisConfig.host);
        const redisSubscriber = new Redis(redisConfig.port, redisConfig.host);

        // initialize the eventstore
        const eventstore = require('@saperiuminc/eventstore')({
            type: 'mysql',
            host: mysqlConfig.host,
            port: mysqlConfig.port,
            user: mysqlConfig.user,
            password: mysqlConfig.password,
            database: mysqlConfig.database,
            connectionPoolLimit: 10,
            // projections-specific configuration below
            redisCreateClient: function(type) {
                switch (type) {
                    case 'client':
                        return redisClient;

                    case 'subscriber':
                        return redisSubscriber;
                }
            },
            listStore: {
                type: 'mysql',
                connection: {
                    host: mysqlConfig.host,
                    port: mysqlConfig.port,
                    user: mysqlConfig.user,
                    password: mysqlConfig.password,
                    database: mysqlConfig.database
                },
                pool: {
                    min: 10,
                    max: 10
                }
            }, // required
            projectionStore: {
                type: 'mysql',
                connection: {
                    host: mysqlConfig.host,
                    port: mysqlConfig.port,
                    user: mysqlConfig.user,
                    password: mysqlConfig.password,
                    database: mysqlConfig.database
                },
                pool: {
                    min: 10,
                    max: 10
                }
            }, // required
            enableProjection: true,
            enableProjectionEventStreamBuffer: true,
            eventCallbackTimeout: 1000,
            lockTimeToLive: 1000,
            pollingTimeout: 1000,
            pollingMaxRevisions: 100,
            errorMaxRetryCount: 2,
            errorRetryExponent: 2,
            playbackEventJobCount: 10,
            context: 'vehicle',
            projectionGroup: 'default'
        });

        Bluebird.promisifyAll(eventstore);
        await eventstore.initAsync();

        eventstore.on('rebalance', function() {
            console.info('evenstore rebalance occurred');
        });

        eventstore.on('playbackSuccess', function(data) {
        });

        this.#eventstore = eventstore;
    }

    getEventstore() {
        return this.#eventstore;
    }
}

module.exports = EventstoreService;