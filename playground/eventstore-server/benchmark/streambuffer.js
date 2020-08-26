const bluebird = require('bluebird');
const events = require('events');
const eventEmitter = new events.EventEmitter();
const shortid = require('shortid');

const iterations = 1000;

suite('Test suite', () => {
    set('iterations', iterations);
    set('type', 'static');
    set('concurrency', 1);

    const subscriberCount = 500;
    let eventstore;
    // let stream;
    let vehicleId = `vehicle-${shortid.generate()}`;
    const query = {
        context: 'vehicle',
        aggregate: 'vehicle',
        aggregateId: vehicleId
    };
    let callbackCount = {};

    before(async function(next) {
        console.log('BEFORE ALL TRIGGERED FOR VEHICLE ', vehicleId);
        eventstore = require('@saperiuminc/eventstore')({
            type: 'mysql',
            host: process.env.EVENTSTORE_MYSQL_HOST,
            port: process.env.EVENTSTORE_MYSQL_PORT,
            user: process.env.EVENTSTORE_MYSQL_USERNAME,
            password: process.env.EVENTSTORE_MYSQL_PASSWORD,
            database: process.env.EVENTSTORE_MYSQL_DATABASE,
            // projections-specific configuration below
            redisConfig: {
                host: process.env.REDIS_HOST,
                port: process.env.REDIS_PORT
            }, // required
            listStore: {
                host: process.env.EVENTSTORE_MYSQL_HOST,
                port: process.env.EVENTSTORE_MYSQL_PORT,
                user: process.env.EVENTSTORE_MYSQL_USERNAME,
                password: process.env.EVENTSTORE_MYSQL_PASSWORD,
                database: process.env.EVENTSTORE_MYSQL_DATABASE
            }, // required
            eventCallbackTimeout: 1000,
            pollingTimeout: 1000, // optional,
            pollingMaxRevisions: 5,
            errorMaxRetryCount: 2,
            errorRetryExponent: 2,
            enableProjectionSubscribe: true,
            enableProjectionPublish: true
        });
        bluebird.promisifyAll(eventstore);
        await eventstore.initAsync();

        for(let i = 0; i < subscriberCount; i++) {
            await eventstore.subscribe(vehicleId, 0, (err, event, callback) => {
                const iteration = event.payload.payload.iteration;
                if (!callbackCount[iteration]) {
                    callbackCount[iteration] = 0;
                }
                callbackCount[iteration]++;

                if (callbackCount[iteration] >= subscriberCount) {
                    // console.log('GOT MAX SUB');
                    eventEmitter.emit(`event-${iteration}`);
                } 
                // else {
                    // console.log('INCOMPLETE SUB', callbackCount[iteration]);
                // }
                callback();
            }, (error) => {
                console.error('onErrorCallback received error', error);
            });
        }

        await eventstore.startAllProjectionsAsync();
        console.log('BEFORE ALL TRIGGERED DONE FOR VEHICLE', vehicleId);
        next();
    });

    let iters = 0;
    bench('emit vehicle_created event to all subscribers', async function(next) {
        iters++;

        eventEmitter.on(`event-${iters}`, function() {
            // console.log(vehicleId, 'eventEmitter event for iteration', iters);
            next();
        });
        const event = {
            name: "VEHICLE_CREATED",
            payload: {
                vehicleId: vehicleId,
                year: 2012,
                make: 'Honda',
                model: 'Jazz',
                mileage: 1245,
                iteration: iters
            }
        };
        const stream = await eventstore.getLastEventAsStreamAsync(query);
        bluebird.promisifyAll(stream);
        stream.addEvent(event);
        await stream.commitAsync();
    });
});
