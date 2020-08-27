const bluebird = require('bluebird');
const events = require('events');
const eventEmitter = new events.EventEmitter();
const shortid = require('shortid');

const EVENTS_TO_EMIT_COUNT = 1000;

suite('Eventstore subscribe benchmark - on growing subscribers', () => {
    set('iterations', EVENTS_TO_EMIT_COUNT);
    set('type', 'static');
    set('concurrency', 1);

    let eventstore;
    let id = shortid.generate();
    let vehicleId = `vehicle-${id}`;
    let scivId = `sciv-${id}`;

    const query = {
        context: 'auction',
        aggregate: 'salesChannelInstanceVehicle',
        aggregateId: scivId
    };
    const subscribers = [];
    const eventCallbackCount = {};
    const eventCallbackCountReq = {};
    const eventCallbackEmitted = {};
    let subscribersCount = 0;

    const onEventCallback = function(err, event, callback) {
        const iteration = event.payload.payload.iteration;
        // console.log('Got event iteration', iteration);
        // console.log('   Subscriber count requirement:', eventCallbackCountReq[iteration]);
        if (!eventCallbackCount[iteration]) {
            eventCallbackCount[iteration] = 0;
        }
        eventCallbackCount[iteration]++;
        // console.log('   Current counter for iteration:', eventCallbackCount[iteration]);

        if (eventCallbackCount[iteration] >= eventCallbackCountReq[iteration]) {
            if (!eventCallbackEmitted[iteration]) {
                eventCallbackEmitted[iteration] = true;
                eventEmitter.emit(`event-${iteration}`);
                // console.log('GOT MAX SUB WITH', iteration, ':', eventCallbackCount[iteration], '/', eventCallbackCountReq[iteration]);
            }
        } 
        // else {
            // console.log('INCOMPLETE SUB', eventCallbackCount[iteration], '/', eventCallbackCountReq[iteration]);
        // }
        callback();
    };

    before(async function(next) {
        console.log('BEFORE ALL TRIGGERED FOR SCIV ', scivId);
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
            pollingTimeout: 1000,
            pollingMaxRevisions: 5,
            errorMaxRetryCount: 2,
            errorRetryExponent: 2,
            enableProjection: true
        });
        bluebird.promisifyAll(eventstore);
        await eventstore.initAsync();

        await eventstore.startAllProjectionsAsync();
        console.log('BEFORE ALL TRIGGERED DONE FOR SCIV', scivId);
        next();
    });

    let iterationCounter = 0;
    bench(`emit ${EVENTS_TO_EMIT_COUNT} leader_computed events (${scivId}) up to ${EVENTS_TO_EMIT_COUNT/2} subscribers and await all callbacks`, async function(next) {
        iterationCounter++;
        // console.log('Bench iteration', iterationCounter);
        if (iterationCounter % 2 === 1) {
            // console.log('Adding new subscriber');
            subscribersCount++;
            eventCallbackCountReq[iterationCounter] = subscribersCount;
            const subscriberToken = await eventstore.subscribe(scivId, iterationCounter-10 > 0 ? iterationCounter-10 : 0, onEventCallback, (error) => {
                console.error('onErrorCallback received error', error);
            });
            subscribers.push(subscriberToken);
        } else {
            eventCallbackCountReq[iterationCounter] = subscribersCount;
        }

        eventEmitter.on(`event-${iterationCounter}`, function() {
            // console.log(scivId, 'eventEmitter event for iteration', iterationCounter);
            next();
        });
        const event = {
            name: 'sales_channel_instance_vehicle_leader_computed',
            payload: {
                previousLeader: null,
                extensionEndsAt: 0,
                bidders: null,
                salesChannelInstanceVehicleId: scivId,
                isReserveMet: false,
                vehicleId: vehicleId,
                iteration: iterationCounter // for benchmark tracking
            }
        };
        const stream = await eventstore.getLastEventAsStreamAsync(query);
        bluebird.promisifyAll(stream);
        stream.addEvent(event);
        await stream.commitAsync();
    });

    after(function(next) {
        console.log('AFTER ALL TRIGGERED FOR SCIV ', scivId);
        for(let i = 0; i < subscribers.length; i++) {
            const subscriberToken = subscribers[i];
            eventstore.unsubscribe(subscriberToken);
        }
        console.log('AFTER ALL TRIGGERED DONE FOR SCIV', scivId);
        next();
    });
});
