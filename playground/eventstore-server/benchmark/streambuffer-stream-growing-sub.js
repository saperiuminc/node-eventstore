const bluebird = require('bluebird');
const events = require('events');
const eventEmitter = new events.EventEmitter();
const shortid = require('shortid');

const STREAMS_COUNT = 500;
const EVENTS_TO_EMIT_COUNT = 1000;

suite('Eventstore subscribe benchmark - on growing subscribers', () => {
    set('iterations', EVENTS_TO_EMIT_COUNT);
    set('type', 'static');
    set('concurrency', 1);

    let eventstore;
    
    const ids = [];
    const subscribers = [];
    const eventCallbackCount = {};
    const eventCallbackCountReq = {};
    const eventCallbackEmitted = {};
    let subscribersCount = 0;

    const onEventCallback = function(err, event, callback) {
        const iteration = event.payload.payload.iteration;
        // const scivId = event.payload.payload.salesChannelInstanceVehicleId;
        // console.log('Got event iteration', iteration, scivId);
        // console.log('   Subscriber count requirement:', eventCallbackCountReq[iteration], scivId);
        if (!eventCallbackCount[iteration]) {
            eventCallbackCount[iteration] = 0;
        }
        eventCallbackCount[iteration]++;
        // console.log('   Current counter for iteration:', eventCallbackCount[iteration], scivId);

        if (eventCallbackCount[iteration] >= eventCallbackCountReq[iteration]) {
            if (!eventCallbackEmitted[iteration]) {
                eventCallbackEmitted[iteration] = true;
                eventEmitter.emit(`event-${iteration}`);
                // console.log('GOT MAX SUB WITH', iteration, ':', eventCallbackCount[iteration], '/', eventCallbackCountReq[iteration], scivId);
            }
        } 
        else {
            // console.log('INCOMPLETE SUB', eventCallbackCount[iteration], '/', eventCallbackCountReq[iteration], scivId);
        }
        callback();
    };

    before(async function(next) {
        // console.log('BEFORE ALL TRIGGERED');
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

        for(let i = 0; i < STREAMS_COUNT; i++) {
            // Generate Ids for Multiple Streams
            let id = shortid.generate();
            ids.push(id);
        }

        await eventstore.startAllProjectionsAsync();
        // console.log('BEFORE ALL TRIGGERED DONE');
        next();
    });

    let iterationCounter = 0;
    bench(`emit ${EVENTS_TO_EMIT_COUNT} leader_computed events to up to ${EVENTS_TO_EMIT_COUNT/2} subscribers, each subscribed to ${STREAMS_COUNT} streams, and await all callbacks`, async function(next) {
        iterationCounter++;
        // console.log('Bench iteration', iterationCounter);
        if (iterationCounter % 2 === 1) {
            // console.log('Adding new subscriber');
            subscribersCount++;
            eventCallbackCountReq[iterationCounter] = subscribersCount;
            for(let i = 0; i < STREAMS_COUNT; i++) {
                // Subscribe the new user for each stream
                const id = ids[i];
                let scivId = `sciv-${id}`;
                // console.log('SUBSCRIBED', scivId);
                const subscriberToken = await eventstore.subscribe(scivId, iterationCounter-10 > 0 ? iterationCounter-10 : 0, onEventCallback, (error) => {
                    console.error('onErrorCallback received error', error);
                });
                subscribers.push(subscriberToken);
            }
        } else {
            eventCallbackCountReq[iterationCounter] = subscribersCount;
        }

        // Send an Event to a random stream; Get the index of that stream
        const streamIndex = Math.floor(Math.random() * Math.floor(STREAMS_COUNT));
        // Get the id associated to that index
        const id = ids[streamIndex];

        let vehicleId = `vehicle-${id}`;
        let scivId = `sciv-${id}`;
        // console.log('Emitting to', scivId);
        const query = {
            context: 'auction',
            aggregate: 'salesChannelInstanceVehicle',
            aggregateId: scivId
        };

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
        // console.log('AFTER ALL TRIGGERED');
        for(let i = 0; i < subscribers.length; i++) {
            const subscriberToken = subscribers[i];
            eventstore.unsubscribe(subscriberToken);
        }
        // console.log('AFTER ALL TRIGGERED DONE');
        next();
    });
});
