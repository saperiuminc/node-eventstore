const bluebird = require('bluebird');
const events = require('events');
const eventEmitter = new events.EventEmitter();
const shortid = require('shortid');

const STREAMS_COUNT = 2000;
    // # of streamBuffers & subscription pools
const EVENTS_TO_EMIT_COUNT = 1000;
const SUBSCRIBERS_COUNT = 1000;
const SUBSCRIPTIONS_PER_SUBSCRIBER_COUNT = 26;

const CONNECTION_POOL_LIMIT = 1;

suite('Eventstore subscribe benchmark - limited subscriptions per subscriber', () => {
    set('iterations', EVENTS_TO_EMIT_COUNT);
    set('type', 'static');
    set('concurrency', 1);

    let eventstore;

    const ids = [];
    // const streamSubscribers = {}; // Map of Array of Subscribers. Key = id, value = array of subscribers
    const subscribers = [];
    const eventCallbackCount = {};
    const eventCallbackCountReq = {};
    const eventCallbackEmitted = {};

    const onEventCallback = function(err, event, callback) {
        const iteration = event.payload.payload.iteration;
        const scivId = event.payload.payload.salesChannelInstanceVehicleId;
        // console.log('Got event iteration', iteration);
        // console.log('   Subscriber count requirement:', eventCallbackCountReq[scivId]);
        if (!eventCallbackCount[iteration]) {
            eventCallbackCount[iteration] = 0;
        }
        eventCallbackCount[iteration]++;
        // console.log('   Current counter for iteration:', eventCallbackCount[iteration]);

        if (eventCallbackCount[iteration] >= eventCallbackCountReq[scivId]) {
            if (!eventCallbackEmitted[iteration]) {
                eventCallbackEmitted[iteration] = true;
                eventEmitter.emit(`event-${iteration}`);
                // console.log('GOT MAX SUB WITH', iteration, ':', eventCallbackCount[iteration], '/', eventCallbackCountReq[scivId]);
            }
        }
        else {
            // console.log('INCOMPLETE SUB', eventCallbackCount[iteration], '/', eventCallbackCountReq[scivId]);
        }
        callback();
    };

    before(async function(next) {
        console.log('BEFORE ALL TRIGGERED');
        eventstore = require('@saperiuminc/eventstore')({
            type: 'mysql',
            host: process.env.EVENTSTORE_MYSQL_HOST,
            port: process.env.EVENTSTORE_MYSQL_PORT,
            user: process.env.EVENTSTORE_MYSQL_USERNAME,
            password: process.env.EVENTSTORE_MYSQL_PASSWORD,
            database: process.env.EVENTSTORE_MYSQL_DATABASE,
            connectionPoolLimit: CONNECTION_POOL_LIMIT,
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
            pollingTimeout: 60000,
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

        let subscriptionIterator = 0;
        for(let i = 0; i < SUBSCRIBERS_COUNT; i++) {
            // For each subscriber, subscribe up to SUBSCRIPTIONS_PER_SUBSCRIBER_COUNT streams,
            // But the assignment of the streams to subscribe on are spread via round-robin.
            for(let j = 0; j < SUBSCRIPTIONS_PER_SUBSCRIBER_COUNT; j++) {
                // Use modulo for round-robin assignment
                const streamIdIndex = subscriptionIterator % STREAMS_COUNT;
                subscriptionIterator++;

                const id = ids[streamIdIndex];
                const scivId = `sciv-${id}`;
                const subscriberToken = await eventstore.subscribe(scivId, 0, onEventCallback, (error) => {
                    console.error('onErrorCallback received error', error);
                });

                // Increase the counter requirement for that stream
                if (!eventCallbackCountReq[scivId]) {
                    eventCallbackCountReq[scivId] = 1;
                } else {
                    eventCallbackCountReq[scivId] = eventCallbackCountReq[scivId] + 1;
                }

                // streamSubscribers[id].push(subscriberToken);
                subscribers.push(subscriberToken);
            }
        }
        // console.log('SUB REQ FOR ', ids[0], eventCallbackCountReq[`sciv-${ids[0]}`]);
        // console.log('SUB REQ FOR ', ids[1], eventCallbackCountReq[`sciv-${ids[1]}`]);

        await eventstore.startAllProjectionsAsync();
        setTimeout(() => {
            console.log('BEFORE ALL TRIGGERED DONE');
            next();
        }, 40000);
    });

    let iterationCounter = 0;
    bench(`emit ${EVENTS_TO_EMIT_COUNT} leader_computed events to a pool of ${SUBSCRIBERS_COUNT} subscribers, each exactly subscribed to ${SUBSCRIPTIONS_PER_SUBSCRIBER_COUNT} streams across a total of ${STREAMS_COUNT} streams, and await all callbacks`, async function(next) {
        iterationCounter++;
        
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
            // console.log(scivId, 'eventEmitter event for iteration', iterationCounter, scivId);
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
        console.log('AFTER ALL TRIGGERED');
        for(let i = 0; i < subscribers.length; i++) {
            const subscriberToken = subscribers[i];
            eventstore.unsubscribe(subscriberToken);
        }
        console.log('AFTER ALL TRIGGERED DONE');
        next();
    });
});
