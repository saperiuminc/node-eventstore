const bluebird = require('bluebird');

module.exports = (async function() {
    const eventstore = require('@saperiuminc/eventstore')({
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

        pollingTimeout: 10000 // optional
    });

    bluebird.promisifyAll(eventstore);

    try {
        await eventstore.initAsync();

        console.log('eventstore initialized');

        // some dummy calls for testing
        eventstore.subscribe('dummy-projection-id-1-result', 0, (err, event) => {
            console.log('received event', event);
        });

        // neeed to await the project call to initalize the playback list
        await eventstore.projectAsync({
            projectionId: 'vehicle-list',
            playbackInterface: {
                $init: function() {
                    return {
                        count: 0
                    }
                },
                /**
                 * @param {import('./types').VehicleListState} state the name of the playback list
                 * @param {import('./types').VehicleCreatedEvent} event the name of the playback list
                 * @param {Object} funcs the last event that built this projection state
                 * @param {Function} done the last event that built this projection state
                 * @returns {void} Returns void. Use the callback to the get playbacklist
                 */
                VEHICLE_CREATED: function(state, event, funcs, done) {
                    funcs.getPlaybackList('vehicle_list', function(err, playbackList) {
                        console.log('got vehicle_created event', event);
                        const eventPayload = event.payload.payload;
                        const data = {
                            vehicleId: eventPayload.vehicleId,
                            year: eventPayload.year,
                            make: eventPayload.make,
                            model: eventPayload.model,
                            mileage: eventPayload.mileage
                        };
                        playbackList.add(event.aggregateId, event.streamRevision, data, {}, function(err) {
                            state.count++;
                            done();
                        })
                    });
                }
            },
            query: {
                context: 'vehicle',
                aggregate: 'vehicle'
            },
            partitionBy: '',
            outputState: 'true',
            playbackList: {
                name: 'vehicle_list',
                fields: [{
                    name: 'vehicleId',
                    type: 'string'
                }],
                secondaryKeys: {
                    idx_vehicleId: [{
                        name: 'vehicleId',
                        sort: 'ASC'
                    }]
                }
            },
            stateList: {
                name: 'vehicle_state_list',
                fields: [{
                    name: 'vehicleId',
                    type: 'string'
                }],
                secondaryKeys: {
                    idx_vehicleId: [{
                        name: 'vehicleId',
                        sort: 'ASC'
                    }]
                }
            }
        });

        await eventstore.registerPlaybackListViewAsync(
            'vehicle_list_view',
            `
            SELECT
                *
            FROM vehicle_list v;`);


        await eventstore.startAllProjectionsAsync();
    } catch (error) {
        console.error('error in setting up the projection', error);
    }


    return eventstore;
})();