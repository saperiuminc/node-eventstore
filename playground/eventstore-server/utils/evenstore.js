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
    pollingTimeout: 10000
});

eventstore.init(function(err) {
    if (err) {
        console.error(err);
        console.error('error in init');
    } else {
        console.log('es initialized');

        // some dummy calls for testing
        eventstore.subscribe('dummy-projection-id-1-result', 0, (err, event) => {
            console.log('received event', event);
        });

        // this code is in vehicle micro
        // expectation is saleschannelinstancevehicle is bound to the vehicle micro
        eventstore.project({
            projectionId: 'titles-dashboard-vehicle-stream',
            playbackInterface: {
                $init: function() {
                    return {
                        count: 0
                    }
                },
                SALES_CHANNEL_INSTANCE_VEHICLE_SOLD: function(state, event, funcs, done) {
                    const targetQuery = {
                        context: 'vehicle',
                        aggregate: 'titles-dashboard-vehicle-stream',
                        aggregateId: event.payload.vehicleId
                    }
                    funcs.emit(targetQuery, event);
                    done();
                },
                VEHICLE_CREATED: function(state, event, funcs, done) {
                    const targetQuery = {
                        context: 'vehicle',
                        aggregate: 'titles-dashboard-vehicle-stream',
                        aggregateId: event.payload.vehicleId
                    }
                    funcs.emit(targetQuery, event);
                    done();
                }
            },
            query: {},
            partitionBy: 'stream',
            outputState: 'false'
        });

        eventstore.project({
            projectionId: 'titles-dashboard-vehicle-list',
            playbackInterface: {
                $init: function() {
                    return {
                        vehicles: []
                    }
                },
                SALES_CHANNEL_INSTANCE_VEHICLE_SOLD: function(state, event, funcs, done) {
                    state.vehicles.forEach((vehicle) => {
                        if (vehicle.vehicleId == event.payload.vehicleId) {
                            vehicle.sold_at = event.payload.sold_at
                        }
                    });
                    done();
                },
                VEHICLE_CREATED: function(state, event, funcs, done) {
                    vehicles.push({
                        vehicleId: event.payload.vehicleId,
                        year: event.payload.year,
                        make: event.payload.make,
                        model: event.payload.model
                    });

                    const targetQuery = {
                        context: 'vehicle',
                        aggregate: 'titles-dashboard-vehicle-stream',
                        aggregateId: event.payload.vehicleId
                    }
                    funcs.emit(targetQuery, event);
                    done();
                }
            },
            query: {
                context: 'vehicle',
                aggregate: 'titles-dashboard-vehicle-stream'
            },
            partitionBy: '',
            outputState: 'false'
        });

        eventstore.startAllProjections((err) => {
            if (err) {
                console.error('error in startAllProjections');
            } else {
                console.log('startAllProjections done');
            }
        })

        const event = await eventstore._getLastEventAsync('titles-dashboard-vehicle-list-result');

        const state = event.payload;

        const titlesDashboardVehicles = state.vehicles.filter((vehicle) => {
            if (vehicle.sold_at) {
                return true;
            } else {
                return false;
            }
        });

        titlesDashboardVehicles
    }
});

module.exports = eventstore;