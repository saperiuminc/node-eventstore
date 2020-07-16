const _projectAsync = function(eventstore, projection) {
    return new Promise((resolve, reject) => {
        try {
            eventstore.project(projection, (error, result) => {
                if (error) {
                    reject(error);
                } else {
                    resolve(result);
                }
            })
        } catch (error) {
            reject(error);
        }
    })
}

const _registerPlaybackListViewAsync = function(eventstore, listname, query) {
    return new Promise((resolve, reject) => {
        try {
            eventstore.registerPlaybackListView(listname, query, (error, result) => {
                if (error) {
                    reject(error);
                } else {
                    resolve(result);
                }
            })
        } catch (error) {
            reject(error);
        }
    })
}

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
    playbackListStore: {
        host: process.env.EVENTSTORE_MYSQL_HOST,
        port: process.env.EVENTSTORE_MYSQL_PORT,
        user: process.env.EVENTSTORE_MYSQL_USERNAME,
        password: process.env.EVENTSTORE_MYSQL_PASSWORD,
        database: process.env.EVENTSTORE_MYSQL_DATABASE
    },
    pollingTimeout: 10000
});

eventstore.init(async function(err) {
    if (err) {
        console.error(err);
        console.error('error in init');
    } else {
        console.log('es initialized');

        // some dummy calls for testing
        eventstore.subscribe('dummy-projection-id-1-result', 0, (err, event) => {
            console.log('received event', event);
        });

        
        
        // neeed to await the project call to initalize the playback list
        await _projectAsync(eventstore, {
            projectionId: 'vehicle-list',
            playbackInterface: {
                $init: function() {
                    return {
                        count: 0
                    }
                },
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
                fields: [
                    {
                        name: 'vehicleId',
                        type: 'string'
                    }
                ],
                secondaryKeys: {
                    idx_vehicleId: [{
                        name: 'vehicleId',
                        sort: 'ASC'
                    }]
                }
            }
        });

        await _registerPlaybackListViewAsync(eventstore, 'vehicle_list_view', 
        `
                SELECT
                    *
                FROM vehicle_list v;
        `)


        eventstore.startAllProjections((err) => {
            if (err) {
                console.error('error in startAllProjections');
            } else {
                console.log('startAllProjections done');
            }
        })
        
    }
});

module.exports = eventstore;