const Bluebird = require('bluebird');
const bluebird = require('bluebird');

module.exports = (function() {
    const eventstore = require('@saperiuminc/eventstore')({
        type: 'mysql',
        host: process.env.EVENTSTORE_MYSQL_HOST,
        port: process.env.EVENTSTORE_MYSQL_PORT,
        user: process.env.EVENTSTORE_MYSQL_USERNAME,
        password: process.env.EVENTSTORE_MYSQL_PASSWORD,
        database: process.env.EVENTSTORE_MYSQL_DATABASE,
        connectionPoolLimit: 10,
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
            database: process.env.EVENTSTORE_MYSQL_DATABASE,
            connectionPoolLimit: 1
        }, // required
        projectionStore: {
            host: process.env.EVENTSTORE_MYSQL_HOST,
            port: process.env.EVENTSTORE_MYSQL_PORT,
            user: process.env.EVENTSTORE_MYSQL_USERNAME,
            password: process.env.EVENTSTORE_MYSQL_PASSWORD,
            database: process.env.EVENTSTORE_MYSQL_DATABASE,
            connectionPoolLimit: 1,
            name: 'projections'
        }, // required
        enableProjection: true,
        eventCallbackTimeout: 1000,
        pollingTimeout: 10000, // optional,
        pollingMaxRevisions: 100,
        errorMaxRetryCount: 2,
        errorRetryExponent: 2,
        playbackEventJobCount: 10,
        context: 'vehicle'
    });

    bluebird.promisifyAll(eventstore);

    const initialize = async function() {
        try {
            await eventstore.initAsync();

            console.log('eventstore initialized');

            // add dummy data
            for (let index = 0; index < 100; index++) {
                const vehicleId = `vehicle_${index}`;
                const stream = await eventstore.getLastEventAsStreamAsync({
                    context: 'vehicle',
                    aggregate: 'vehicle',
                    aggregateId: vehicleId
                });

                Bluebird.promisifyAll(stream);

                const event = {
                    name: "VEHICLE_CREATED",
                    payload: {
                        vehicleId: vehicleId,
                        year: 2012,
                        make: "Honda",
                        model: "Jazz",
                        mileage: 1245
                    }
                }
                stream.addEvent(event);
                await stream.commitAsync();
            }


            // some dummy calls for testing
            // eventstore.subscribe('dummy-projection-id-1-result', 0, (err, event, callback) => {
            //     console.log('dummy onEventCallback received event', event);
            //     callback();
            // }, (error) => {
            //     console.error('onErrorCallback received error', error);
            // });

            // eventstore.subscribe('vehicle_6', 0, (err, event, callback) => {
            //     console.log('vehicle_6 onEventCallback event', event);
            //     callback();
            // }, (error) => {
            //     console.error('onErrorCallback received error', error);
            // });

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
                        // return done(new Error('this is an error'));
                        funcs.getPlaybackList('vehicle_list', function(err, playbackList) {
                            const eventPayload = event.payload.payload;
                            const data = {
                                vehicleId: eventPayload.vehicleId,
                                year: eventPayload.year,
                                make: eventPayload.make,
                                model: eventPayload.model,
                                mileage: eventPayload.mileage
                            };
                            console.log('adding', event.aggregateId);
                            playbackList.add(event.aggregateId, event.streamRevision, data, {}, function(err) {
                                state.count++;
                                done();
                            })
                        });
                    },
                    VEHICLE_MILEAGE_CHANGED: function(state, event, funcs, done) {
                        funcs.getPlaybackList('vehicle_list', function(err, playbackList) {
                            const eventPayload = event.payload.payload;
                            const data = {
                                mileage: eventPayload.mileage
                            };

                            playbackList.get(event.aggregateId, function(err, result) {
                                if (result) {
                                    const oldData = result.data;
                                    const newData = Object.assign(oldData, {
                                        mileage: eventPayload.mileage
                                    });

                                    playbackList.update(event.aggregateId, event.streamRevision, oldData, newData, {}, function(err) {
                                        done();
                                    })
                                } else {
                                    done();
                                }
                            });
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
                    }, {
                        name: 'spread',
                        type: 'decimal'
                    }],
                    secondaryKeys: {
                        idx_vehicleId: [{
                            name: 'vehicleId',
                            sort: 'ASC'
                        }]
                    }
                }
            });

            await eventstore.projectAsync({
                projectionId: 'vehicle-state-list',
                playbackInterface: {
                    $init: function() {
                        return {}
                    },
                    /**
                     * @param {import('./types').VehicleListState} state the name of the playback list
                     * @param {import('./types').VehicleCreatedEvent} event the name of the playback list
                     * @param {Object} funcs the last event that built this projection state
                     * @param {Function} done the last event that built this projection state
                     * @returns {void} Returns void. Use the callback to the get playbacklist
                     */
                    VEHICLE_CREATED: async function(state, event, funcs) {
                        const eventPayload = event.payload.payload;
                        const stateList = await funcs.getStateList('vehicles_state_list');

                        const data = {
                            vehicleId: eventPayload.vehicleId,
                            year: eventPayload.year,
                            make: eventPayload.make,
                            model: eventPayload.model,
                            mileage: eventPayload.mileage
                        };

                        await stateList.push(data);
                    },
                    VEHICLE_MILEAGE_CHANGED: async function(state, event, funcs) {
                        const eventPayload = event.payload.payload;
                        const stateList = await funcs.getStateList('vehicles_state_list');

                        const filters = [{
                            field: 'vehicleId',
                            operator: 'is',
                            value: eventPayload.vehicleId
                        }];

                        const row = await stateList.find(filters);

                        if (row) {
                            const rowIndex = row.index;
                            const rowValue = row.value;
                            const rowMeta = row.meta;

                            rowValue.mileage = eventPayload.mileage;

                            await stateList.set(rowIndex, rowValue, rowMeta);
                        }
                    }
                },
                query: {
                    context: 'vehicle',
                    aggregate: 'vehicle'
                },
                partitionBy: '',
                outputState: 'true',
                stateList: {
                    name: 'vehicles_state_list',
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
                `SELECT * FROM vehicle_list v;`);

            await eventstore.startAllProjectionsAsync();

            eventstore.subscribe({
                context: 'vehicle',
                aggregate: 'vehicle',
                aggregateId: 'vehicle_1'
            }, 0, function(err, event, done) {

                console.log('got event from subscribe', event.aggregateId, event.streamRevision);
                done();
            });
        } catch (error) {
            console.error('error in setting up the projection', error);
        }
    }

    initialize();

    return eventstore;
})();