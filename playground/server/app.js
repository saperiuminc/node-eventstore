module.exports = async function() {
    var createError = require('http-errors');
    var express = require('express');
    var path = require('path');
    var cookieParser = require('cookie-parser');
    var logger = require('morgan');

    var indexRouter = require('./routes/index');
    var usersRouter = require('./routes/users');

    var app = express();

    // view engine setup
    app.set('views', path.join(__dirname, 'views'));
    app.set('view engine', 'jade');

    app.use(logger('dev'));
    app.use(express.json());
    app.use(express.urlencoded({
        extended: false
    }));
    app.use(cookieParser());
    app.use(express.static(path.join(__dirname, 'public')));

    app.use('/', indexRouter);
    app.use('/users', usersRouter);

    // catch 404 and forward to error handler
    app.use(function(req, res, next) {
        next(createError(404));
    });

    // error handler
    app.use(function(err, req, res, next) {
        // set locals, only providing error in development
        res.locals.message = err.message;
        res.locals.error = req.app.get('env') === 'development' ? err : {};

        // render the error page
        res.status(err.status || 500);
        res.render('error');
    });

    const EventstoreService = require('./services/eventstore-service/evenstore-service');
    const eventstoreService = new EventstoreService();
    await eventstoreService.init();

    const Bluebird = require('bluebird');
    const nanoid = require('nanoid').nanoid;

    const eventstore = eventstoreService.getEventstore();

    // create the projections
    const mappingProjectionConfig = {
        projectionId: 'vehicles-list-mapping',
        projectionName: 'Vehicles Mapping',
        playbackInterface: {
            vehicle_created: async function(state, event, funcs) {
                const eventPayload = event.payload.payload;

                await funcs.emit({
                    context: 'vehicle',
                    aggregate: 'vehicleListItem',
                    aggregateId: event.aggregateId
                }, {
                    name: 'vehicle_list_item_created',
                    payload: eventPayload
                });

                await funcs.emit({
                    context: 'vehicle',
                    aggregate: 'inspectorVehicleListItem',
                    aggregateId: eventPayload.inspectedByUserId
                }, {
                    name: 'inspector_vehicle_list_item_created',
                    payload: eventPayload
                });
            }
        },
        query: {
            context: 'vehicle',
            aggregate: 'vehicle'
        }
    };

    const vehiclesListProjectionConfig = {
        projectionId: 'vehicles-list',
        projectionName: 'Vehicles Listing',
        playbackInterface: {
            vehicle_list_item_created: async function(state, event, funcs) {

                const playbackList = await funcs.getPlaybackList('vehicle_list');
                const eventPayload = event.payload.payload;
                const data = eventPayload;
                await playbackList.add(event.aggregateId, event.streamRevision, data, {});
            }
        },
        query: {
            context: 'vehicle',
            aggregate: 'vehicleListItem'
        },
        playbackList: {
            name: 'vehicle_list',
            fields: [{
                name: 'vehicleId',
                type: 'string'
            }, {
              name: 'inspectedByUserId',
              type: 'string'
          }],
          secondaryKeys: {
            idx_inspectedByUserId: [{
              name: 'inspectedByUserId',
              sort: 'ASC'
            }],
            idx_vehicleId: [{
              name: 'vehicleId',
              sort: 'ASC'
            }]
          }
        }
    };

    const inspectorVehiclesListProjectionConfig = {
        projectionId: 'inspector-vehicles-list',
        projectionName: 'Inspector Vehicles Listing',
        playbackInterface: {
            inspector_vehicle_list_item_created: async function(state, event, funcs) {

                const playbackList = await funcs.getPlaybackList('inspectors_list');

                const oldData = await playbackList.get(event.aggregateId);

                if (oldData) {
                    const data = oldData && oldData.data ? oldData.data : {};
                    const newData = {
                        count: data.count ? data.count + 1 : 1
                    };

                    await playbackList.update(event.aggregateId, event.streamRevision, data, newData, {});
                } else {
                    const newData = {
                        count: 1
                    };
                    await playbackList.add(event.aggregateId, event.streamRevision, newData, {});
                }
            }
        },
        query: {
            context: 'vehicle',
            aggregate: 'inspectorVehicleListItem'
        },
        playbackList: {
            name: 'inspectors_list',
            fields: [{
                name: 'inspectedByUserId',
                type: 'string'
            }]
        }
    };

    // run the projection
    await eventstore.projectAsync(mappingProjectionConfig);
    await eventstore.runProjectionAsync(mappingProjectionConfig.projectionId, false);
    await eventstore.projectAsync(vehiclesListProjectionConfig);
    await eventstore.runProjectionAsync(vehiclesListProjectionConfig.projectionId, false);
    await eventstore.projectAsync(inspectorVehiclesListProjectionConfig);
    await eventstore.runProjectionAsync(inspectorVehiclesListProjectionConfig.projectionId, false);
    await eventstore.startAllProjectionsAsync();

    const inspectors = [{
        inspectedByUserId: 'inspector_1',
        inspectedByName: 'Juan Gomez'
    }, {
        inspectedByUserId: 'inspector_2',
        inspectedByName: 'Miguel Marquez'
    }, {
        inspectedByUserId: 'inspector_3',
        inspectedByName: 'Joncho Villanueva'
    }];

    const makes = ['Honda', 'Toyota', 'Mitsubishi', 'Chevrolet'];
    const models = ['Jazz', 'Fortuner', 'Lancer', 'Cruze'];
    const vehiclesCount = Math.floor(Math.random() * 10) + 1;

    const vehicleIds = [];
    for (let i = 0; i < vehiclesCount; i++) {
        const vehicleId = nanoid();
        vehicleIds.push(vehicleId);
        const stream = await eventstore.getLastEventAsStreamAsync({
            context: 'vehicle',
            aggregate: 'vehicle',
            aggregateId: vehicleId
        });

        Bluebird.promisifyAll(stream);

        const inspector = inspectors[Math.floor(Math.random() * inspectors.length)];

        const event = {
            name: "vehicle_created",
            payload: {
                vehicleId: vehicleId,
                year: Math.floor(Math.random() * 10) + 2012,
                make: makes[Math.floor(Math.random() * makes.length)],
                model: models[Math.floor(Math.random() * models.length)],
                mileage: Math.floor(Math.random() * 10000) + 50000,
                inspectedByUserId: inspector.inspectedByUserId,
                inspectedByName: inspector.inspectedByName
            }
        }

        // add event
        stream.addEvent(event);
        await stream.commitAsync();
    }

    // subscribe to all inspector streams
    for (const inspector of inspectors) {
      eventstore.subscribe({ context: 'vehicle', aggregate: 'inspectorVehicleListItem', aggregateId: inspector.inspectedByUserId}, 0, function(error, event, done) {
        console.log('got subscription message', event.aggregateId, event.payload.payload.year, event.payload.payload.make, event.payload.payload.model);
        done();
      })
    }
    

    return app;
};