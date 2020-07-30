/**
 * VehicleCreatedEvent
 * @typedef {Object} VehicleCreatedEvent
 * @property {string} aggregateId
 * @property {string} context
 * @property {string} aggregate
 * @property {Object} payload the eventstore payload
 * @property {Object} payload.name the name of the event
 * @property {Object} payload.payload the actual event payload
 * @property {string} payload.payload.vehicleId the vehicleId
 * @property {Number} payload.payload.year the year
 * @property {String} payload.payload.make the make
 * @property {String} payload.payload.model the model
 * @property {Number} payload.payload.mileage the mileage
 */

 /**
 * VehicleListState
 * @typedef {Object} VehicleListState
 * @property {Number} count
 */

 module.exports = {};