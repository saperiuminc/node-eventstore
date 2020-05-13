const Eventstore = require('../eventstore');
const _ = require('lodash');

/**
 * EventstoreProjectionConfig
 * @typedef {Object} EventstoreProjectionConfig
 */

/**
 * ProjectionParams
 * @typedef {Object} ProjectionParams
 * @property {String} projectionId - The unique projectionId of the projection
 * @property {AggregateQuery} query - The query to use to get the events for the projection
 * @property {Object} userData - Optional userData
 * @property {("instance"|"")} partitionBy - Partition the state by instance
 */


/**
 * AggregateQuery
 * @typedef {Object} AggregateQuery
 * @property {String} aggregate - The aggregate
 * @property {String} aggregateId - The aggregateId
 * @property {String} context - The context
 */


/**
 * @class EventstoreWithProjection
 */
function EventstoreWithProjection(options, store) {
    Eventstore.call(this, options, store);

    if (options && options.projectionConfig) {
        this.projectionName = options.projectionConfig.name;
    }
}

EventstoreWithProjection.prototype = Eventstore.prototype; // Set prototype to Person's
EventstoreWithProjection.prototype.constructor = EventstoreWithProjection;

/**
 * @param {ProjectionParams} projectionParams - parameters for the projection
 * @returns {Promise<void>} - returns a void promise
 */
EventstoreWithProjection.prototype.project = function(projectionParams) {

    const projectionId = projectionParams.projectionId;
    const query = projectionParams.query;
    const userData = projectionParams.userData;

    var partitionBy = projectionParams.partitionBy; // '' or 'instance' (default; '')
    var channel = getChannel(query);
    var self = this;

    // store list of projections to redis (to utilize for autoCommits, progress/counts, etc...)
    var jobId = `job:${projectionId}`;
    var key = `projections:${group}:${projectionId}:${channel}`;
    var resource = `locks:${key}`;
    var ttl = 5000;

    var queryProjection = {
        aggregateId: key,
        aggregate: 'projection',
        context: '__projections__'
    };

    this.getLastEventAsStream()
    console.log('projectionParams!', projectionParams);
};

module.exports = EventstoreWithProjection;