const Eventstore = require('../eventstore');
const _ = require('lodash');
const util = require('util');

/**
 * EventstoreProjectionConfig
 * @typedef {Object} EventstoreProjectionConfig
 */

/**
 * ProjectionParams
 * @typedef {Object} ProjectionParams
 * @property {String} projectionId The unique projectionId of the projection
 * @property {AggregateQuery} query The query to use to get the events for the projection
 * @property {Object} userData Optional userData
 * @property {("instance"|"")} partitionBy Partition the state by instance
 */


/**
 * AggregateQuery
 * @typedef {Object} AggregateQuery
 * @property {String} aggregate The aggregate
 * @property {String} aggregateId The aggregateId
 * @property {String} context The context
 * @property {String} streamId The streamId
 */


/**
 * @class Eventstore
 * @returns {Eventstore}
 */
function EventstoreWithProjection(options, store) {

    if (options && options.projectionConfig) {
        this.projectionName = options.projectionConfig.name;
    }

    Eventstore.call(this, options, store);
}

util.inherits(EventstoreWithProjection, Eventstore);


EventstoreWithProjection.prototype = Eventstore.prototype;
EventstoreWithProjection.prototype.constructor = EventstoreWithProjection;

/**
 * @param {ProjectionParams} projectionParams parameters for the projection
 * @param {ProjectionParams} callback success/error callback
 * @returns {void} - returns void
 */
EventstoreWithProjection.prototype.project = function(projectionParams, callback) {
    try {
        // just preserving that adrai's public functions are callback based but internally we can just use async/await for elegance
        this._project(projectionParams).then(callback).catch(function(error) {
            if (callback) {
                callback(error);
            }
        });
    } catch (error) {
        if (callback) {
            callback(error);
        }
    }
};

EventstoreWithProjection.prototype._project = async function(projectionParams) {
    try {
        if (!projectionParams) {
            throw new Error('projectionParams is required');
        }

        if (!projectionParams.projectionId) {
            throw new Error('projectionId is required');
        }

        if (!projectionParams.query) {
            throw new Error('query is required');
        }
        const projectionId = projectionParams.projectionId;
        const channel = this._getChannel(projectionParams.query);
        const key = `projections:${this.projectionName}:${projectionId}:${channel}`;

        var queryProjection = {
            aggregateId: key,
            aggregate: 'projection',
            context: '__projections__'
        };

        const stream = await this._getLastEventAsStream(queryProjection);
    } catch (error) {
        console.error(error);
        console.error('error in Eventstore._project');
        throw error;
    }
};

/**
 * @param {AggregateQuery} query - aggregate query
 * @returns {Promise<stream>} - returns a stream as promise
 */
EventstoreWithProjection.prototype._getLastEventAsStream = function(query) {
    return new Promise((resolve, reject) => {
        try {
            this.getLastEventAsStream(query, function(err, stream) {
                if (err) {
                    reject(err);
                } else {
                    resolve(stream);
                }
            });
        } catch (error) {
            reject(error);
        }
    });
}

EventstoreWithProjection.prototype._getChannel = function(query) {
    const channel = (typeof(query) === 'string' ?
        `::${query}` :
        `${query.context || ''}:${query.aggregate || ''}:${query.aggregateId || query.streamId || ''}`);
    return channel;
}

module.exports = EventstoreWithProjection;