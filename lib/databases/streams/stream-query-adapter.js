/* eslint-disable valid-jsdoc */
const { Writable, Readable, Transform } = require('stream');
const _ = require('lodash');
const EventEmitter = require('events');
const JoinStreams = require('./join-streams');


/**
 * StreamsQueryAdapterOptions
 * @typedef {Object} StreamsQueryAdapterOptions
 * @property {ReadableStreams[]} readableStreams
 * @property {String[]} readableSortFieldNames
*/

/**
 * @type Sharding
 */
class StreamQueryAdapter {
    /**
     * @param {StreamsQueryAdapterOptions} options
     * @returns {void} Returns void
     */
     constructor(options) {
        /**
         * @type {StreamsQueryAdapterOptions}
         */
        this._options = options;
        if (!this._options.readableStreams || !Array.isArray(this._options.readableStreams)) {
            throw new Error('missing readableStreams');
        }
        if (!this._options.readableSortFieldNames || !Array.isArray(this._options.readableSortFieldNames)) {
            throw new Error('missing readableSortFieldNames');
        }

        this._joinedStreams = new JoinStreams({
          readableSortFieldNames: this._options.readableSortFieldNames
        });

        this._options.readableStreams.forEach((stream) => {
          this._joinedStreams.addToReadableStreams(stream);
        });
    }
    getJoinedReadableStreams() {
      return this._joinedStreams.getJoinedReadableStreams();
    }
    async getRowsFromReadableAsync(limit, transformRow) {
      const self = this;
      return new Promise((resolve, reject) => {
        const readableStream = self._joinedStreams.getJoinedReadableStreams();
        let dataCount = 1;
        let dataRows = [];
        readableStream.on('data', (data) => {
          if (transformRow) {
            const transformedData = transformRow(data);
            dataRows.push(transformedData);
          } else {
            dataRows.push(data);
          }
          dataCount++;
          if (limit && dataCount === limit) {
            readableStream.push(null);
          }
        });
        readableStream.on('end', () => {
          resolve(dataRows);
        });
        readableStream.on('error', (err) => {
          reject(err);
        });
      })
    }
}

module.exports = StreamQueryAdapter;
