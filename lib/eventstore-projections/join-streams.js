/* eslint-disable valid-jsdoc */
const { Writable, Readable, Transform } = require('stream');
const _ = require('lodash');
const EventEmitter = require('events');


/**
 * JoinStreamsOptions
 * @typedef {Object} JoinStreamsOptions
 * @property {Stream[]} readableStreams
 * @property {Stream[]} writableStreams
 * @property {?Function[]} writableWrites accepts writeId, row, and callback to run for a writable stream write. Callback must be called
 * @property {?Function[]} writableFinals accepts callback to run for a writable stream final. Callback must be called
*/

/**
 * @type Sharding
 */
class JoinStreams extends EventEmitter {
    /**
     * @param {JoinStreamsOptions} options
     * @returns {void} Returns void
     */
     constructor(options) {
        super();
        /**
         * @type {JoinStreamsOptions}
         */
        this._options = options;

        if (!this._options.readableStreams || !Array.isArray(this._options.readableStreams)) {
            throw new Error('missing readableStreams');
        }
        if (!this._options.writableStreams || !Array.isArray(this._options.writableStreams)) {
            throw new Error('missing readableStreams');
        }

        this._writeInProgress = {};

        this._readableStreams = this._options.readableStreams;
        this._writableStreams = this._options.writableStreams;

        this._messengerReadStream = null;
        this._messengerWriteStream = null;
    }
    /**
     * @param {String} writeId
     * @param {import('./types').ConnectionConfig} connectionConfig
     * @param {Object} row
     * @param {Function} callback
     * @returns {void}
     */
     _writableWrite(writeId, row, callback) {
        if (this._options.writableWrites && this._options.writableWrites[writeId]) {
            this._options.writableWrites[writeId](writeId, row, callback);
        } else {
            throw new Error('_writableWrite must be implemented in subclass');
        }
    }
    /**
     * @param {String} writeId
     * @param {Function} callback
     * @returns {void}
     */
     _writableFinal(writeId, callback) {
        if (this._options.writableFinals && this._options.writableFinals[writeId]) {
            this._options.writableFinals[writeId](writeId, callback);
        } else {
          callback();
        }
    }
    /**
     * @param {Object} row
     * @returns {void}
     */
    _getWritablePartition(row) {
      if (this._options.getWritablePartition) {
        this._options.getWritablePartition(row);
      } else {
        return 'unpartitioned';
      }
    }
    /**
     * @returns {Stream} writableStream
     */
    _createWritableStream(writeId) {
        const self = this;
        this._writeInProgress[writeId] = true;
        const writableStream = new Writable({
            objectMode: true,
            writeev: (row, callback) => {
                self._writableWrite(writeId, row, callback);
                self.emit('write');
            },
            write: (row, encoding, callback) => {
                self._writableWrite(writeId, row, callback);
                self.emit('write');
            },
            final: (callback) => {
                self._writableFinal(writeId, () => {
                    delete self._writeInProgress[writeId];
                    if (_.isEmpty(self._writeInProgress)) {
                        self.emit('finishWrite');
                    }
                    callback();
                });
            }
        });

        return writableStream;
    }
    addToWritableStreams(writeId) {
      this._writableStreams.push(this._createWritableStream(writeId));
    }
    /**
     * @returns {Stream} readableStream
     */
     getJoinedReadableStreams() {
        const self = this;
        if (self._messengerReadStream) {
            return self._messengerReadStream;
        }
        const currentProcessingRows = {};
        const readableInProgress = {};
        const unpausedCounts = {};

        const hasUnpaused = () => {
            let oneUnpaused = false;
            for (const readIndex in unpausedCounts) {
                if (Object.hasOwnProperty.call(unpausedCounts, readIndex)) {
                    const count = unpausedCounts[readIndex];
                    if (count) {
                        oneUnpaused = true;
                        break;
                    }
                }
            }
            return oneUnpaused;
        };

        self._messengerReadStream = new Readable({
            objectMode: true
        });
        const sortFields = self._options.readableSortFieldNames;
        const sweepFunc = () => {
            // check first if all streams are paused or ended
            if (_.isEmpty(readableInProgress) && _.isEmpty(currentProcessingRows)) {
                self._messengerReadStream.push(null);
            } else if (!hasUnpaused()) {
                // currentProcessingRows should be filled up by now. Determine which should be pushed first based on timestamp
                let earliestRow = null;
                let readIdWithEarliest = null;
                for (const readIndex in currentProcessingRows) {
                    if (Object.hasOwnProperty.call(currentProcessingRows, readIndex)) {
                        const row = currentProcessingRows[readIndex];
                        if (!earliestRow) {
                            earliestRow = row;
                            readIdWithEarliest = readIndex;
                        } else {
                            let sortFieldCount = 0;
                            while (sortFieldCount < sortFields.length) {
                                const sortFieldName = sortFields[sortFieldCount];
                                if (earliestRow[sortFieldName] !== row[sortFieldName]) {
                                    if (earliestRow[sortFieldName] > row[sortFieldName]) {
                                        earliestRow = row;
                                        readIdWithEarliest = readIndex;
                                    }
                                    break;
                                }
                                sortFieldCount++;
                            }
                        }
                    }
                }
                const rowToPush = _.clone(earliestRow);

                delete currentProcessingRows[readIdWithEarliest];
                if (readableInProgress[readIdWithEarliest]) {
                    self._readableStreams[readIdWithEarliest].resume();
                    unpausedCounts[readIdWithEarliest]++;
                }
                self._messengerReadStream.push(rowToPush);
            }
        };
        _.forEach(self._readableStreams, (stream, index) => {
            // read one row for all shards and populate currentProcessingRows values
            stream.pipe(new Transform({
                objectMode: true,
                transform(row, encoding, callback) {
                    if (!currentProcessingRows[index] && row) {
                        currentProcessingRows[index] = row;
                    } else {
                        console.error('stream has unpaused unnecessarily');
                    }
                    stream.pause();
                    callback();
                }
            }));
            unpausedCounts[index] = 1;
            readableInProgress[index] = true;

            stream.on('pause', () => {
                unpausedCounts[index]--;
                if (!hasUnpaused()) {
                    sweepFunc();
                }
            });
            stream.on('end', () => {
                delete readableInProgress[index];
                delete unpausedCounts[index];
                sweepFunc();
            });
        });

        self._messengerReadStream._read = (size) => {
            sweepFunc();
        };

        self._messengerReadStream.on('error', (err) => {
            console.error(err);
        });

        return self._messengerReadStream;
    }
    /**
     * @returns {Stream} readableStream
     */
    getJoinedWritableStreams() {
        const self = this;
        if (self._messengerWriteStream) {
            return self._messengerWriteStream;
        }
        const messengerWrite = (row, callback) => {
          const partition = self._getWritablePartition(row);
          if (partition === 'unpartitioned') {
            // write to all
            _.forEach(self._writableStreams, (stream) => {
                stream.write(row);
            });
          } else {
            // write to certain parition
            self._writableStreams[partition].write(row);
          }
          callback();
        };
        self._messengerWriteStream = new Writable({
            objectMode: true,
            writev: (row, callback) => {
                messengerWrite(row, callback);
            },
            write: (row, encoding, callback) => {
                messengerWrite(row, callback);
            },
            final: (callback) => {
                _.forEach(self._writableStreams, (stream) => {
                    stream.end();
                });
                callback();
            }
        });
        self._messengerWriteStream.on('error', (err) => {
            console.error(err);
        });
        return self._messengerWriteStream;
    }
    // pauseReadableStream() {
    //   const readableStream = this.getJoinedReadableStreams();
    //   readableStream.pause();
    // }
    // endReadableStream() {
    //   const readableStream = this.getJoinedReadableStreams();
    //   readableStream.push(null);
    // }
    // destroyReadableStream(error) {
    //   const readableStream = this.getJoinedReadableStreams();
    //   readableStream.destroy(error);
    // }
    // destroyWritableStream(error) {
    //   const writableStream = this.getJoinedWritableStreams();
    //   writableStream.destroy(error);
    // }
}

module.exports = JoinStreams;
