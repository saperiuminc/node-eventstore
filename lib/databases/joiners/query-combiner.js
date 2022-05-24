/* eslint-disable valid-jsdoc */
const _ = require('lodash');
const EventEmitter = require('events');


/**
 * QueryCombinerOptions
 * @typedef {Object} QueryCombinerOptions
 * @property {String[]} readableSortFieldNames
*/

/*
 * @type QueryCombiner
 */
class QueryCombiner extends EventEmitter {
    /**
     * @param {QueryCombinerOptions} options
     * @returns {void} Returns void
     */
     constructor(options) {
        super();
        /**
         * @type {QueryCombinerOptions}
         */
        this._options = options;

        if (!this._options.readableSortFieldNames || !Array.isArray(this._options.readableSortFieldNames)) {
            throw new Error('missing readableSortFieldNames');
        }

        this._rowResults = [];
    }
    /**
     * @param {Object[]} rowResults
     * @returns {void} 
    */
    addRowResults(queryResult) {
        this._rowResults.push(queryResult);
    }
    /**
    * @returns {void}  
    */
    clearRowResults() {
        this._rowResults = [];
    }
    /**
   * @returns {Object[]} finalRow resultSet
   */
    getJoinedQueryRows(limit, transformRow) {
      const self = this;
      let currentProcessingRows = {};
      const nextReadIndices = {};
      const finalRows = [];

      const sortFields = self._options.readableSortFieldNames;
      _.forEach(self._rowResults, (resultSet, index) => {
          nextReadIndices[index] = 0;
          const row = resultSet[nextReadIndices[index]];
          if (row) {
            currentProcessingRows[index] = row;
          }
          nextReadIndices[index]++;
      });
      const sweepFunc = () => {
        let earliestRow = null;
        let shardIdWithEarliest = null;
        for (const readIndex in currentProcessingRows) {
            if (Object.hasOwnProperty.call(currentProcessingRows, readIndex)) {
                const row = currentProcessingRows[readIndex];
                if (!earliestRow) {
                    earliestRow = row;
                    shardIdWithEarliest = readIndex;
                } else {
                    let sortFieldCount = 0;
                    while (sortFieldCount < sortFields.length) {
                    const sortFieldName = sortFields[sortFieldCount].fieldName;
                    const isSortFieldAsc = sortFields[sortFieldCount].sort === 'asc';
                        if (earliestRow[sortFieldName] !== row[sortFieldName]) {
                            if ((isSortFieldAsc && earliestRow[sortFieldName] > row[sortFieldName]) ||
                                (!isSortFieldAsc && earliestRow[sortFieldName] < row[sortFieldName])) {
                                earliestRow = row;
                                shardIdWithEarliest = readIndex;
                            }
                            break;
                        }
                        sortFieldCount++;
                    }
                }
            }
        }
        let rowToPush = earliestRow;
        if (transformRow) {
          rowToPush = _.clone(earliestRow);
          rowToPush = transformRow(rowToPush);
        }

        delete currentProcessingRows[shardIdWithEarliest];
        if (nextReadIndices[shardIdWithEarliest] < self._rowResults[shardIdWithEarliest].length) {
            currentProcessingRows[shardIdWithEarliest] = self._rowResults[shardIdWithEarliest][nextReadIndices[shardIdWithEarliest]];
            nextReadIndices[shardIdWithEarliest]++;
        }
        
        finalRows.push(rowToPush);

        // Remove limit
        // if (limit && finalRows.length === (Math.round((limit * shardNum) * 0.75))) {
        //   currentProcessingRows = {};
        // }
      };

      while (!_.isEmpty(currentProcessingRows)) {
        sweepFunc();
      }

      return finalRows;
    }
}

module.exports = QueryCombiner;
