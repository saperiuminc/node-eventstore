/**
 * @typedef {("CREATE"|"UPDATE"|"DELETE")} EventStoreStateListRowType
 **/

/**
 * EventstoreStateListDoneCallback
 * @callback EventstoreStateListDoneCallback
 * @param {Error} error The error if any
 * @param {Object} result Result of this callback
 * @returns {void} Returns void
 */

/**
 * EventstoreStateListFilters
 * @typedef {Object} EventstoreStateListFilters
 */

/**
 * EventstoreStateListSort
 * @typedef {Object} EventstoreStateListSort
 */

/**
 * EventstoreStateListSecondaryKey
 * @typedef {Object} EventstoreStateListSecondaryKey
 * @property {String} name The name of the field
 * @property {("ASC"|"DESC")} sort The sort directioon of the key. Default is ASC
 */

/**
 * EventstoreStateListField
 * @typedef {Object} EventstoreStateListField
 * @property {String} type The type of the field
 * @property {String} name The field name
 */

/**
 * EventstoreStateListData
 * @typedef {Object} EventstoreStateListData
 * @property {String} rowIndex The last rowIndex
 * @property {String} lastId The last id
 */

/**
 * EventstoreStateListOptions
 * @typedef {Object} EventstoreStateListOptions
 * @property {String} listName the name of this list
 */


/**
 * @param {EventstorePlaybackListOptions} options additional options for the Eventstore playback list
 * @constructor
 */
function EventstoreStateList(store, listName, state) {
    this._store = store;
    this._state = state;
    this._listName = listName;
}


/**
 * @type {EventstoreStateListStore}
 */
EventstoreStateList.prototype._store;

/**
 * @type {EventstoreStateListState}
 */
EventstoreStateList.prototype._state;

/**
 * @type {String}
 */
EventstoreStateList.prototype._listName;


/**
 * @param {Number} index the index of the row to delete
 * @param {EventstoreStateListDoneCallback} cb the callback to call after the operation
 * @returns {void} - returns void. use the cb to get results
 */
EventstoreStateList.prototype.delete = function(index, cb) {
    if (cb) {
        this._delete(index).then(cb).catch(cb);
    } else {
        return this._delete(index);
    }
};


/**
 * @param {Number} index the index of the row to delete
 * @param {EventstoreStateListDoneCallback} cb the callback to call after the operation
 * @returns {void} - returns void. use the cb to get results
 */
EventstoreStateList.prototype._delete = async function(index) {
    try {
        // push to end
        const revision = await this._store.delete(this._listName, index);
        this._setRevision(revision);
    } catch (error) {
        console.error('error in statelist.set', error);
        throw error;
    } 
};

/**
 * @param {Number} index the index of the row to delete
 * @param {Object} state the new state of the item to update
 * @param {EventstoreStateListDoneCallback} cb the callback to call after the operation
 * @returns {void} - returns void. use the cb to get results
 */
EventstoreStateList.prototype.set = function(index, state, cb) {
    if (cb) {
        // push on end
        this._set(index, state).then(cb).catch(cb);
    } else {
        return this._set(index, state);
    }
};

/**
 * @param {Number} index the index of the row to delete
 * @param {Object} state the new state of the item to update
 * @param {EventstoreStateListDoneCallback} cb the callback to call after the operation
 * @returns {void} - returns void. use the cb to get results
 */
EventstoreStateList.prototype._set = async function(index, state) {
    try {
        // push on end
        const revision = await this._store.set(this._listName, index, state);
        this._setRevision(revision);
    } catch (error) {
        console.error('error in statelist.set', error);
        throw error;
    }
};

/**
 * @param {Object} state the state of the item to add
 * @param {EventstoreStateListDoneCallback} cb the callback to call after the operation
 * @returns {void} - returns void. use the cb to get results
 */
EventstoreStateList.prototype.push = function(state, cb) {
    if (cb) {
        // push on end
        this._push(state).then(cb).catch(cb);
    } else {
        return this._push(state);
    }
};


/**
 * @param {Object} state the state of the item to add
 * @param {EventstoreStateListDoneCallback} cb the callback to call after the operation
 * @returns {void} - returns void. use the cb to get results
 */
EventstoreStateList.prototype._push = async function(state) {
    try {
        // push on end
        const revision = await this._store.push(this._listName, state);
        this._setRevision(revision);
    } catch (error) {
        console.error('error in statelist.push', error);
        throw error;
    }
};

/**
 * @param {Number} startRowIndex the start index to get the list
 * @param {Number} limit the number of items to get
 * @param {Object} filters not yet implemented
 * @param {Object} sorting not yet implemented
 * @param {EventstoreStateListDoneCallback} cb the callback to call after the operation
 * @returns {void} - returns void. use the cb to get results
 */
EventstoreStateList.prototype.find = function(filters, cb) {
    if (cb) {
        // push on end
        this._find(this._getRevision(), filters).then((row) => {
            cb(null, row)
        }).catch(cb);
    } else {
        return this._find(this._getRevision(), filters);
    }
};


/**
 * @param {Number} startRowIndex the start index to get the list
 * @param {Number} limit the number of items to get
 * @param {Object} filters not yet implemented
 * @param {Object} sorting not yet implemented
 * @param {EventstoreStateListDoneCallback} cb the callback to call after the operation
 * @returns {void} - returns void. use the cb to get results
 */
EventstoreStateList.prototype._find = async function(revision, filters) {
    try {
        // push on end
        // row has state and rowIndex
        const row = await this._store.find(this._listName, revision, filters);
        return row;
    } catch (error) {
        console.error('error in statelist.find', error);
        throw error;
    }
};

/**
 * @param {Number} startRowIndex the start index to get the list
 * @param {Number} limit the number of items to get
 * @param {Object} filters not yet implemented
 * @param {Object} sorting not yet implemented
 * @param {EventstoreStateListDoneCallback} cb the callback to call after the operation
 * @returns {void} - returns void. use the cb to get results
 */
EventstoreStateList.prototype.filter = function(filters, cb) {
    if (cb) {
        // push on end
        this._filter(this._getRevision(), filters).then((rows) => {
            cb(null, rows)
        }).catch(cb);
    } else {
        return this._filter(this._getRevision(), filters);
    }
};


/**
 * @param {Number} startRowIndex the start index to get the list
 * @param {Number} limit the number of items to get
 * @param {Object} filters not yet implemented
 * @param {Object} sorting not yet implemented
 * @param {EventstoreStateListDoneCallback} cb the callback to call after the operation
 * @returns {void} - returns void. use the cb to get results
 */
EventstoreStateList.prototype._filter = async function(revision, filters) {
    try {
        // rows is an array of state and rowInex. example [{ state: 'state', rowIndex: 1 }]
        const rows = await this._store._filter(this._listName, revision, filters);
        return rows;
    } catch (error) {
        console.error('error in statelist.filter', error);
        throw error;
    }
};

/**
 * @param {EventstoreStateListDoneCallback} cb callback to be called when the query is done retrieving data
 * @returns {void} - returns void. use the callback for the result (cb)
 */
EventstoreStateList.prototype._setRevision = function(revision) {
    if (this._state) {
        this._state[this._listName] = revision;
    }
};


/**
 * @param {EventstoreStateListDoneCallback} cb callback to be called when the query is done retrieving data
 * @returns {void} - returns void. use the callback for the result (cb)
 */
EventstoreStateList.prototype._getRevision = function() {
    if (this._state && this._state[this._listName] > 0) {
        return this._state[this._listName];
    }

    return 0;
};

module.exports = EventstoreStateList;