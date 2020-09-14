function EventstorePlaybackListInMemoryStore() {};

EventstorePlaybackListInMemoryStore.prototype = {
    _lists: {},
    createList: async function(listOptions) {
        this._lists[listOptions.name] = {
            items:{}
        };
    },

    add: async function(listName, rowId, revision, data, meta) {
        if (this._lists[listName].items[rowId]) {
            throw new Error('cannot add. rowId already exists');
        }

        this._lists[listName].items[rowId] = {
            rowId: rowId,
            revision, revision,
            data: data,
            meta: meta
        };
    },

    update: async function(listName, rowId, revision, data, meta) {
        if (!this._lists[listName].items[rowId]) {
            throw new Error('cannot update. rowId does not exist');
        }

        this._lists[listName].items[rowId] = {
            rowId: rowId,
            revision, revision,
            data: data,
            meta: meta
        };
    },

    delete: async function(listName, rowId) {
        if (!this._lists[listName].items[rowId]) {
            throw new Error('cannot delete. rowId does not exist');
        }

        delete this._lists[listName].items[rowId];
    },

    get: async function(listName, rowId) {
        return this._lists[listName].items[rowId];
    },

    pollingGet: async function(listName, rowId, timeout) {
        const startTime = Date.now();
        let result = null;
        do {
            result = this._lists[listName].items[rowId];

            if (!result) {
                // sleep every 10ms
                await this._sleep(10);
            } else {
                break;
            }
            
        } while (Date.now() - startTime < timeout);

        return result;
    },

    reset: async function() {
        // just reset the _lists field
        this._lists = {};
    },
    
    _sleep: async function(timeout) {
        return new Promise((resolve) => {
            setTimeout(resolve, timeout);
        })
    }
}

module.exports = EventstorePlaybackListInMemoryStore;