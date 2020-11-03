function EventstoreProjectionInMemoryStore() {
    this._projectionList = {};     
};

EventstoreProjectionInMemoryStore.prototype = {

    init: async function() { 
    },

    createProjectionIfNotExists: async function(projection) {
        this._projectionList[projection.projectionId] = projection;
    },

    getProjection: async function(projectionId) {
        return this._projectionList[projectionId];
    },

    setProcessed: async function(projectionId, processedDate, newOffset) {
        const projection = await this.getProjection(projectionId);
        if (projection) {
            projection.processedDate = processedDate;
            if (newOffset) {
                projection.offset = newOffset;
            }
        }
    },

    clearAll: async function() {
        this._projectionList = {};
    },

    pollingGetProjection: async function(projectionId, comparer, timeout) {
        if (!comparer) {
            comparer = () => false;
        }

        let delay = parseInt(timeout);
        if (isNaN(delay)) {
            delay = 1000;
        }

        const sleep = async function(timeout) {
            return new Promise((resolve) => {
                setTimeout(resolve, timeout);
            })
        }

        const startTime = Date.now();
        let projection = null;
        do {
            projection = await this.getProjection(projectionId);

            if (!comparer(projection)) {
                // sleep every 10ms
                await sleep(10);
            } else {
                break;
            }

        } while (Date.now() - startTime < delay);

        return projection;
    },

    _sleep: async function(timeout) {
        return new Promise((resolve) => {
            setTimeout(resolve, timeout);
        })
    }
}

module.exports = EventstoreProjectionInMemoryStore;