/* eslint-disable require-jsdoc */
class EventStorePlaybackListViewPaginationError extends Error {
    constructor(message, innerError) {
        super(message);
        this.name = 'EventStorePlaybackListViewPaginationError';
        this.innerError = innerError;
    }
}

module.exports = EventStorePlaybackListViewPaginationError;