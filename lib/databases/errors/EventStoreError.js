/* eslint-disable require-jsdoc */
class EventStoreError extends Error {
    constructor(innerError, message) {
        super(message);
        this.innerError = innerError;
        this.name = 'EventStoreError';
    }
}

module.exports = EventStoreError;