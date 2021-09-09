/* eslint-disable require-jsdoc */
class EventStoreError extends Error {
    constructor(message, innerError) {
        super(message);
        this.name = 'EventStoreError';
        this.innerError = innerError;
    }
}

module.exports = EventStoreError;