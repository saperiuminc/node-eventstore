/* eslint-disable require-jsdoc */
class EventStoreValidationError extends Error {
    constructor(message, innerError) {
        super(message);
        this.name = 'EventStoreValidationError';
        this.innerError = innerError;
    }
}

module.exports = EventStoreValidationError;