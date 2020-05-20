/* eslint-disable require-jsdoc */
class EventStoreValidationError extends Error {
    constructor(message) {
        super(message);
        this.name = 'EventStoreValidationError';
    }
}

module.exports = EventStoreValidationError;