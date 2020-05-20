/* eslint-disable require-jsdoc */
class EventStoreDuplicateError extends Error {
    constructor(message) {
        super(message);
        this.name = 'EventStoreDuplicateError';
    }
}

module.exports = EventStoreDuplicateError;