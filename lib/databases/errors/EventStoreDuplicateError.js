/* eslint-disable require-jsdoc */
class EventStoreDuplicateError extends Error {
    constructor(message, innerError) {
        super(message);
        this.name = 'EventStoreDuplicateError';
        this.innerError = innerError;
    }
}

module.exports = EventStoreDuplicateError;