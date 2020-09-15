const shortid = require('shortid');

module.exports = function() {
    const distributedLock = jasmine.createSpyObj('distributedLock', ['lock', 'unlock']);
    distributedLock.lock.and.callFake(async (key) => {
        return key + '-' + shortid.generate();
    });

    distributedLock.unlock.and.returnValue(Promise.resolve());

    return distributedLock;
};