const shortid = require('shortid');

module.exports = function() {
    const distributedLock = jasmine.createSpyObj('distributedLock', ['lock', 'unlock', 'extend']);
    distributedLock.lock.and.callFake(async (key) => {
        return key + '-' + shortid.generate();
    });

    distributedLock.unlock.and.returnValue(Promise.resolve());
    distributedLock.extend.and.returnValue(Promise.resolve());
    
    return distributedLock;
};