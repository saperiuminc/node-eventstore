const InMemoryStore = require('../../../lib/databases/inmemory');

// TODO: filter out events
InMemoryStore.prototype.getEvents = function(query, skip, limit, callback) {
    var res = [];
    for (var s in this.store) {
        for (var ss in this.store[s]) {
            for (var sss in this.store[s][ss]) {
                res = res.concat(this.store[s][ss][sss]);
            }
        }
    }

    res = _.sortBy(res, function(e) {
        return e.commitStamp.getTime();
    });

    if (!_.isEmpty(query)) {
        res = _.filter(res, function(e) {
            var keys = _.keys(query);
            var values = _.values(query);
            var found = false;
            for (var i in keys) {
                var key = keys[i];
                var deepFound = deepFind(e, key);
                if (_.isArray(deepFound) && deepFound.length > 0) {
                    found = true;
                } else if (deepFound === values[i]) {
                    found = true;
                } else {
                    found = false;
                    break;
                }
            }
            return found;
        });
    }

    if (limit === -1) {
        return callback(null, _.cloneDeep(res.slice(skip)));
    }

    if (res.length <= skip) {
        return callback(null, []);
    }

    callback(null, _.cloneDeep(res.slice(skip, skip + limit)));
}



module.exports = InMemoryStore