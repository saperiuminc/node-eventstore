const ClusteredEventStore = require("./clustered-eventstore");
const ClusteredMappingStore = require('./clustered-mapping-store');

module.exports = function(options) {
    // TODO: subset options
    return new ClusteredEventStore(options, new ClusteredMappingStore(options));
}
