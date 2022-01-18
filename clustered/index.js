const ClusteredEventStore = require("../lib/eventstore-projections/clustered-eventstore");

module.exports = function(options) {
    // TODO: subset options
    return new ClusteredEventStore(options);
}
