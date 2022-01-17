const ClusteredEventStore = require("./clustered-eventstore");

module.exports = function(options) {
    // TODO: subset options
    return new ClusteredEventStore(options);
}
