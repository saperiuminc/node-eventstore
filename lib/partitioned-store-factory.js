const _ = require('lodash');
const debug = require('debug')('eventstore:clustered');
const ClusteredPartitionedStore = require('./databases/partition-stores/clustered/clustered-partitioned-store');
const StreamBufferedStore = require('./databases/partition-stores/stream-buffered/stream-buffered-partitioned-store');
const CompositeOffsetManager = require('./offset-managers/composite-offset-manager');
const ClusteredCompositeOffsetManager = require('./offset-managers/clustered-composite-offset-manager');

function exists(toCheck) {
    var _exists = require('fs').existsSync || require('path').existsSync;
    if (require('fs').accessSync) {
        _exists = function(toCheck) {
            try {
                require('fs').accessSync(toCheck);
                return true;
            } catch (e) {
                return false;
            }
        };
    }
    return _exists(toCheck);
}

/**
 * PartitionedStore
 * @typedef {import('./databases/partitioned-store').PartitionedStore} PartitionedStore
 */

const factory = (function() {
    const _self = this;

    _self._getSpecificPartitionedStore = function(options, parentOptions) {
        let partitionedStore;
        const compositeOffsetManager = new CompositeOffsetManager();
        if (!parentOptions) {
            parentOptions = _.cloneDeep(options);
        }

        if (options.clusters && Array.isArray(options.clusters) && options.clusters.length > 0) {
            const clusteredCompositeOffsetManager = new ClusteredCompositeOffsetManager(options.clusters.length, compositeOffsetManager);
            let clusteredPartitionedStore = new ClusteredPartitionedStore(options, clusteredCompositeOffsetManager);

            for (let storeConfig of options.clusters) {
                if (!storeConfig.partitions && parentOptions.partitions) {
                    storeConfig.partitions = parentOptions.partitions;
                }
                storeConfig.redisCreateClient = parentOptions.redisCreateClient;
                clusteredPartitionedStore.addStore(this.getSpecificPartitionedStore(storeConfig, parentOptions));
            }
            partitionedStore = clusteredPartitionedStore;
        } else {
            const optionsType = options.type + '-partitioned-store';
            const dbPath = __dirname + '/databases/partition-stores/databases/' + optionsType + '.js';

            if (!exists(dbPath)) {
                const errMsg = 'Implementation for db "' + options.type + '" does not exist!';
                debug(errMsg);
                throw new Error(errMsg);
            }

            try {
                debug('dbPath:', dbPath);
                const SpecificPartitionedStore = require(dbPath);
                const specificPartitionedStore = new SpecificPartitionedStore(options, compositeOffsetManager);
                
                if (parentOptions.enableProjectionEventStreamBuffer) {
                    partitionedStore = new StreamBufferedStore({}, compositeOffsetManager, specificPartitionedStore);
                } else {
                    partitionedStore = specificPartitionedStore;
                }
            } catch (err) {
                console.error('error in requiring store');
                console.error(err);
                if (err.message.indexOf('Cannot find module') >= 0 &&
                err.message.indexOf("'") > 0 &&
                err.message.lastIndexOf("'") !== err.message.indexOf("'")) {
                    var moduleName = err.message.substring(err.message.indexOf("'") + 1, err.message.lastIndexOf("'"));
                    var msg = 'Please install module "' + moduleName +
                    '" to work with db implementation "' + options.type + '"!';
                    debug(msg);
                }
                throw err;
            }
        }

        return partitionedStore;
    };

    _self.getSpecificPartitionedStore = function(options) {
        return _self._getSpecificPartitionedStore(options, undefined);
    };

    return _self;
});

module.exports = factory;