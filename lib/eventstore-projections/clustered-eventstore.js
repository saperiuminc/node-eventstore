const _ = require('lodash');
const debug = require('debug')('eventstore:clustered');
const TaskAssignmentGroup = require('./task-assignment').TaskAssignmentGroup;
const EventstoreWithProjection = require('./eventstore-projection');
const helpers = require('../helpers');

class ClusteredEventStore extends EventstoreWithProjection {
    constructor(opts, store, distributedSignal, distributedLock, playbackListStore, playbackListViewStore, projectionStore, stateListStore, outputsTo) {
        super(opts, store, distributedSignal, distributedLock, playbackListStore, playbackListViewStore, projectionStore, stateListStore, outputsTo);
        
        const defaultOptions = {
            partitions: 25,
            membershipPollingTimeout: 10000
        };
        
        this._options = Object.assign(_.clone(defaultOptions), opts);
        this._options.numberOfShards = this._options.clusters.length;
        
        this._eventstoreCount = 0;
        this._eventstores = [];
        this._distributedLock = distributedLock;
        this._taskGroup = null;
    }
    
    async createProjectionTasks(projection) {
        let projectionTasks;
        const projectionConfig = projection.configuration;
        let concurrencyType = '';
        if (projectionConfig.partitionBy == 'stream') {
            concurrencyType = 'multi';
        } else {
            concurrencyType = 'single';
        }
        const storedProjectionTasks = await this._projectionStore.getProjectionTasks(projection.projectionId);
        if (storedProjectionTasks.length === 0) {
            const createProjectionTaskPromises = [];
            let partitions = null;
            if (concurrencyType === 'multi') {
                partitions = [];
                for (let i = 0; i < this._options.partitions; i++) {
                    partitions.push(i);
                }
            }
            const hasPartitions = !partitions || !partitions.length ? false : true;
            const createProjectionTaskFunc = async (shard, partition) => {
                let projectionTaskOffset = null;
                if ((projection && projectionConfig && projectionConfig.fromOffset === 'latest')) {
                    projectionTaskOffset = await this._getLatestOffsetAsync(shard > -1 ? shard : null, hasPartitions ? partition : null);
                } else if (!hasPartitions) {
                    projectionTaskOffset = this.store.getOffsetManager().getStartingSerializedOffset(hasPartitions);
                }
                const newProjectionTask = {
                    projectionTaskId: this._getProjectionTaskId(projection.projectionId, shard, hasPartitions ? partition : null),
                    projectionId: projection.projectionId,
                    shard: shard,
                    partition: hasPartitions ? partition : '__unpartitioned',
                    offset: projectionTaskOffset
                };
                debug('CLUSTERED-ES: CREATING PROJECTION TASK', newProjectionTask);
                createProjectionTaskPromises.push(this._projectionStore.createProjectionTask(newProjectionTask));
            }
            if (!hasPartitions) {
                await createProjectionTaskFunc(-1, null);
            } else {
                for (let i = 0; i < this._options.numberOfShards; i++) {
                    for (const partition of partitions) {
                        await createProjectionTaskFunc(i, partition);
                    }
                }
            }
            
            await Promise.all(createProjectionTaskPromises);
        } else {
            debug('CLUSTERED-ES: TASKS ALREADY EXISTING FOR PROJECTION', projection.projectionId);
            projectionTasks = storedProjectionTasks;
        }
        
        return projectionTasks;
    }
    
    async doTaskAssignment(projectionGroup, projectionTasks) {
        
        const projectionTaskIds = _.map(projectionTasks, (task) => {
            return task.projectionTaskId;
        });
        debug('CLUSTERED-ES: doTaskAssignment CALLED', projectionTaskIds);
        this._taskGroup = new TaskAssignmentGroup({
            initialTasks: projectionTaskIds,
            createRedisClient: this._options.redisCreateClient,
            groupId: projectionGroup,
            membershipPollingTimeout: this._options.membershipPollingTimeout,
            distributedLock: this._distributedLock
        });
        
        await this._taskGroup.join();
        
        this._taskGroup.on('rebalance', async (updatedAssignments, rebalanceId) => {
            debug('CLUSTERED-ES: REBALANCE CALLED', updatedAssignments);
            // distribute to shards
            await this.processProjectionTasks(updatedAssignments, rebalanceId);
        })
    };


    getChannelsToSignal(query) {
        let partition = this.store.getPartitionId(query.aggregateId);
        let index = 0;
        let allChannels = []
        while (index < this._options.numberOfShards) {
            const shard = _.clone(index);
            const channels = this._queryToChannels(query, shard, partition);
            allChannels = _.uniq(allChannels.concat(channels));
            index++;
        }
        // TODO: Review. Add unpartitioned subscription
        const channels = this._queryToChannels(query, -1, '__unpartitioned');
        allChannels = _.uniq(allChannels.concat(channels));
        return allChannels;
    }

    /**
     * @param {AggregateQuery} query
     * @param {Number} shard
     * @param {string} partition
     * @returns {String[]} returns an array of possible topics of the query
     */
    _queryToChannels(query, shard, partition) {
          const channels = [];

          channels.push('all.all.all');
          channels.push(`${query.context}.all.all`);
          channels.push(`${query.context}.${query.aggregate}.all`);
          channels.push(`${query.context}.${query.aggregate}.${query.aggregateId}`);
          channels.push(`all.all.all.${shard}.${partition}`);
          channels.push(`${query.context}.all.all.${shard}.${partition}`);
          channels.push(`${query.context}.${query.aggregate}.all.${shard}.${partition}`);
          channels.push(`${query.context}.${query.aggregate}.${query.aggregateId}.${shard}.${partition}`);

          return channels;
      }
    
    _getProjectionTaskId(projectionId, shard, partition) {
        if (!_.isNil(partition)) {
            return `${projectionId}:shard${shard}:partition${partition}`;
        } else {
            return projectionId;
        }
    }
}

module.exports = ClusteredEventStore;
