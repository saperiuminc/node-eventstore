const zbench = require('@saperiuminc/zbench');
const compose = require('docker-compose');
const mysql2 = require('mysql2/promise');
const path = require('path');
const bluebird = require('bluebird');
const debug = require('debug')('DEBUG');
const shortid = require('shortid');
const _ = require('lodash');
const configs = require('./config');


const shardCount = process.env.SHARD_COUNT || 1;
const config = {
    clusters: configs.clusterConfigs.slice(0, shardCount)
};

const sampleLeaderComputedEventPayload = _.cloneDeep(configs.salesChannelInstanceVehicleLeaderComputedTestEventPayload);

const context = 'auction';
const aggregate = 'salesChannelInstanceVehicle';

const numberOfVehicles = 1000;
const vehicleAggregateIds = [];


const sleep = function(timeout) {
    return new Promise((resolve) => {
        setTimeout(resolve, timeout);
    })
}

const getEventstore = async function() {
    const es = require('../lib/eventstore-projections/clustered-eventstore')(config);
    bluebird.promisifyAll(es);
    await es.initAsync()
    return es;
}

zbench('Sharded Add Events', (z) => {
    z.setupOnce(async (done, b) => {
        debug(`setting up benchmarking for ${shardCount} shards`);
        await compose.upAll({
            cwd: path.join(__dirname),
            composeOptions: [["--profile", configs.profileMap[shardCount]]],
            callback: (chunk) => {
                debug('compose in progress: ', chunk.toString())
            }
        });

        const retryInterval = 1000;
        let connectCounter = 0;
        const connectCounterThreshold = 60;
        while (connectCounter < connectCounterThreshold) {
            try {
                const clusterConfigPorts = configs.clusterConfigs.map( x => x.port );
                for (let i = 0; i < shardCount; i++) {
                    const mysqlConnection = await mysql2.createConnection({
                        host: 'localhost',
                        port: clusterConfigPorts[i],
                        user: 'root',
                        password: 'root'
                    });
                    await mysqlConnection.connect();
                    await mysqlConnection.query('CREATE DATABASE IF NOT EXISTS eventstore;');
                    await mysqlConnection.end();
                }

                break;
            } catch (error) {
                debug(`cannot connect to mysql. sleeping for ${retryInterval}ms`);
                connectCounter++;
                await sleep(retryInterval);
            }
        }

        if (connectCounter == connectCounterThreshold) {
            throw new Error('cannot connect to mysql');
        }
     
        debug('connected to mysql setup complete');
        

        done();
    });

    z.teardownOnce(async (done, b) => {
        debug('docker compose down started');
        await compose.down({
            cwd: path.join(__dirname)
        })
        debug('docker compose down finished');
        done();
    });

    z.setup(`sharding-${shardCount}`, (done, b) => {
        for (let i = 0; i < numberOfVehicles; i++) {
            vehicleAggregateIds.push(shortid.generate());
        }
        debug('setup complete');
        done();
    });

    // z.teardown('sharding', (done) => {
    //     done();
    // })

    z.test(`sharding-${shardCount}`, async (b) => {
        const es = await getEventstore();
        // const randomIndex = Math.floor((Math.random() * numberOfVehicles));
        const eventPayload = _.cloneDeep(sampleLeaderComputedEventPayload);
        const aggregateId = shortid.generate();// vehicleAggregateIds[randomIndex];

        eventPayload.salesChannelInstanceVehicleId = aggregateId;
        eventPayload.vehicleId = `vehicle-${aggregateId}`;
        
        b.start();
        es.getFromSnapshot({
            aggregateId: aggregateId,
            aggregate: aggregate,
            context: context,
        }, 0, (err, snapshot, stream) => {
            if (err) {
                console.log(err)
            }
            stream.addEvent({
                aggregateId: aggregateId,
                aggregate: aggregate,
                context: context,
                payload: eventPayload
            });
            stream.commit(() => {
                b.end();
            });
        });

    })
})