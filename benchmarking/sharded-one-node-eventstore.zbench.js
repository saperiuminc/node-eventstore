const zbench = require('@saperiuminc/zbench');
const compose = require('docker-compose');
const mysql2 = require('mysql2/promise');
const path = require('path');
const bluebird = require('bluebird');
const debug = require('debug')('DEBUG');
const shortid = require('shortid');
const _ = require('lodash');

const config = {
    type: 'mysql',
    host: 'localhost',
    port: '3306',
    user: 'root',
    password: 'root',
    database: 'eventstore',
    connectionPoolLimit: 1,
    projectionGroup: 'auction',
    context: 'auction'
};

const sampleLeaderComputedEventPayload = {
    vehicleId: '',
    salesChannelInstanceVehicleId: '',
    previousLeader: null,
    isReserveMet: false,
    leader: {
        dealerFirstName: 'Test First ',
        dealerPhotoPath: 'test.com/profile-pic.jpg',
        dealerName: 'Test First Last',
        dealershipName: 'Test Dealership',
        dealerLastName: 'Last',
        bid: 145600,
        dealerId: 'dealer-id',
        dealershipId: 'dealership-id',
        bidType: 'regularbid'
    },
    bidders: null,
    extensionEndsAt: (new Date()).getTime()
};

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
    const es = require('@saperiuminc/eventstore')(config);
    bluebird.promisifyAll(es);
    await es.initAsync()
    return es;
}

zbench('bench mysql', (z) => {
    z.setupOnce(async (done, b) => {
        // TODO: wait for download of images
        await compose.upAll({
            cwd: path.join(__dirname),
            composeOptions: [["--profile", "one"]],
            callback: (chunk) => {
                debug('compose in progress: ', chunk.toString())
            }
        });

        const retryInterval = 1000;
        let connectCounter = 0;
        const connectCounterThreshold = 30;
        while (connectCounter < connectCounterThreshold) {
            try {
                const mysqlConnection = await mysql2.createConnection({
                    host: 'localhost',
                    port: '3306',
                    user: 'root',
                    password: 'root'
                });
                await mysqlConnection.connect();
                await mysqlConnection.query('CREATE DATABASE IF NOT EXISTS eventstore;');
                await mysqlConnection.end();

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

    z.setup('sharding', (done, b) => {
        for (let i = 0; i < numberOfVehicles; i++) {
            vehicleAggregateIds.push(shortid.generate());
        }
        debug('setup complete');
        done();
    });

    z.teardown('sharding', (done) => {
        done();
    })

    z.test('sharding', async (b) => {
        const es = await getEventstore();
        const randomIndex = Math.floor((Math.random() * numberOfVehicles));
        const eventPayload = _.cloneDeep(sampleLeaderComputedEventPayload);
        const aggregateId = vehicleAggregateIds[randomIndex];

        eventPayload.salesChannelInstanceVehicleId = aggregateId;
        eventPayload.vehicleId = `vehicle-${aggregateId}`;
        
        b.start();
        const stream = await es.getEventStreamAsync({
            aggregateId: aggregateId,
            aggregate: aggregate,
            context: context,
        });
        
        stream.addEvent({
            aggregateId: aggregateId,
            aggregate: aggregate,
            context: context,
            payload: eventPayload
        });
        stream.commit();

        b.end();
    })
})