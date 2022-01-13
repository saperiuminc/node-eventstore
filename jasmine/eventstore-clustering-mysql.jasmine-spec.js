const compose = require('docker-compose');
const path = require('path');
const debug = require('debug')('eventstore:clustering:mysql');
const mysql2 = require('mysql2/promise');
const clustedEs = require('../clustered/index');
const Bluebird = require('bluebird');
const shortId = require('shortid');

fdescribe('eventstore clustering mysql tests', () => {
    const sleep = function(timeout) {
        return new Promise((resolve) => {
            setTimeout(resolve, timeout);
        })
    }

    beforeAll(async () => {
        await compose.upAll({
            cwd: path.join(__dirname),
            callback: (chunk) => {
                debug('compose in progres: ', chunk.toString())
            }
        });

        const retryInterval = 1000;
        let connectCounter = 0;
        while (connectCounter < 30) {
            try {
                const mysqlConnection = await mysql2.createConnection({
                    host: 'localhost',
                    port: '3306',
                    user: 'root',
                    password: 'root'
                });
                await mysqlConnection.connect();
                await mysqlConnection.end();

                const mysqlConnection2 = await mysql2.createConnection({
                    host: 'localhost',
                    port: '3307',
                    user: 'root',
                    password: 'root'
                });
                await mysqlConnection2.connect();
                await mysqlConnection2.end();

                break;
            } catch (error) {
                debug(`cannot connect to mysql. sleeping for ${retryInterval}ms`);
                connectCounter++;
                await sleep(retryInterval);
            }
        }

        if (connectCounter == 10) {
            throw new Error('cannot connect to mysql');
        }

        debug('successfully connected to mysql');
    });

    afterAll(async () => {
        debug('docker compose down started');
        await compose.down({
            cwd: path.join(__dirname)
        })
        debug('docker compose down finished');
    });

    it('should implement init', async () => {
        // TODO: add the options
        const clustedEventstore = clustedEs({});

        Bluebird.promisifyAll(clustedEventstore);
        await clustedEventstore.initAsync();
    })

    it('should be able to add event to the stream', async () => {
        // TODO: add the options
        const clustedEventstore = clustedEs({});

        Bluebird.promisifyAll(clustedEventstore);
        await clustedEventstore.initAsync();

        const aggregateId = shortId.generate();

        const stream = await clustedEventstore.getEventStreamAsync({
            aggregateId: aggregateId,
            aggregate: 'vehicle',
            context: 'vehicle'
        });

        Bluebird.promisifyAll(stream);

        const event = {
            name: "VEHICLE_CREATED",
            payload: {
                vehicleId: aggregateId,
                year: 2012,
                make: "Honda",
                model: "Jazz",
                mileage: 1245
            }
        }

        stream.addEvent(event);
        await stream.commitAsync();

        const savedStream = await clustedEventstore.getEventStreamAsync({
            aggregateId: aggregateId,
            aggregate: 'vehicle',
            context: 'vehicle'
        });

        expect(savedStream.events[0].payload).toEqual(event);
    })
})