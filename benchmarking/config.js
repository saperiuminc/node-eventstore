module.exports = {
    clusterConfigs: [
        {
            type: 'mysql',
            host: 'localhost',
            port: '3306',
            user: 'root',
            password: 'root',
            database: 'eventstore',
            connectionPoolLimit: 1,
            projectionGroup: 'auction',
            context: 'auction'
        },
        {
            type: 'mysql',
            host: 'localhost',
            port: '3307',
            user: 'root',
            password: 'root',
            database: 'eventstore',
            connectionPoolLimit: 1,
            projectionGroup: 'auction',
            context: 'auction'
        },
        {
            type: 'mysql',
            host: 'localhost',
            port: '3308',
            user: 'root',
            password: 'root',
            database: 'eventstore',
            connectionPoolLimit: 1,
            projectionGroup: 'auction',
            context: 'auction'
        },
        {
            type: 'mysql',
            host: 'localhost',
            port: '3309',
            user: 'root',
            password: 'root',
            database: 'eventstore',
            connectionPoolLimit: 1,
            projectionGroup: 'auction',
            context: 'auction'
        }
    ],
    salesChannelInstanceVehicleLeaderComputedTestEventPayload: {
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
        extensionEndsAt: 1645064128300
    },
    profileMap: {
        1: 'one',
        2: 'two',
        3: 'three',
        4: 'four'
    }
}