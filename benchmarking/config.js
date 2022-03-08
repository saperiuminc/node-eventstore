module.exports = {
    clusterPorts: [
        '3306', '3307', '3308',
        '3309', '3310', '3311',
        '3312', '3313', '3314',
        '3315', '3316', '3317',
        '3318', '3319', '3320'
    ],
    esConfig: {
        type: 'mysql',
        host: 'localhost',
        port: '3306',
        user: 'root',
        password: 'root',
        database: 'eventstore',
        connectionPoolLimit: 20,
        projectionGroup: 'auction',
        context: 'auction'
    },
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
        4: 'four',
        5: 'five',
        6: 'six',
        7: 'seven',
        8: 'eight',
        9: 'nine',
        10: 'ten',
        15: 'all'
    }
}