const es = require('../index')();



(async function() {
    try {
        await es.project({
            projectionId: 'sdf',
            query: {
                aggregate: 'a',
                aggregateId: 'sdf',
                context: 'adf'
            },
            userData: null,
            partitionBy: "instance"
        });
        console.log('done');
    } catch (error) {
        console.error(error);
    }
})();