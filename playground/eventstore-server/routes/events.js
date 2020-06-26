var express = require('express');
var router = express.Router();
const utils = require('../utils/index');
const shortid = require('shortid');

/* GET users listing. */
router.post('/', async function(req, res) {
    // NOTE: used private async interface just for tests
    const query = {
        context: 'dummy_context',
        aggregate: 'dummy_aggregate',
        aggregateId: shortid.generate()
    }
    const stream = await utils.eventstore._getEventStreamAsync(query, 0, 10);

    const event = {
        name: 'DUMMY_CREATED',
        payload: {
            field1: 'field1value'
        }
    };

    await utils.eventstore._addEventToStream(stream, event);
    await utils.eventstore._commitStream(stream);

    res.json({
        result: 'OK'
    });

});

module.exports = router;