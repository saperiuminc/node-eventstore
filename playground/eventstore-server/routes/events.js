var express = require('express');
var router = express.Router();
const utils = require('../utils/index');
const shortid = require('shortid');
const { conforms } = require('lodash');
const Bluebird = require('bluebird');

const _getPlaybackListAsync = function(listName) {
    return new Promise((resolve, reject) => {
        utils.eventstore.getPlaybackList(listName, function(error, playbackList) {
            if (error) {
                reject(error);
            } else {
                resolve(playbackList);
            }
        })
    });
}

const _getPlaybackListViewAsync = function(listName) {
    return new Promise((resolve, reject) => {
        utils.eventstore.getPlaybackListView(listName, function(error, playbackList) {
            if (error) {
                reject(error);
            } else {
                resolve(playbackList);
            }
        })
    });
}

const _queryPlaybackListAsync = function(playbackList, start, limit) {
    return new Promise((resolve, reject) => {
        playbackList.query(start, limit, null, null, function(error, results) {
            if (error) {
                reject(error);
            } else {
                resolve(results);
            }
        })
    });
}
/* Add event */
router.post('/', async function(req, res) {
    const query = req.body.query
    const stream = await utils.eventstore.getLastEventAsStreamAsync(query);
    Bluebird.promisifyAll(stream);
    
    // const event = {
    //     name: 'DUMMY_CREATED',
    //     payload: {
    //         field1: 'field1value'
    //     }
    // };

    const event = req.body.event;
    stream.addEvent(event)
    await stream.commitAsync();

    res.json({
        result: 'OK'
    });

});

/* Add event */
router.post('/subscribe', async function(req, res) {
    // NOTE: used private async interface just for tests
    const query = req.body.query;

    await utils.eventstore.subscribe(query, 0, (err, event, callback) => {
        console.log('onEventCallback on', query, event);
        callback();
    }, (error) => {
        console.error('onErrorCallback on', query, error);
    });

    res.json({
        result: 'OK'
    });
});

// GET playback list
router.get('/:listName', async function(req, res) {
    const listName = req.params.listName;
    let start = parseInt(req.query.start);
    let limit = parseInt(req.query.limit);

    if (isNaN(start)) {
        start = 0;
    }

    if (isNaN(limit)) {
        limit = 10;
    }

    const playbackList = await _getPlaybackListViewAsync(listName);
    const results = await _queryPlaybackListAsync(playbackList, start, limit);

    res.json(results);

});

module.exports = router;