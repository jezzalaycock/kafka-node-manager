/**
 * Module dependencies.
 */
var KafkaJobManager = require('../lib/kafka/KafkaJobManager');

var messageCount = 0;
var jobManager = new KafkaJobManager(function (obj, id, cb) {
    try {
        messageCount++;
        cb();

    }
    catch
        (e) {
        console.log("Cannot parse message " + e)
    }

}, 'localhost:2181', "consumerId", 'test', 1000, null, null);

jobManager.excute();
