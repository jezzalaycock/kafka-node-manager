var log4js = require('log4js');
var logger = log4js.getLogger('test-producer');

var KafkaWriter = require('../lib/kafka/KafkaWriter');


logger.setLevel('INFO');

var topic = 'test',
    zookeeper = 'localhost:2181',
    clientId = 'clientId';
// -------------------------
// Init

var writer = new KafkaWriter(topic, zookeeper, clientId);
writer.start(function(){});

var count = 0;

setInterval(function () {
    for (var i = 0; i < 1; i++) {
        logger.info('Sent: ' + count);
        var payload = "oink: " + count;
        var message = { 'value': payload };
        writer.write(message, function (err) {
            if (err) {
                logger.error('Kafka error: %s', err.toString());
            }
        });
        count++;
    }

}, 2000);
