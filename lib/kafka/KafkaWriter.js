var kafka = require('kafka-node'),
    log4js  = require('log4js'),
    util = require('util'),
    EventEmitter = require('events').EventEmitter;
var _ = require('underscore');

var logger = log4js.getLogger('KafkaWriter');

KafkaWriter = function(topic, zk, clientId, options) {
    logger.info('Connecting to Kafka for client %s on - %s', clientId, zk);
    this.topic = topic;
    this.zk = zk;
    this.clientId = clientId;
    this.statusTopic = topic+'-status';
    this.ready = false;
    this._initting = true;
    this.options = _.extend({
        maxSizeBytes: 1000000
    }, options || {});
}

util.inherits(KafkaWriter, EventEmitter);

KafkaWriter.prototype.start = function(callback) {
    var self = this;
    self.client = new kafka.Client(self.zk, self.clientId);
    self.producer = new kafka.HighLevelProducer(self.client);
    self.producer.on('ready', function() {
        self._write(self.statusTopic, 'ping', function (err, data) {
            if (err) {
                logger.error('%s connection error: %s ', self.clientId, err.toString());
                self.ready = false;
                self._initting = false;
                callback(err);
            } else {
                logger.debug('%s connected', self.clientId);
                self.ready = true;
                self._initting = false;
                callback();
            }
        });
    });
}


KafkaWriter.prototype._write = function(topic, data, callback) {
    var self = this;

    if (self.ready || self._initting) {
        var message = JSON.stringify(data);
        if (Buffer.byteLength(message, 'utf8') <= self.options.maxSizeBytes) {
            self.producer.send([
                { topic: topic, messages: message }
            ], function (err) {
                if (err) {
                    self.producer.send([
                        { topic: topic, messages: message }
                    ], function (err) {
                        if (err) {
                            callback(err);
                        }
                        callback();
                    });
                } else {
                    callback();
                }
            });
        }
        else{
            var err = 'Kafka error: ' + this.topic + ' write request is bigger than the maximum allowed: ' + Buffer.byteLength(message, 'utf8');
            logger.error(err);
            callback (err);
        }
    }
    else{
        var err = 'Kafka error: write request before the producer is ready for topic: ' + this.topic;
        logger.error(err);
        callback (err);
    }
}

KafkaWriter.prototype.write = function(data, callback) {
    this._write(this.topic, data, callback);
}

module.exports = KafkaWriter;


