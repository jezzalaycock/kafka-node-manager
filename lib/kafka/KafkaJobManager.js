var kafka = require('kafka-node'),
    Consumer = kafka.HighLevelConsumer,
    Producer = kafka.HighLevelProducer;

var MessageManager = require('./MessageManager');

var log4js = require('log4js');
var logger = log4js.getLogger('KafkaJobManager');

var KafkaJobManager = function (job, zk, clientId, groupId, topic, messageCount, notifySuccess, notifyFailure, deadLetterTopic) {
    this.job = job;
    this.zk = zk;
    this.clientId = clientId;
    this.groupId = groupId || this.clientId;
    this.topic = topic;
    this.deadLetterTopic = deadLetterTopic;
    this.messageCount = messageCount || 100;
    this.notifyFailure = notifyFailure;
    this.notifySuccess = notifySuccess;
    this.initialised = false;
    this.client = null;
    this.offset = null;
    this.consumer = null;
    this.messageManager = null;
    this.initialise();
}

KafkaJobManager.prototype.isMessageValid = function (id) {
 return this.messageManager.isMappingPresent(id);
}


KafkaJobManager.prototype.excute = function () {

    var self = this;

    var messagesRead = 0;
    var messagesProcesed = 0;
    var paused = true;
    var rebalancing = true;

    this.consumer.on('message', function (message) {
        var messageId = self.messageManager.addMessageMapping(self.groupId, message.topic, message.partition);
        messagesRead++;
        var obj = JSON.parse(message.value);
        // run the job
        self.job(obj, messageId, function (err) {
            try {
                messagesProcesed++;
                self.messageManager.updateMessageMapping(messageId, message.offset, function (mappingErr) {
                    if (mappingErr) {
                        logger.info("Unable to update the mapping: %s, offset: %s because of: %s", messageId, message.offset, JSON.stringify(mappingErr));
                    }
                    else {
                        if (err) {
                            // Dump onto deal letter topic
                            if (self.deadLetterTopic) self._fail(message.value, function (dlerr){
                                if (dlerr) logger.error("Error sending message: %s to deadletter topic: %s", message.value, dlerr);
                            })
                            if (self.notifyFailure)
                                self.notifyFailure(obj);
                        }
                        else {
                            if (self.notifySuccess)
                                self.notifySuccess(obj);
                        }
                    }
                });
            } catch (e) {
                jobFailure(self.consumer, new Error("Error processing message - %s", e));
            }
        });
    });

    this.consumer.on('rebalancing', function (err) {
        if (err)  jobFailure(self.consumer, err);
        else {
            rebalancing = true;
            self.consumer.pause();
            paused = true;
            self.messageManager.reset();
        }
    });

    this.consumer.on('rebalanced', function (err) {
        if (err)  jobFailure(self.consumer, err);
        else {
            rebalancing = false;
        }
    });

    this.consumer.on('error', function (err) {
        jobFailure(self.consumer, err);
    });

    setInterval(function () {
        if (!rebalancing) {
            self.messageManager.commit(function (err) {
                try {
                    if (err) {
                        jobFailure(self.consumer, err);
                    }
                } catch (e) {
                    jobFailure(self.consumer, new Error("Error committing- %s", e));
                }
            });
        }
    }, 500);

    setInterval(function () {
        if (messagesRead - messagesProcesed > self.messageCount) {
            if (!paused) {
                logger.info("Pausing: messages- read: %s, processed: %s ", messagesRead, messagesProcesed);
                self.consumer.pause();
                paused = true;
            }
        }
        else {
            if (paused) {
                logger.info("Resuming: messages- read: %s, processed: %s ", messagesRead, messagesProcesed);
                self.consumer.resume();
                paused = false;
            }
        }
    }, 5000);

}

KafkaJobManager.prototype._fail = function (message, cb) {
    logger.info("Sending failed message to the dead-letter topic: %s ", this.deadLetterTopic);
    sendMessage(this.producer, this.deadLetterTopic, message, cb);
}

KafkaJobManager.prototype.initialise = function () {
    this.client = new kafka.Client(this.zk, this.clientId, { sessionTimeout: 30000, spinDelay : 1000, retries : 2 });
    this.producer = new Producer(this.client);
    this.consumer = new Consumer(this.client, [
        { topic: this.topic }
    ], {groupId: this.groupId, autoCommit: false, paused:true, maxTickMessages: 100, fetchMaxBytes: 1024 * 1024});
    this.offset = new kafka.Offset(this.client);
    this.messageManager = new MessageManager(this.offset, this.consumer);
    this.initialised = true;
}

function sendMessage(producer, topic, message, callback) {
    producer.send([ { topic: topic, messages:message } ], function (err) {
        if (err) {
            producer.send([
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

function jobFailure(consumer, err) {
    logger.error("Catastrophic error - %s", err.toString());
    consumer.close();
    process.exit(1);
}

module.exports = KafkaJobManager;
