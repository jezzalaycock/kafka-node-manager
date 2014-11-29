var log4js = require('log4js');
var logger = log4js.getLogger('MessageManager');

var MessageEntry = require('./MessageEntry');
var async = require("async");

var MessageManager = function (offsetService, consumer) {
    this.offsetService = offsetService;
    this.consumer = consumer;
    this.consumerTopicOffsetMap = [];
}

MessageManager.prototype.addMessageMapping = function (group, topic, partition) {
    // Add to mapping or reset the existing
    var id = group + '-' + topic + '-' + partition;
    var topicOffsetEntry = this.consumerTopicOffsetMap[id];
    if (topicOffsetEntry == undefined) {
        logger.info("Registering new mapping: %s", id);
        topicOffsetEntry = new MessageEntry(this, id, group, topic, partition);
        this.consumerTopicOffsetMap.push(topicOffsetEntry.id);
        this.consumerTopicOffsetMap[topicOffsetEntry.id] = topicOffsetEntry;
    }
    return id;
}

MessageManager.prototype.reset = function () {
    // Clear all the mappings
    logger.info("Resetting the mappings..");
    this.consumerTopicOffsetMap = [];
}

MessageManager.prototype.updateMessageMapping = function (id, offset, cb) {
    // Update to mapping and set as dirty
    var topicOffsetEntry = this.consumerTopicOffsetMap[id];
    if (topicOffsetEntry != undefined) {
        topicOffsetEntry.addOffset(offset, cb);
    }
    else cb(new Error("Message entry does not exist"))
}

MessageManager.prototype.isMappingPresent = function (id) {
    var topicOffsetEntry = this.consumerTopicOffsetMap[id];
    if (topicOffsetEntry != undefined) {
        return true;
    }
    else return false;
}

MessageManager.prototype.commit = function (callback) {
    // Iterate through each entry and commit if required
    var self = this;

    if (self.consumerTopicOffsetMap != undefined && self.consumerTopicOffsetMap.length > 0) {

        // Commit any outstanding data
        async.each(self.consumerTopicOffsetMap, function (topicOffsetEntry, cbb) {
            if (self.consumerTopicOffsetMap[topicOffsetEntry].dirty && !self.consumerTopicOffsetMap[topicOffsetEntry].stale) {
                self.consumerTopicOffsetMap[topicOffsetEntry].commit(function (err) {
                    if (err)
                        cbb(err);
                    else {
                        cbb();
                    }
                });
            }
            else {
                cbb();
            }
        }, function (err) {
            if (err)
                callback(err);
            else callback();
        });
    }
    else {
        callback();
    }

}

module.exports = MessageManager;
