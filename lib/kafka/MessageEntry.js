var log4js = require('log4js');
var logger = log4js.getLogger('MessageEntry');

var MessageEntry = function (messageManager, id, group, topic, partition) {
    this.id = id;
    this.topic = topic;
    this.group = group;
    this.partition = partition;
    this.currentOffset = -1;
    this.toCommitOffsets = [];
    this.stale = false;
    this.dirty = false;
    this.firstTime = true;
    this.messageManager = messageManager;
}

MessageEntry.prototype.addOffset = function (offsetVal, cb) {
//    logger.debug("got message: " + offsetVal + " " + this.id);

    if (this.stale || offsetVal <= this.currentOffset) {
        logger.info("Consumer: %s received an unexpected message - ignoring", this.id);
    }
    else {
        var found = false;
        this.dirty = true;
        var offsetEntry = new OffsetEntry(offsetVal, cb);
        for (var el in this.toCommitOffsets) {
            if (this.toCommitOffsets[el].offset == offsetVal) {
                found = true;
                logger.warn("Consumer: %s received a duplicate message - replacing", this.id);
                this.toCommitOffsets[el] = offsetEntry;
                break;
            }
        }

        // add the entry
        if (!found) {
            this.toCommitOffsets.push(offsetEntry);
        }

        if (this.toCommitOffsets.length % 1000 == 0) {
            logger.warn('Consumer: %s has %s messages queued', this.id, this.toCommitOffsets.length);

            // Fail safe - if things go badly wrong
            if (this.toCommitOffsets.length > 10000){
                logger.error('Consumer: %s has %s messages queued exiting', this.id, this.toCommitOffsets.length);
                process.exit(1);
            }
        }
    }
}

MessageEntry.prototype.fetchOffset = function (cb) {
    var self = this;

    var request = {
        topic: this.topic,
        partition: this.partition
    }
    this.messageManager.offsetService.fetchCommits(this.group, [request], function (err, data) {
        if (err)
            cb(err);
        else {
            if (data[self.topic][self.partition] != -1) {
                self.currentOffset = data[self.topic][self.partition];
                // Ensure that the offset is actually correct
                if (self.firstTime){
                    self.firstTime = false;
                    var request = {'topic': self.topic, 'partition': self.partition, 'maxNum': 1000, 'metadata': 'm', 'time': Date.now()};
                    self.messageManager.offsetService.fetch([request], function (err, offsets) {
                        if (err)
                            cb(err);
                        else {
                            var fetchedOffset = offsets[self.topic][self.partition][0] - 1;
                            if (fetchedOffset > self.currentOffset) {
                                logger.error("Missmatch in %s offsets - fetched offset - %s, current offset %s", self.id, fetchedOffset, self.currentOffset);
                                self.currentOffset = fetchedOffset;
                            }
                            cb();
                        }
                    });
                }
                else
                    cb();
            }
            // Offsets are invalid - need to retrieve the earliest value from the partition
            else {
                logger.warn("Cannot retrieve current offsets - fetching the earliest entry for partition %s of %s", self.partition, self.topic);
                var request = {'topic': self.topic, 'partition': self.partition, 'maxNum': 1000, 'metadata': 'm', 'time': Date.now()};
                self.messageManager.offsetService.fetch([request], function (err, offsets) {
                    if (err)
                        cb(err);
                    else {
                        self.currentOffset = offsets[self.topic][self.partition][0] - 1;
                        logger.debug("Retrieved offset - %s", self.currentOffset);
                        cb();
                    }
                });

            }
        }
    });
}

MessageEntry.prototype.commit = function (cb) {
    var self = this;
    self.fetchOffset(function (err) {
        if (err) (cb(err));
        else {
            self.dirty = false;
            self.toCommitOffsets.sort(compareOffsets);
            if (!self.stale && self.toCommitOffsets.length
                && ((self.toCommitOffsets[self.toCommitOffsets.length - 1].offset - self.currentOffset) == 1
                    || self.currentOffset >= self.toCommitOffsets[self.toCommitOffsets.length - 1].offset)) {
                if (self.currentOffset >= self.toCommitOffsets[self.toCommitOffsets.length - 1].offset){
                    if (self.currentOffset >= self.toCommitOffsets[0].offset){
                        // simply empty the commited offsets
                        self.toCommitOffsets = [];
                    }
                    else{
                        // pop off the items that have already been committed
                        var poppedOffset = self.toCommitOffsets.pop();
                        while (poppedOffset != undefined && (self.currentOffset > poppedOffset.offset)) {
                            poppedOffset = self.toCommitOffsets.pop();
                        }
                    }
                }
                if (self.toCommitOffsets.length > 0) {
                    var newHighWaterMark = self.toCommitOffsets[self.toCommitOffsets.length - 1].offset;
                    var commitList = []
                    var poppedOffset = self.toCommitOffsets.pop();
                    while (poppedOffset != undefined && (poppedOffset.offset == newHighWaterMark)) {
                        newHighWaterMark = poppedOffset.offset;
                        commitList.push(poppedOffset);
                        poppedOffset = self.toCommitOffsets.pop();
                    }
                    if (poppedOffset != undefined) self.toCommitOffsets.push(poppedOffset);

                    // Commit the new offsets
                    var request = {
                        topic: self.topic,
                        partition: self.partition,
                        offset: newHighWaterMark,
                        metadata: 'm'
                    }
                    self.messageManager.offsetService.commit(self.group, [request], function (err, data) {
                        if (err) {
                            // Cannot commit inform the consumer
                            logger.warn("Failed to commit offset %s of %s", newHighWaterMark, self.id);
                            cb(err);
                        }
                        else {
                            // Notify all listeners
                            commitList.map(function (offsetEntry) {
                                offsetEntry.cb();
                            })
                            logger.info("committed offset %s of %s count: %s", newHighWaterMark, self.id, commitList.length);
                            cb();
                        }
                    });
                }
                else {
                    logger.info("Nothing to commit: %s", self.id);
                    cb();
                }

            }
        }
    });

}

function compareOffsets(a, b) {
    return b.offset - a.offset;
}

var OffsetEntry = function (offset, cb) {
    this.offset = offset;
    this.cb = cb;
}

module.exports = MessageEntry;