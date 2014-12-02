Kafka-node-manager
==================

Kafka-node-manager performs management of kafka-node processing jobs and ensures that messages are only committed when the job completes.

Operation
=========

KafkaJobManager reads and commits messages on behalf of the client. The jobmmanager will call the provided client function with the message read from kafka. 
The client can throttle the number of inflight messages and optionally provide callbacks for success and failure and a topic to send failed messages to.

    var jobManager = new KafkaJobManager(clientFunction, zkConnection, consumerGroup, topic, inflightMessages, notifySuccess, notifyFailure, deadLetterTopic);

Use of [pm2](https://github.com/Unitech/pm2) to manage the execution of your client process is strongly recommended as there are circumstances where a forced reconnect is required.

Installation
============

    npm install kafka-node-manager
    

Example Usage
=============

    var kjm = require('kafka-node-manager'),
    KafkaJobManager = kjm.KafkaJobManager;
    
    var jobManager = new KafkaJobManager(onRead, 'localhost:2181', "consumerId", 'test', 1000, notifySuccess, notifyFailure, 'deadLetter');
    var onRead = function (obj, messageId, cb) { 
        console.log(JSON.stringify(obj));
        // do something if ok cb()
        // if (err) cb(err)
        cb();
     };
     
     var notifySuccess = function () { };
     var notifyFailure = function () { };

    
    jobManager.excute();

Limitations
===========

Only a single topic can be specified.


# LICENSE - "MIT"

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions: 

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
