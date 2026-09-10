// Copyright (c) 2026, WSO2 LLC. (http://www.wso2.com)
//
// WSO2 LLC. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

import ballerina/lang.runtime;
import ballerina/test;

isolated int itAckAutoCount = 0;
isolated int itAckClientCount = 0;
isolated int itAckRecoverFirstCount = 0;
isolated int itAckRecoverSecondCount = 0;
isolated int itAckDupsOkCount = 0;

@test:Config {
    groups: ["integration", "ack"]
}
function testItAckAutoAcknowledge() returns error? {
    check drainQueue("it.ack.auto.queue");
    lock {
        itAckAutoCount = 0;
    }
    Listener ackAutoListener = check new (brokerUrl, username = username, password = password);
    Service autoSvc = @ServiceConfig {
        queueName: "it.ack.auto.queue",
        sessionAckMode: AUTO_ACKNOWLEDGE
    } service object {
        remote function onMessage(Message message) returns error? {
            lock {
                itAckAutoCount += 1;
            }
        }
    };
    check ackAutoListener.attach(autoSvc, "it-ack-auto-svc");
    check ackAutoListener.'start();

    MessageProducer prod = check new (brokerUrl, username = username, password = password);
    check prod->send({
        messageId: "it-ack-auto-01",
        payload: "auto-ack message".toBytes()
    }, {queueName: "it.ack.auto.queue"});
    check prod->close();

    runtime:sleep(5);

    lock {
        test:assertEquals(itAckAutoCount, 1,
            "message should be delivered exactly once with AUTO_ACKNOWLEDGE");
    }
    check ackAutoListener.gracefulStop();
}

@test:Config {
    groups: ["integration", "ack"]
}
function testItAckClientAcknowledgeExplicit() returns error? {
    check drainQueue("it.ack.client.queue");
    lock {
        itAckClientCount = 0;
    }
    Listener ackClientListener = check new (brokerUrl, username = username, password = password);
    Service clientAckSvc = @ServiceConfig {
        queueName: "it.ack.client.queue",
        sessionAckMode: CLIENT_ACKNOWLEDGE
    } service object {
        remote function onMessage(Message message, Caller caller) returns error? {
            lock {
                itAckClientCount += 1;
            }
            check caller->acknowledge(message);
        }
    };
    check ackClientListener.attach(clientAckSvc, "it-ack-client-svc");
    check ackClientListener.'start();

    MessageProducer prod = check new (brokerUrl, username = username, password = password);
    check prod->send({
        messageId: "it-ack-client-01",
        payload: "client-ack message".toBytes()
    }, {queueName: "it.ack.client.queue"});
    check prod->close();

    runtime:sleep(5);

    lock {
        test:assertEquals(itAckClientCount, 1,
            "message should be delivered exactly once after explicit acknowledge");
    }
    check ackClientListener.gracefulStop();
}

@test:Config {
    groups: ["integration", "ack"]
}
function testItAckClientAcknowledgeRecover() returns error? {
    check drainQueue("it.ack.recover.queue");
    lock { itAckRecoverFirstCount = 0; }
    lock { itAckRecoverSecondCount = 0; }

    MessageProducer prod = check new (brokerUrl, username = username, password = password);
    check prod->send({
        messageId: "it-ack-recover-01",
        payload: "recover test".toBytes()
    }, {queueName: "it.ack.recover.queue"});
    check prod->close();

    Listener listener1 = check new (brokerUrl, username = username, password = password);
    Service noAckSvc = @ServiceConfig {
        queueName: "it.ack.recover.queue",
        sessionAckMode: CLIENT_ACKNOWLEDGE
    } service object {
        remote function onMessage(Message message, Caller caller) returns error? {
            lock {
                itAckRecoverFirstCount += 1;
            }
        }
    };
    check listener1.attach(noAckSvc, "it-ack-no-ack-svc");
    check listener1.'start();
    runtime:sleep(5);

    lock {
        test:assertEquals(itAckRecoverFirstCount, 1,
            "listener 1 should receive the message");
    }

    check listener1.immediateStop();
    runtime:sleep(3);

    Listener listener2 = check new (brokerUrl, username = username, password = password);
    Service redeliverSvc = @ServiceConfig {
        queueName: "it.ack.recover.queue",
        sessionAckMode: CLIENT_ACKNOWLEDGE
    } service object {
        remote function onMessage(Message message, Caller caller) returns error? {
            lock {
                itAckRecoverSecondCount += 1;
            }
            check caller->acknowledge(message);
        }
    };
    check listener2.attach(redeliverSvc, "it-ack-redeliver-svc");
    check listener2.'start();
    runtime:sleep(6);

    lock {
        test:assertTrue(itAckRecoverSecondCount >= 1,
            "message should be redelivered to listener 2 after listener 1 closed without acking");
    }
    check listener2.gracefulStop();
}

@test:Config {
    groups: ["integration", "ack"]
}
function testItAckDupsOkAcknowledge() returns error? {
    check drainQueue("it.ack.dups.queue");
    lock {
        itAckDupsOkCount = 0;
    }
    Listener dupsOkListener = check new (brokerUrl, username = username, password = password);
    Service dupsOkSvc = @ServiceConfig {
        queueName: "it.ack.dups.queue",
        sessionAckMode: DUPS_OK_ACKNOWLEDGE
    } service object {
        remote function onMessage(Message message) returns error? {
            lock {
                itAckDupsOkCount += 1;
            }
        }
    };
    check dupsOkListener.attach(dupsOkSvc, "it-ack-dups-svc");
    check dupsOkListener.'start();

    MessageProducer prod = check new (brokerUrl, username = username, password = password);
    check prod->send({
        messageId: "it-ack-dups-01",
        payload: "dups-ok message".toBytes()
    }, {queueName: "it.ack.dups.queue"});
    check prod->close();

    runtime:sleep(5);

    lock {
        test:assertTrue(itAckDupsOkCount >= 1,
            "message should be received with DUPS_OK_ACKNOWLEDGE without error");
    }
    check dupsOkListener.gracefulStop();
}
