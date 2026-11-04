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

isolated int itAckClientBindFailGoodCount = 0;
isolated int itAckClientBindFailErrorCount = 0;
isolated int itAckTxBindFailGoodCount = 0;
isolated int itAckTxBindFailErrorCount = 0;

// A message that fails data binding never reaches onMessage, so the service has no Caller to roll it
// back with. The connector hands it back instead, so it is redelivered and dead-lettered like in
// AUTO_ACKNOWLEDGE rather than being removed by the next message's acknowledge or commit.
@test:Config {
    groups: ["integration", "ack"]
}
function testItAckClientAcknowledgeBindingFailureIsDeadLettered() returns error? {
    string queueName = "it.ack.client.bindfail.queue";
    string testTag = "it-ack-client-bindfail";
    check drainQueue(queueName);
    check drainDeadLetters(testTag);
    lock { itAckClientBindFailGoodCount = 0; }
    lock { itAckClientBindFailErrorCount = 0; }

    Listener bindFailListener = check new (brokerUrl, username = username, password = password);
    Service clientAckSvc = @ServiceConfig {
        queueName: "it.ack.client.bindfail.queue",
        sessionAckMode: CLIENT_ACKNOWLEDGE,
        redeliveryPolicy: {maximumRedeliveries: 2, initialRedeliveryDelay: 500, redeliveryDelay: 500}
    } service object {
        remote function onMessage(record {|*Message; string payload;|} message, Caller caller) returns error? {
            lock { itAckClientBindFailGoodCount += 1; }
            check caller->acknowledge(message);
        }

        remote function onError(Error err) returns error? {
            lock { itAckClientBindFailErrorCount += 1; }
        }
    };
    check bindFailListener.attach(clientAckSvc, "it-ack-client-bindfail-svc");
    check bindFailListener.'start();

    MessageProducer prod = check new (brokerUrl, username = username, password = password);
    check prod->send({payload: "binding fails".toBytes(), properties: {"testTag": testTag}}, {queueName});
    check prod->send({payload: "binding succeeds"}, {queueName});
    check prod->close();

    runtime:sleep(6);
    check bindFailListener.gracefulStop();

    lock {
        test:assertEquals(itAckClientBindFailGoodCount, 1, "the bindable message should reach onMessage once");
    }
    lock {
        test:assertEquals(itAckClientBindFailErrorCount, 3,
            "the failed message should be delivered 1 + maximumRedeliveries times");
    }
    test:assertTrue(check isDeadLettered(testTag),
        "a message that failed data binding should be moved to ActiveMQ.DLQ, not acknowledged");
}

@test:Config {
    groups: ["integration", "ack"]
}
function testItAckSessionTransactedBindingFailureIsDeadLettered() returns error? {
    string queueName = "it.ack.tx.bindfail.queue";
    string testTag = "it-ack-tx-bindfail";
    check drainQueue(queueName);
    check drainDeadLetters(testTag);
    lock { itAckTxBindFailGoodCount = 0; }
    lock { itAckTxBindFailErrorCount = 0; }

    Listener bindFailListener = check new (brokerUrl, username = username, password = password);
    Service transactedSvc = @ServiceConfig {
        queueName: "it.ack.tx.bindfail.queue",
        sessionAckMode: SESSION_TRANSACTED,
        redeliveryPolicy: {maximumRedeliveries: 2, initialRedeliveryDelay: 500, redeliveryDelay: 500}
    } service object {
        remote function onMessage(record {|*Message; string payload;|} message, Caller caller) returns error? {
            lock { itAckTxBindFailGoodCount += 1; }
            check caller->'commit();
        }

        remote function onError(Error err) returns error? {
            lock { itAckTxBindFailErrorCount += 1; }
        }
    };
    check bindFailListener.attach(transactedSvc, "it-ack-tx-bindfail-svc");
    check bindFailListener.'start();

    MessageProducer prod = check new (brokerUrl, username = username, password = password);
    check prod->send({payload: "binding fails".toBytes(), properties: {"testTag": testTag}}, {queueName});
    check prod->send({payload: "binding succeeds"}, {queueName});
    check prod->close();

    runtime:sleep(6);
    check bindFailListener.gracefulStop();

    lock {
        test:assertEquals(itAckTxBindFailGoodCount, 1, "the bindable message should reach onMessage once");
    }
    lock {
        test:assertEquals(itAckTxBindFailErrorCount, 3,
            "the failed message should be delivered 1 + maximumRedeliveries times");
    }
    test:assertTrue(check isDeadLettered(testTag),
        "a message that failed data binding should be moved to ActiveMQ.DLQ, not committed");
}

isolated function drainDeadLetters(string testTag) returns error? {
    MessageConsumer drainer = check new (brokerUrl, username = username, password = password,
        destination = {queueName: "ActiveMQ.DLQ"}, messageSelector = string `testTag = '${testTag}'`);
    error? drainError = ();
    do {
        Message? msg = check drainer->receive(1000);
        while msg is Message {
            msg = check drainer->receive(500);
        }
    } on fail error e {
        drainError = e;
    }
    check drainer->close();
    if drainError is error {
        return drainError;
    }
}

isolated function isDeadLettered(string testTag) returns boolean|error {
    MessageConsumer dlqConsumer = check new (brokerUrl, username = username, password = password,
        destination = {queueName: "ActiveMQ.DLQ"}, messageSelector = string `testTag = '${testTag}'`);
    Message|Error? deadLettered = dlqConsumer->receive(2000);
    check dlqConsumer->close();
    return deadLettered is Message;
}
