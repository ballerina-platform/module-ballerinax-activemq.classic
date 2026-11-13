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

isolated function waitForCounts(isolated function () returns [int, int] readCounts,
        int expectedGood, int expectedError, decimal timeoutSeconds = 15) {
    decimal waited = 0;
    while waited < timeoutSeconds {
        [int, int] [good, errs] = readCounts();
        if good >= expectedGood && errs >= expectedError {
            return;
        }
        runtime:sleep(0.25d);
        waited += 0.25d;
    }
}

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
            if err.message().startsWith("Data binding failed") {
                lock { itAckClientBindFailErrorCount += 1; }
            }
        }
    };
    check bindFailListener.attach(clientAckSvc, "it-ack-client-bindfail-svc");
    check bindFailListener.'start();

    MessageProducer prod = check new (brokerUrl, username = username, password = password);
    check prod->send({payload: "binding fails".toBytes(), properties: {"testTag": testTag}}, {queueName});
    check prod->send({payload: "binding succeeds"}, {queueName});
    check prod->close();

    waitForCounts(isolated function () returns [int, int] {
        int good;
        int errs;
        lock { good = itAckClientBindFailGoodCount; }
        lock { errs = itAckClientBindFailErrorCount; }
        return [good, errs];
    }, 1, 3);
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
            if message.payload == "binding succeeds" {
                lock { itAckTxBindFailGoodCount += 1; }
            }
            check caller->'commit();
        }

        remote function onError(Error err) returns error? {
            if err.message().startsWith("Data binding failed") {
                lock { itAckTxBindFailErrorCount += 1; }
            }
        }
    };
    check bindFailListener.attach(transactedSvc, "it-ack-tx-bindfail-svc");
    check bindFailListener.'start();

    MessageProducer prod = check new (brokerUrl, username = username, password = password);
    check prod->send({payload: "binding fails".toBytes(), properties: {"testTag": testTag}}, {queueName});
    check prod->send({payload: "binding succeeds"}, {queueName});
    check prod->close();

    waitForCounts(isolated function () returns [int, int] {
        int good;
        int errs;
        lock { good = itAckTxBindFailGoodCount; }
        lock { errs = itAckTxBindFailErrorCount; }
        return [good, errs];
    }, 1, 3);
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

isolated int itAckPoisonErrorCount = 0;
isolated int itAckPoisonGoodCount = 0;

// ActiveMQ records the failure, including the consumer's redelivery policy, on a dead-lettered message as
// dlqDeliveryFailureCause. Matching both the policy and the class picks out this test's poison message.
const string POISON_DLQ_SELECTOR =
    "dlqDeliveryFailureCause LIKE '%maximumRedeliveries = 1,%Forbidden class java.util.ArrayList%'";

// When a message runs out of redeliveries, ActiveMQ sends the broker a poison ack carrying the failure. The
// broker must accept it even when the failure came from an ObjectMessage of an untrusted class, so the
// listener's connection survives and later messages are still delivered.
@test:Config {
    groups: ["integration", "ack"]
}
function testItAckUnreadableObjectMessageKeepsListenerConnection() returns error? {
    string queueName = "it.ack.poison.objmsg.queue";
    check drainQueue(queueName);
    while check takePoisonDeadLetter(500) {
    }
    lock { itAckPoisonErrorCount = 0; }
    lock { itAckPoisonGoodCount = 0; }

    Listener poisonListener = check new (brokerUrl, username = username, password = password);
    Service poisonSvc = @ServiceConfig {
        queueName: "it.ack.poison.objmsg.queue",
        redeliveryPolicy: {maximumRedeliveries: 1, initialRedeliveryDelay: 200, redeliveryDelay: 200}
    } service object {
        remote function onMessage(Message message) returns error? {
            lock { itAckPoisonGoodCount += 1; }
        }

        remote function onError(Error err) returns error? {
            lock { itAckPoisonErrorCount += 1; }
        }
    };
    check poisonListener.attach(poisonSvc, "it-ack-poison-objmsg-svc");
    check poisonListener.'start();

    check sendObjectMessageToQueue(brokerUrl, queueName, "unreadable-payload");
    waitForCounts(isolated function () returns [int, int] {
        int good;
        int errs;
        lock { good = itAckPoisonGoodCount; }
        lock { errs = itAckPoisonErrorCount; }
        return [good, errs];
    }, 0, 2);
    // The message reaches the DLQ only once the broker has processed the poison ack.
    test:assertTrue(check takePoisonDeadLetter(10000),
        "the unreadable message should be moved to ActiveMQ.DLQ once its redeliveries run out");

    MessageProducer prod = check new (brokerUrl, username = username, password = password);
    check prod->send({payload: "delivered after the poison ack"}, {queueName});
    check prod->close();

    waitForCounts(isolated function () returns [int, int] {
        int good;
        int errs;
        lock { good = itAckPoisonGoodCount; }
        lock { errs = itAckPoisonErrorCount; }
        return [good, errs];
    }, 1, 2);
    check poisonListener.gracefulStop();

    lock {
        test:assertEquals(itAckPoisonErrorCount, 2,
            "the unreadable message should fail 1 + maximumRedeliveries times");
    }
    lock {
        test:assertEquals(itAckPoisonGoodCount, 1,
            "the listener's connection should survive the poison ack and deliver the next message");
    }
}

isolated int itAckAutoReturnedErrorCount = 0;
isolated int itAckDupsReturnedErrorCount = 0;

// In AUTO_ACKNOWLEDGE and DUPS_OK_ACKNOWLEDGE, JMS acknowledges a message once onMessage returns. When
// onMessage returns an error, the connector hands the message back instead, so it is redelivered and
// dead-lettered rather than lost.
@test:Config {
    groups: ["integration", "ack"]
}
function testItAckAutoAcknowledgeReturnedErrorIsDeadLettered() returns error? {
    string queueName = "it.ack.auto.returnederror.queue";
    string testTag = "it-ack-auto-returnederror";
    check drainQueue(queueName);
    check drainDeadLetters(testTag);
    lock { itAckAutoReturnedErrorCount = 0; }

    Listener returnedErrorListener = check new (brokerUrl, username = username, password = password);
    Service autoSvc = @ServiceConfig {
        queueName: "it.ack.auto.returnederror.queue",
        sessionAckMode: AUTO_ACKNOWLEDGE,
        redeliveryPolicy: {maximumRedeliveries: 2, initialRedeliveryDelay: 500, redeliveryDelay: 500}
    } service object {
        remote function onMessage(Message message) returns error? {
            lock { itAckAutoReturnedErrorCount += 1; }
            return error("intentional business failure");
        }

        remote function onError(Error err) returns error? {
        }
    };
    check returnedErrorListener.attach(autoSvc, "it-ack-auto-returnederror-svc");
    check returnedErrorListener.'start();

    MessageProducer prod = check new (brokerUrl, username = username, password = password);
    check prod->send({payload: "always fails", properties: {"testTag": testTag}}, {queueName});
    check prod->close();

    waitForCounts(isolated function () returns [int, int] {
        int attempts;
        lock { attempts = itAckAutoReturnedErrorCount; }
        return [attempts, 0];
    }, 3, 0);
    check returnedErrorListener.gracefulStop();

    lock {
        test:assertEquals(itAckAutoReturnedErrorCount, 3,
            "a message whose onMessage returns an error should be delivered 1 + maximumRedeliveries times");
    }
    test:assertTrue(check isDeadLettered(testTag, 10000),
        "a message whose onMessage keeps returning an error should be moved to ActiveMQ.DLQ, not acknowledged");
}

@test:Config {
    groups: ["integration", "ack"]
}
function testItAckDupsOkReturnedErrorIsDeadLettered() returns error? {
    string queueName = "it.ack.dups.returnederror.queue";
    string testTag = "it-ack-dups-returnederror";
    check drainQueue(queueName);
    check drainDeadLetters(testTag);
    lock { itAckDupsReturnedErrorCount = 0; }

    Listener returnedErrorListener = check new (brokerUrl, username = username, password = password);
    Service dupsSvc = @ServiceConfig {
        queueName: "it.ack.dups.returnederror.queue",
        sessionAckMode: DUPS_OK_ACKNOWLEDGE,
        redeliveryPolicy: {maximumRedeliveries: 2, initialRedeliveryDelay: 500, redeliveryDelay: 500}
    } service object {
        remote function onMessage(Message message) returns error? {
            lock { itAckDupsReturnedErrorCount += 1; }
            return error("intentional business failure");
        }
    };
    check returnedErrorListener.attach(dupsSvc, "it-ack-dups-returnederror-svc");
    check returnedErrorListener.'start();

    MessageProducer prod = check new (brokerUrl, username = username, password = password);
    check prod->send({payload: "always fails", properties: {"testTag": testTag}}, {queueName});
    check prod->close();

    waitForCounts(isolated function () returns [int, int] {
        int attempts;
        lock { attempts = itAckDupsReturnedErrorCount; }
        return [attempts, 0];
    }, 3, 0);
    check returnedErrorListener.gracefulStop();

    lock {
        test:assertEquals(itAckDupsReturnedErrorCount, 3,
            "a message whose onMessage returns an error should be delivered 1 + maximumRedeliveries times");
    }
    test:assertTrue(check isDeadLettered(testTag, 10000),
        "a message whose onMessage keeps returning an error should be moved to ActiveMQ.DLQ, not acknowledged");
}

// Takes the poison test's dead-lettered message, waiting up to timeoutMs. The connector can't read the
// ObjectMessage, so receive returns an error for it, but the message is still consumed. Only that error
// counts as the message arriving; any other receive error is returned so it isn't mistaken for one.
isolated function takePoisonDeadLetter(int timeoutMs) returns boolean|error {
    MessageConsumer dlqConsumer = check new (brokerUrl, username = username, password = password,
        destination = {queueName: "ActiveMQ.DLQ"}, messageSelector = POISON_DLQ_SELECTOR);
    Message|Error? deadLettered = dlqConsumer->receive(timeoutMs);
    check dlqConsumer->close();
    if deadLettered is Error {
        if deadLettered.message().includes("Forbidden class java.util.ArrayList") {
            return true;
        }
        return deadLettered;
    }
    return false;
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

isolated function isDeadLettered(string testTag, int timeoutMs = 2000) returns boolean|error {
    MessageConsumer dlqConsumer = check new (brokerUrl, username = username, password = password,
        destination = {queueName: "ActiveMQ.DLQ"}, messageSelector = string `testTag = '${testTag}'`);
    Message|Error? deadLettered = dlqConsumer->receive(timeoutMs);
    check dlqConsumer->close();
    return deadLettered is Message;
}
