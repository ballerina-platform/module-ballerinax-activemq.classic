// Copyright (c) 2026, WSO2 LLC. (http://www.wso2.com)
//
// WSO2 LLC. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import ballerina/lang.runtime;
import ballerina/test;

// Tests for listener services whose acknowledgements take part in Ballerina `transaction` blocks.

final MessageProducer listenerTxOutProducer = check new (BROKER_URL, transacted = true);

// Polls `condition` until it holds or the timeout passes.
isolated function waitUntil(isolated function () returns boolean condition, decimal timeoutSeconds = 15) {
    decimal waited = 0;
    while waited < timeoutSeconds && !condition() {
        runtime:sleep(0.25d);
        waited += 0.25d;
    }
}

isolated int lstCommitDeliveries = 0;

@test:Config {
    groups: ["transaction", "listener-transaction"]
}
function testListenerAcknowledgeInBlockCommits() returns error? {
    string queueName = "tx.listener.commit.queue";
    check drainQueue(queueName);
    Listener txListener = check new (BROKER_URL);
    Service svc = @ServiceConfig {
        queueName: "tx.listener.commit.queue",
        sessionAckMode: SESSION_TRANSACTED
    } service object {
        remote function onMessage(Message message, Caller caller) returns error? {
            transaction {
                check caller->acknowledge(message);
                check commit;
            }
            lock {
                lstCommitDeliveries += 1;
            }
        }
    };
    check txListener.attach(svc);
    check txListener.'start();

    check sendToQueue(BROKER_URL, queueName, "commit-me");
    waitUntil(isolated function () returns boolean {
        lock {
            return lstCommitDeliveries >= 1;
        }
    });
    runtime:sleep(1);
    check txListener.gracefulStop();

    lock {
        test:assertEquals(lstCommitDeliveries, 1, "the committed message should be delivered once");
    }
    test:assertEquals(check receiveTexts(queueName), [], "the block's commit should acknowledge the message");
}

isolated int lstRollbackDeliveries = 0;

@test:Config {
    groups: ["transaction", "listener-transaction"]
}
function testListenerRollbackInBlockRedelivers() returns error? {
    string queueName = "tx.listener.rollback.queue";
    check drainQueue(queueName);
    Listener txListener = check new (BROKER_URL);
    Service svc = @ServiceConfig {
        queueName: "tx.listener.rollback.queue",
        sessionAckMode: SESSION_TRANSACTED,
        redeliveryPolicy: {maximumRedeliveries: 5, initialRedeliveryDelay: 200, redeliveryDelay: 200}
    } service object {
        remote function onMessage(Message message, Caller caller) returns error? {
            int delivery;
            lock {
                lstRollbackDeliveries += 1;
                delivery = lstRollbackDeliveries;
            }
            transaction {
                check caller->acknowledge(message);
                if delivery == 1 {
                    rollback;
                } else {
                    check commit;
                }
            }
        }
    };
    check txListener.attach(svc);
    check txListener.'start();

    check sendToQueue(BROKER_URL, queueName, "roll-me-back-once");
    waitUntil(isolated function () returns boolean {
        lock {
            return lstRollbackDeliveries >= 2;
        }
    });
    runtime:sleep(1);
    check txListener.gracefulStop();

    lock {
        test:assertEquals(lstRollbackDeliveries, 2,
                "the rolled-back message should be redelivered once and then committed");
    }
    test:assertEquals(check receiveTexts(queueName), [], "the second attempt should acknowledge the message");
}

isolated int lstFailedBlockDeliveries = 0;

@test:Config {
    groups: ["transaction", "listener-transaction"]
}
function testListenerFailedBlockRedelivers() returns error? {
    string queueName = "tx.listener.failedblock.queue";
    check drainQueue(queueName);
    Listener txListener = check new (BROKER_URL);
    Service svc = @ServiceConfig {
        queueName: "tx.listener.failedblock.queue",
        sessionAckMode: SESSION_TRANSACTED,
        redeliveryPolicy: {maximumRedeliveries: 5, initialRedeliveryDelay: 200, redeliveryDelay: 200}
    } service object {
        remote function onMessage(Message message, Caller caller) returns error? {
            int delivery;
            lock {
                lstFailedBlockDeliveries += 1;
                delivery = lstFailedBlockDeliveries;
            }
            do {
                transaction {
                    check caller->acknowledge(message);
                    if delivery == 1 {
                        check failStep();
                    }
                    check commit;
                }
            } on fail {
                // The failed block has already rolled back the session.
            }
        }
    };
    check txListener.attach(svc);
    check txListener.'start();

    check sendToQueue(BROKER_URL, queueName, "fail-once");
    waitUntil(isolated function () returns boolean {
        lock {
            return lstFailedBlockDeliveries >= 2;
        }
    });
    runtime:sleep(1);
    check txListener.gracefulStop();

    lock {
        test:assertEquals(lstFailedBlockDeliveries, 2, "a failed block should have the message redelivered");
    }
    test:assertEquals(check receiveTexts(queueName), [], "the second attempt should acknowledge the message");
}

isolated int lstWithProducerDeliveries = 0;

@test:Config {
    groups: ["transaction", "listener-transaction"]
}
function testListenerAcknowledgeAndProducerSendCommitTogether() returns error? {
    string inQueue = "tx.listener.withproducer.in.queue";
    string outQueue = "tx.listener.withproducer.out.queue";
    check drainQueue(inQueue);
    check drainQueue(outQueue);
    Listener txListener = check new (BROKER_URL);
    Service svc = @ServiceConfig {
        queueName: "tx.listener.withproducer.in.queue",
        sessionAckMode: SESSION_TRANSACTED,
        redeliveryPolicy: {maximumRedeliveries: 5, initialRedeliveryDelay: 200, redeliveryDelay: 200}
    } service object {
        remote function onMessage(Message message, Caller caller) returns error? {
            int delivery;
            lock {
                lstWithProducerDeliveries += 1;
                delivery = lstWithProducerDeliveries;
            }
            transaction {
                check listenerTxOutProducer->send({payload: "processed-" + delivery.toString()},
                        {queueName: "tx.listener.withproducer.out.queue"});
                check caller->acknowledge(message);
                if delivery == 1 {
                    rollback;
                } else {
                    check commit;
                }
            }
        }
    };
    check txListener.attach(svc);
    check txListener.'start();

    check sendToQueue(BROKER_URL, inQueue, "process-me");
    waitUntil(isolated function () returns boolean {
        lock {
            return lstWithProducerDeliveries >= 2;
        }
    });
    runtime:sleep(1);
    check txListener.gracefulStop();

    test:assertEquals(check receiveTexts(outQueue), ["processed-2"],
            "only the committed attempt's send should be delivered");
    test:assertEquals(check receiveTexts(inQueue), [], "the committed attempt should acknowledge the input");
}

isolated string[] lstEarlierPayloads = [];

@test:Config {
    groups: ["transaction", "listener-transaction"]
}
function testListenerRollbackRedeliversMessagesReceivedBeforeBlock() returns error? {
    string queueName = "tx.listener.earlier.queue";
    check drainQueue(queueName);
    Listener txListener = check new (BROKER_URL);
    Service svc = @ServiceConfig {
        queueName: "tx.listener.earlier.queue",
        sessionAckMode: SESSION_TRANSACTED,
        redeliveryPolicy: {maximumRedeliveries: 5, initialRedeliveryDelay: 200, redeliveryDelay: 200}
    } service object {
        remote function onMessage(record {|*Message; string payload;|} message, Caller caller) returns error? {
            int secondDeliveries;
            lock {
                lstEarlierPayloads.push(message.payload);
                secondDeliveries = lstEarlierPayloads.filter(p => p == "second").length();
            }
            if message.payload == "first" {
                // Leave it unsettled: it stays in the session's open JMS transaction.
                return;
            }
            transaction {
                check caller->acknowledge(message);
                if secondDeliveries == 1 {
                    rollback;
                } else {
                    check commit;
                }
            }
        }
    };
    check txListener.attach(svc);
    check txListener.'start();

    check sendToQueue(BROKER_URL, queueName, "first");
    check sendToQueue(BROKER_URL, queueName, "second");
    waitUntil(isolated function () returns boolean {
        lock {
            return lstEarlierPayloads.length() >= 4;
        }
    });
    runtime:sleep(1);
    check txListener.gracefulStop();

    lock {
        test:assertEquals(lstEarlierPayloads, ["first", "second", "first", "second"],
                "rolling back the block should also redeliver the message received before it");
    }
    test:assertEquals(check receiveTexts(queueName), [],
            "the committed block should acknowledge both messages, including the one received before it");
}

isolated string lstNonTransactedError = "";

@test:Config {
    groups: ["transaction", "listener-transaction"]
}
function testListenerNonTransactedAcknowledgeInBlockReturnsError() returns error? {
    string queueName = "tx.listener.nontransacted.queue";
    check drainQueue(queueName);
    Listener txListener = check new (BROKER_URL);
    Service svc = @ServiceConfig {
        queueName: "tx.listener.nontransacted.queue",
        sessionAckMode: CLIENT_ACKNOWLEDGE
    } service object {
        remote function onMessage(Message message, Caller caller) returns error? {
            Error? inBlock = ();
            transaction {
                inBlock = caller->acknowledge(message);
                check commit;
            }
            lock {
                lstNonTransactedError = inBlock is Error ? inBlock.message() : "no error";
            }
            // Outside the block, acknowledging works as before.
            check caller->acknowledge(message);
        }
    };
    check txListener.attach(svc);
    check txListener.'start();

    check sendToQueue(BROKER_URL, queueName, "client-ack");
    waitUntil(isolated function () returns boolean {
        lock {
            return lstNonTransactedError != "";
        }
    });
    runtime:sleep(1);
    check txListener.gracefulStop();

    lock {
        test:assertTrue(lstNonTransactedError.includes("isn't transacted"),
                "unexpected result: " + lstNonTransactedError);
    }
    test:assertEquals(check receiveTexts(queueName), [], "acknowledging outside the block should settle it");
}

isolated string lstExplicitCommitError = "";

@test:Config {
    groups: ["transaction", "listener-transaction"]
}
function testListenerExplicitCommitInsideBlockReturnsError() returns error? {
    string queueName = "tx.listener.explicit.queue";
    check drainQueue(queueName);
    Listener txListener = check new (BROKER_URL);
    Service svc = @ServiceConfig {
        queueName: "tx.listener.explicit.queue",
        sessionAckMode: SESSION_TRANSACTED
    } service object {
        remote function onMessage(Message message, Caller caller) returns error? {
            Error? explicitCommit = ();
            transaction {
                check caller->acknowledge(message);
                explicitCommit = caller->'commit();
                check commit;
            }
            lock {
                lstExplicitCommitError = explicitCommit is Error ? explicitCommit.message() : "no error";
            }
        }
    };
    check txListener.attach(svc);
    check txListener.'start();

    check sendToQueue(BROKER_URL, queueName, "explicit");
    waitUntil(isolated function () returns boolean {
        lock {
            return lstExplicitCommitError != "";
        }
    });
    runtime:sleep(1);
    check txListener.gracefulStop();

    lock {
        test:assertTrue(lstExplicitCommitError.includes("Cannot commit explicitly"),
                "unexpected result: " + lstExplicitCommitError);
    }
    test:assertEquals(check receiveTexts(queueName), [], "the block's commit should still acknowledge it");
}

isolated string lstExplicitBeforeJoinError = "";

@test:Config {
    groups: ["transaction", "listener-transaction"]
}
function testListenerExplicitCommitBeforeJoiningReturnsError() returns error? {
    string queueName = "tx.listener.explicitbeforejoin.queue";
    check drainQueue(queueName);
    Listener txListener = check new (BROKER_URL);
    Service svc = @ServiceConfig {
        queueName: "tx.listener.explicitbeforejoin.queue",
        sessionAckMode: SESSION_TRANSACTED
    } service object {
        remote function onMessage(Message message, Caller caller) returns error? {
            Error? explicitCommit = ();
            transaction {
                // The session hasn't joined the block yet, but the explicit commit is still refused.
                explicitCommit = caller->'commit();
                check caller->acknowledge(message);
                check commit;
            }
            lock {
                lstExplicitBeforeJoinError = explicitCommit is Error ? explicitCommit.message() : "no error";
            }
        }
    };
    check txListener.attach(svc);
    check txListener.'start();

    check sendToQueue(BROKER_URL, queueName, "explicit-before-join");
    waitUntil(isolated function () returns boolean {
        lock {
            return lstExplicitBeforeJoinError != "";
        }
    });
    runtime:sleep(1);
    check txListener.gracefulStop();

    lock {
        test:assertTrue(lstExplicitBeforeJoinError.includes("Cannot commit explicitly inside a transaction block"),
                "unexpected result: " + lstExplicitBeforeJoinError);
    }
    test:assertEquals(check receiveTexts(queueName), [], "the block's commit should still acknowledge it");
}
