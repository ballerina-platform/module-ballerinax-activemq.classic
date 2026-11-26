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

import ballerina/test;

// Tests for listener services whose acknowledgements take part in Ballerina `transaction` blocks.

final MessageProducer listenerTxOutProducer = check new (BROKER_URL, transacted = true);

// Attaches `svc` to a new listener and starts it.
function startService(Service svc) returns Listener|error {
    Listener txListener = check new (BROKER_URL);
    check txListener.attach(svc);
    check txListener.'start();
    return txListener;
}

// Waits until `condition` holds and stops the listener. Stopping waits for the message in progress.
function stopWhen(Listener txListener, isolated function () returns boolean condition) returns error? {
    boolean held = waitUntil(condition);
    check txListener.gracefulStop();
    if !held {
        return error("timed out waiting for the listener");
    }
}

isolated int lstCommitDeliveries = 0;

@test:Config {
    groups: ["transaction", "listener-transaction"]
}
function testListenerAcknowledgeInBlockCommits() returns error? {
    string queueName = "tx.listener.commit.queue";
    check drainQueue(queueName);
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
    Listener txListener = check startService(svc);

    check sendToQueue(BROKER_URL, queueName, "commit-me");
    check stopWhen(txListener, isolated function () returns boolean {
        lock {
            return lstCommitDeliveries >= 1;
        }
    });

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
    Listener txListener = check startService(svc);

    check sendToQueue(BROKER_URL, queueName, "roll-me-back-once");
    check stopWhen(txListener, isolated function () returns boolean {
        lock {
            return lstRollbackDeliveries >= 2;
        }
    });

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
    Listener txListener = check startService(svc);

    check sendToQueue(BROKER_URL, queueName, "fail-once");
    check stopWhen(txListener, isolated function () returns boolean {
        lock {
            return lstFailedBlockDeliveries >= 2;
        }
    });

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
                check listenerTxOutProducer->send("processed-" + delivery.toString(),
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
    Listener txListener = check startService(svc);

    check sendToQueue(BROKER_URL, inQueue, "process-me");
    check stopWhen(txListener, isolated function () returns boolean {
        lock {
            return lstWithProducerDeliveries >= 2;
        }
    });

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
    Listener txListener = check startService(svc);

    check sendToQueue(BROKER_URL, queueName, "first");
    check sendToQueue(BROKER_URL, queueName, "second");
    check stopWhen(txListener, isolated function () returns boolean {
        lock {
            return lstEarlierPayloads.length() >= 4;
        }
    });

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
    Listener txListener = check startService(svc);

    check sendToQueue(BROKER_URL, queueName, "client-ack");
    check stopWhen(txListener, isolated function () returns boolean {
        lock {
            return lstNonTransactedError != "";
        }
    });

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
    Listener txListener = check startService(svc);

    check sendToQueue(BROKER_URL, queueName, "explicit");
    check stopWhen(txListener, isolated function () returns boolean {
        lock {
            return lstExplicitCommitError != "";
        }
    });

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
    Listener txListener = check startService(svc);

    check sendToQueue(BROKER_URL, queueName, "explicit-before-join");
    check stopWhen(txListener, isolated function () returns boolean {
        lock {
            return lstExplicitBeforeJoinError != "";
        }
    });

    lock {
        test:assertTrue(lstExplicitBeforeJoinError.includes("Cannot commit explicitly inside a transaction block"),
                "unexpected result: " + lstExplicitBeforeJoinError);
    }
    test:assertEquals(check receiveTexts(queueName), [], "the block's commit should still acknowledge it");
}

isolated string[] lstErrorPayloads = [];

@test:Config {
    groups: ["transaction", "listener-transaction"]
}
function testListenerErrorBeforeAcknowledgeRedelivers() returns error? {
    string queueName = "tx.listener.errorbeforeack.queue";
    check drainQueue(queueName);
    Service svc = @ServiceConfig {
        queueName: "tx.listener.errorbeforeack.queue",
        sessionAckMode: SESSION_TRANSACTED,
        redeliveryPolicy: {maximumRedeliveries: 5, initialRedeliveryDelay: 200, redeliveryDelay: 200}
    } service object {
        remote function onMessage(record {|*Message; string payload;|} message, Caller caller) returns error? {
            boolean isFirstDelivery;
            lock {
                isFirstDelivery = lstErrorPayloads.length() == 0;
                lstErrorPayloads.push(message.payload);
            }
            transaction {
                if isFirstDelivery {
                    // The session hasn't joined the block, so the block's rollback doesn't cover the message.
                    check failStep();
                }
                check caller->acknowledge(message);
                check commit;
            }
        }
    };
    Listener txListener = check startService(svc);

    check sendToQueue(BROKER_URL, queueName, "first");
    check sendToQueue(BROKER_URL, queueName, "second");
    check stopWhen(txListener, isolated function () returns boolean {
        lock {
            return lstErrorPayloads.length() >= 3;
        }
    });

    lock {
        test:assertEquals(lstErrorPayloads, ["first", "first", "second"],
                "a message whose onMessage failed should be redelivered, not acknowledged with the next one");
    }
    test:assertEquals(check receiveTexts(queueName), [], "both messages should be acknowledged in the end");
}

// Joins the session, then commits after onMessage has failed.
function acknowledgeAfterOnMessageFails(Message message, Caller caller) returns error? {
    transaction {
        check caller->acknowledge(message);
        setFlag("listener.async.joined");
        check waitForFlag("listener.async.failed");
        check commit;
    }
}

isolated int lstAsyncDeliveries = 0;

@test:Config {
    groups: ["transaction", "listener-transaction"]
}
function testListenerErrorWhileAnotherStrandHoldsTheBlockRedelivers() returns error? {
    string queueName = "tx.listener.async.queue";
    check drainQueue(queueName);
    Service svc = @ServiceConfig {
        queueName: "tx.listener.async.queue",
        sessionAckMode: SESSION_TRANSACTED,
        redeliveryPolicy: {maximumRedeliveries: 5, initialRedeliveryDelay: 200, redeliveryDelay: 200}
    } service object {
        remote function onMessage(Message message, Caller caller) returns error? {
            lock {
                lstAsyncDeliveries += 1;
            }
            if message.redelivered != true {
                future<error?> _ = start acknowledgeAfterOnMessageFails(message, caller);
                check waitForFlag("listener.async.joined");
                check failStep();
            }
            check caller->'commit();
        }

        // Called after the connector has recorded the failure.
        remote function onError(Error err) returns error? {
            test:assertEquals(err.message(), "simulated failure");
            setFlag("listener.async.failed");
        }
    };
    Listener txListener = check startService(svc);

    check sendToQueue(BROKER_URL, queueName, "async");
    check stopWhen(txListener, isolated function () returns boolean {
        lock {
            return lstAsyncDeliveries >= 2;
        }
    });

    lock {
        test:assertEquals(lstAsyncDeliveries, 2, "the block must not acknowledge a message whose onMessage failed");
    }
    test:assertEquals(check receiveTexts(queueName), [], "the second delivery should commit the message");
}

isolated string lstOtherMessageError = "";

@test:Config {
    groups: ["transaction", "listener-transaction"]
}
function testListenerAcknowledgeOfAnotherConsumersMessageReturnsError() returns error? {
    string queueName = "tx.listener.othermessage.queue";
    string otherQueue = "tx.listener.othermessage.other.queue";
    check drainQueue(queueName);
    check drainQueue(otherQueue);
    Service svc = @ServiceConfig {
        queueName: "tx.listener.othermessage.queue",
        sessionAckMode: SESSION_TRANSACTED
    } service object {
        remote function onMessage(Message message, Caller caller) returns error? {
            MessageConsumer other = check newTransactedConsumer("tx.listener.othermessage.other.queue");
            TextMessage? foreign = check other->receive(5000);
            Error? result = ();
            if foreign is TextMessage {
                transaction {
                    result = caller->acknowledge(foreign);
                    check commit;
                }
            }
            check other->close();
            Error? consumerResult = ();
            MessageConsumer consumer = check newTransactedConsumer("tx.listener.othermessage.other.queue");
            transaction {
                consumerResult = consumer->acknowledge(message);
                check commit;
            }
            check consumer->close();
            check caller->'commit();
            lock {
                lstOtherMessageError = (result is Error ? result.message() : "no error") + "|"
                        + (consumerResult is Error ? consumerResult.message() : "no error");
            }
        }
    };
    check sendToQueue(BROKER_URL, otherQueue, "foreign");
    Listener txListener = check startService(svc);

    check sendToQueue(BROKER_URL, queueName, "own");
    check stopWhen(txListener, isolated function () returns boolean {
        lock {
            return lstOtherMessageError != "";
        }
    });

    lock {
        test:assertTrue(lstOtherMessageError.includes("another service or consumer"),
                "unexpected result: " + lstOtherMessageError);
        test:assertTrue(lstOtherMessageError.includes("received by another consumer"),
                "unexpected result: " + lstOtherMessageError);
    }
    test:assertEquals(check receiveTexts(otherQueue), ["foreign"], "the other message should stay unacknowledged");
}

isolated int lstPanicDeliveries = 0;

@test:Config {
    groups: ["transaction", "listener-transaction"]
}
function testListenerPanicBeforeSettlingRedelivers() returns error? {
    string queueName = "tx.listener.panic.queue";
    check drainQueue(queueName);
    Service svc = @ServiceConfig {
        queueName: "tx.listener.panic.queue",
        sessionAckMode: SESSION_TRANSACTED,
        redeliveryPolicy: {maximumRedeliveries: 5, initialRedeliveryDelay: 200, redeliveryDelay: 200}
    } service object {
        remote function onMessage(Message message, Caller caller) returns error? {
            lock {
                lstPanicDeliveries += 1;
            }
            if message.redelivered != true {
                panic error("simulated panic");
            }
            check caller->'commit();
        }
    };
    Listener txListener = check startService(svc);

    check sendToQueue(BROKER_URL, queueName, "panic-once");
    check stopWhen(txListener, isolated function () returns boolean {
        lock {
            return lstPanicDeliveries >= 2;
        }
    });

    lock {
        test:assertEquals(lstPanicDeliveries, 2, "a message whose onMessage panicked should be redelivered");
    }
    test:assertEquals(check receiveTexts(queueName), [], "the second delivery should commit the message");
}

isolated string[] lstErrorAfterCommitPayloads = [];

@test:Config {
    groups: ["transaction", "listener-transaction"]
}
function testListenerErrorAfterCommitDoesNotRedeliver() returns error? {
    string queueName = "tx.listener.erroraftercommit.queue";
    check drainQueue(queueName);
    Service svc = @ServiceConfig {
        queueName: "tx.listener.erroraftercommit.queue",
        sessionAckMode: SESSION_TRANSACTED
    } service object {
        remote function onMessage(record {|*Message; string payload;|} message, Caller caller) returns error? {
            lock {
                lstErrorAfterCommitPayloads.push(message.payload);
            }
            transaction {
                check caller->acknowledge(message);
                check commit;
            }
            check failStep();
        }
    };
    Listener txListener = check startService(svc);

    // A redelivery of the first message would arrive before the marker.
    check sendToQueue(BROKER_URL, queueName, "committed");
    check sendToQueue(BROKER_URL, queueName, "marker");
    check stopWhen(txListener, isolated function () returns boolean {
        lock {
            return lstErrorAfterCommitPayloads.indexOf("marker") !is ();
        }
    });

    lock {
        test:assertEquals(lstErrorAfterCommitPayloads, ["committed", "marker"],
                "a committed message must not be redelivered");
    }
    test:assertEquals(check receiveTexts(queueName), [], "both messages should stay acknowledged");
}
