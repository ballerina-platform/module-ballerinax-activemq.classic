// Copyright (c) 2025, WSO2 LLC. (http://www.wso2.org).
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

// Dedicated listener for client integration tests that need a subscriber (topic tests).
listener Listener clientTestListener = check new Listener(BROKER_URL);

// ─────────────────────────────────────────────────────────────────────────────
// Test 1: Basic send and receive on a queue
// ─────────────────────────────────────────────────────────────────────────────

@test:Config {
    groups: ["client"]
}
isolated function testClientSendAndReceiveFromQueue() returns error? {
    Client mqClient = check new (BROKER_URL);
    check mqClient->sendMessage("client.test.basic.queue", {
        messageId: "basic-1",
        payload: "Hello ActiveMQ".toBytes()
    });
    Message? received = check mqClient->receiveMessage("client.test.basic.queue", 5000);
    check mqClient->close();
    test:assertTrue(received is Message, "should receive the sent message");
    if received is Message {
        string content = check string:fromBytes(received.payload);
        test:assertEquals(content, "Hello ActiveMQ", "payload content should match");
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 2: receiveMessage returns () when queue is empty within timeout
// ─────────────────────────────────────────────────────────────────────────────

@test:Config {
    groups: ["client"]
}
isolated function testClientReceiveReturnsNilOnTimeout() returns error? {
    Client mqClient = check new (BROKER_URL);
    Message? received = check mqClient->receiveMessage("client.test.empty.queue", 1000);
    check mqClient->close();
    test:assertTrue(received is (), "should return nil when no message arrives in timeout");
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 3: Multiple messages are received in the order they were sent
// ─────────────────────────────────────────────────────────────────────────────

@test:Config {
    groups: ["client"]
}
isolated function testClientMultipleMessages() returns error? {
    Client mqClient = check new (BROKER_URL);
    string[] payloads = ["First", "Second", "Third"];
    foreach string p in payloads {
        check mqClient->sendMessage("client.test.multi.queue", {
            messageId: p,
            payload: p.toBytes()
        });
    }
    string[] received = [];
    Message? msg = check mqClient->receiveMessage("client.test.multi.queue", 3000);
    while msg is Message {
        received.push(check string:fromBytes(msg.payload));
        msg = check mqClient->receiveMessage("client.test.multi.queue", 2000);
    }
    check mqClient->close();
    test:assertEquals(received.length(), 3, "should receive all 3 sent messages");
    test:assertEquals(received, payloads, "messages should arrive in send order");
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 4: JMS headers and custom properties survive the send/receive roundtrip
// ─────────────────────────────────────────────────────────────────────────────

@test:Config {
    groups: ["client"]
}
isolated function testClientMessageFieldsRoundtrip() returns error? {
    Client mqClient = check new (BROKER_URL);
    check mqClient->sendMessage("client.test.fields.queue", {
        messageId: "fields-1",
        payload: "Roundtrip payload".toBytes(),
        correlationId: "corr-abc-123",
        'type: "TestOrder",
        persistent: true,
        priority: 7,
        properties: {
            "category": "electronics",
            "region": "APAC"
        }
    });
    Message? received = check mqClient->receiveMessage("client.test.fields.queue", 5000);
    check mqClient->close();
    test:assertTrue(received is Message, "should receive message");
    if received is Message {
        test:assertEquals(received.correlationId, "corr-abc-123", "correlationId should roundtrip");
        test:assertEquals(received.'type, "TestOrder", "type should roundtrip");
        boolean? persistent = received.persistent;
        test:assertTrue(persistent is boolean && persistent == true, "persistent should be true");
        int? priority = received.priority;
        test:assertTrue(priority is int && priority >= 7, "priority should be preserved");
        map<anydata>? props = received.properties;
        test:assertTrue(props is map<anydata>, "custom properties should be present");
        if props is map<anydata> {
            test:assertEquals(props["category"], "electronics");
            test:assertEquals(props["region"], "APAC");
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 5: Persistent and non-persistent delivery modes are reflected in received
//         messages
// ─────────────────────────────────────────────────────────────────────────────

@test:Config {
    groups: ["client"]
}
isolated function testClientPersistenceField() returns error? {
    Client mqClient = check new (BROKER_URL);
    check mqClient->sendMessage("client.test.persist.queue", {
        messageId: "p-1",
        payload: "persistent".toBytes(),
        persistent: true
    });
    check mqClient->sendMessage("client.test.nonpersist.queue", {
        messageId: "np-1",
        payload: "non-persistent".toBytes(),
        persistent: false
    });
    Message? pMsg = check mqClient->receiveMessage("client.test.persist.queue", 3000);
    Message? npMsg = check mqClient->receiveMessage("client.test.nonpersist.queue", 3000);
    check mqClient->close();
    test:assertTrue(pMsg is Message, "persistent message should be received");
    test:assertTrue(npMsg is Message, "non-persistent message should be received");
    if pMsg is Message {
        test:assertTrue(pMsg.persistent == true, "persistent field should be true");
    }
    if npMsg is Message {
        test:assertTrue(npMsg.persistent == false, "persistent field should be false");
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 6: replyTo header is preserved and returned as a destination string
// ─────────────────────────────────────────────────────────────────────────────

@test:Config {
    groups: ["client"]
}
isolated function testClientReplyToField() returns error? {
    Client mqClient = check new (BROKER_URL);
    check mqClient->sendMessage("client.test.replyto.queue", {
        messageId: "rr-1",
        payload: "Request".toBytes(),
        replyTo: "client.test.reply.queue"
    });
    Message? received = check mqClient->receiveMessage("client.test.replyto.queue", 5000);
    check mqClient->close();
    test:assertTrue(received is Message, "should receive message with replyTo set");
    if received is Message {
        string? replyTo = received.replyTo;
        test:assertTrue(replyTo is string, "replyTo should be present");
        // ActiveMQ serialises queue destinations as "queue://name"
        test:assertTrue((<string>replyTo).includes("client.test.reply.queue"),
            "replyTo should contain the original queue name");
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 7: Client can publish to a JMS topic; a Listener service receives it
// ─────────────────────────────────────────────────────────────────────────────

isolated int clientTopicReceivedCount = 0;

@test:Config {
    groups: ["client", "topics"]
}
isolated function testClientSendToTopic() returns error? {
    Service topicSvc = @ServiceConfig {
        topicName: "client.test.topic",
        pollingInterval: 1,
        receiveTimeout: 1
    } service object {
        remote function onMessage(Message message) returns error? {
            lock {
                clientTopicReceivedCount += 1;
            }
        }
    };
    check clientTestListener.attach(topicSvc, "client-topic-svc");
    // Allow the subscriber to fully register before the producer sends.
    runtime:sleep(2);

    Client mqClient = check new (BROKER_URL);
    check mqClient->sendMessage("topic://client.test.topic", {
        messageId: "topic-msg-1",
        payload: "Topic message from Client".toBytes()
    });
    check mqClient->close();

    runtime:sleep(2);
    lock {
        test:assertEquals(clientTopicReceivedCount, 1,
            "listener service should receive the message published by Client to a topic");
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 8: Calling sendMessage after close() returns an Error
// ─────────────────────────────────────────────────────────────────────────────

@test:Config {
    groups: ["client"]
}
isolated function testClientClose() returns error? {
    Client mqClient = check new (BROKER_URL);
    check mqClient->close();
    Error? result = mqClient->sendMessage("client.test.close.queue", {
        messageId: "after-close",
        payload: "should fail".toBytes()
    });
    test:assertTrue(result is Error, "sendMessage after close should return an Error");
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 9: messageId, timestamp, and destination are populated by the broker
// ─────────────────────────────────────────────────────────────────────────────

@test:Config {
    groups: ["client"]
}
isolated function testClientBrokerPopulatedFields() returns error? {
    Client mqClient = check new (BROKER_URL);
    check mqClient->sendMessage("client.test.broker.fields.queue", {
        messageId: "sent-id",
        payload: "Broker fields test".toBytes()
    });
    Message? received = check mqClient->receiveMessage("client.test.broker.fields.queue", 5000);
    check mqClient->close();
    test:assertTrue(received is Message, "should receive message");
    if received is Message {
        // Broker assigns its own message ID
        test:assertTrue(received.messageId.length() > 0, "broker-assigned messageId should be present");
        // Broker sets the timestamp at send time
        int? timestamp = received.timestamp;
        test:assertTrue(timestamp is int && timestamp > 0, "broker-assigned timestamp should be > 0");
        // Destination should reflect where the message landed
        string? destination = received.destination;
        test:assertTrue(destination is string, "destination should be present");
        test:assertTrue((<string>destination).includes("client.test.broker.fields.queue"),
            "destination should contain the queue name");
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 10: receiveMessage with a message selector — only matching messages returned
// ─────────────────────────────────────────────────────────────────────────────

@test:Config {
    groups: ["client", "selector"]
}
isolated function testClientReceiveWithSelector() returns error? {
    Client mqClient = check new (BROKER_URL);
    // Send two messages: one with region=APAC and one with region=EMEA.
    check mqClient->sendMessage("client.test.selector.queue", {
        messageId: "sel-apac",
        payload: "APAC order".toBytes(),
        properties: {"region": "APAC"}
    });
    check mqClient->sendMessage("client.test.selector.queue", {
        messageId: "sel-emea",
        payload: "EMEA order".toBytes(),
        properties: {"region": "EMEA"}
    });

    // Receive with selector — should get only APAC even though EMEA arrived first.
    Message? apacMsg = check mqClient->receiveMessage(
        "client.test.selector.queue", 5000, "region = 'APAC'");
    // Drain the EMEA message that was skipped.
    Message? emeaMsg = check mqClient->receiveMessage(
        "client.test.selector.queue", 3000, "region = 'EMEA'");
    check mqClient->close();

    test:assertTrue(apacMsg is Message, "should receive the APAC message via selector");
    if apacMsg is Message {
        string content = check string:fromBytes(apacMsg.payload);
        test:assertEquals(content, "APAC order", "selector should return only the matching message");
    }
    test:assertTrue(emeaMsg is Message, "EMEA message should also be receivable with its selector");
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 11: receiveMessage without selector still works (backward compat)
// ─────────────────────────────────────────────────────────────────────────────

@test:Config {
    groups: ["client", "selector"]
}
isolated function testClientReceiveWithoutSelector() returns error? {
    Client mqClient = check new (BROKER_URL);
    check mqClient->sendMessage("client.test.no.selector.queue", {
        messageId: "no-sel-1",
        payload: "no selector".toBytes()
    });
    Message? msg = check mqClient->receiveMessage("client.test.no.selector.queue", 5000);
    check mqClient->close();
    test:assertTrue(msg is Message, "receiveMessage without selector should still work");
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 12: Transaction — commit delivers all messages atomically
// ─────────────────────────────────────────────────────────────────────────────

@test:Config {
    groups: ["client", "transaction"]
}
isolated function testClientTransactionCommit() returns error? {
    Client mqClient = check new (BROKER_URL);

    Transaction tx = check mqClient->'transaction();
    check tx->sendMessage("client.tx.commit.queue", {
        messageId: "tx-1",
        payload: "tx message 1".toBytes()
    });
    check tx->sendMessage("client.tx.commit.queue", {
        messageId: "tx-2",
        payload: "tx message 2".toBytes()
    });
    check tx->'commit();
    check tx->close();

    // Both messages must now be visible.
    Message? msg1 = check mqClient->receiveMessage("client.tx.commit.queue", 5000);
    Message? msg2 = check mqClient->receiveMessage("client.tx.commit.queue", 5000);
    check mqClient->close();

    test:assertTrue(msg1 is Message, "first committed message should be received");
    test:assertTrue(msg2 is Message, "second committed message should be received");
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 13: Transaction — rollback discards all messages
// ─────────────────────────────────────────────────────────────────────────────

@test:Config {
    groups: ["client", "transaction"]
}
isolated function testClientTransactionRollback() returns error? {
    Client mqClient = check new (BROKER_URL);

    Transaction tx = check mqClient->'transaction();
    check tx->sendMessage("client.tx.rollback.queue", {
        messageId: "tx-rb-1",
        payload: "will be discarded".toBytes()
    });
    check tx->'rollback();
    check tx->close();

    // Queue must be empty — rollback discarded the message.
    Message? msg = check mqClient->receiveMessage("client.tx.rollback.queue", 2000);
    check mqClient->close();
    test:assertTrue(msg is (), "rolled-back message must not be delivered");
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 14: Transaction — close without commit implicitly rolls back
// ─────────────────────────────────────────────────────────────────────────────

@test:Config {
    groups: ["client", "transaction"]
}
isolated function testClientTransactionCloseRollsBack() returns error? {
    Client mqClient = check new (BROKER_URL);

    Transaction tx = check mqClient->'transaction();
    check tx->sendMessage("client.tx.close.queue", {
        messageId: "tx-close-1",
        payload: "implicit rollback".toBytes()
    });
    check tx->close(); // close without commit — broker must discard the message

    Message? msg = check mqClient->receiveMessage("client.tx.close.queue", 2000);
    check mqClient->close();
    test:assertTrue(msg is (), "closing a transaction without committing must roll back");
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 15: sendRequest — responder sends reply to JMSReplyTo; caller receives it
// ─────────────────────────────────────────────────────────────────────────────

isolated int rrResponderCount = 0;

@test:Config {
    groups: ["client", "request-reply"]
}
function testClientSendRequest() returns error? {
    lock { rrResponderCount = 0; }

    // Responder: receives requests and sends a reply to the replyTo destination,
    // echoing the request's correlationId so the caller can match the reply.
    Listener responderListener = check new (BROKER_URL);
    Service responderSvc = @ServiceConfig {
        queueName: "client.rr.request.queue",
        pollingInterval: 1,
        receiveTimeout: 2
    } service object {
        remote function onMessage(Message message) returns error? {
            lock { rrResponderCount += 1; }
            string? replyTo = message.replyTo;
            if replyTo is string {
                Client responder = check new (BROKER_URL);
                check responder->sendMessage(replyTo, {
                    messageId: "rr-reply-1",
                    correlationId: message.correlationId,
                    payload: "Reply: received".toBytes()
                });
                check responder->close();
            }
        }
    };
    check responderListener.attach(responderSvc, "rr-responder-svc");
    check responderListener.'start();

    runtime:sleep(1);

    Client requester = check new (BROKER_URL);
    // Use correlationId (a JMS header) to tag the request for matching with the reply.
    Message? reply = check requester->sendRequest("client.rr.request.queue", {
        messageId: "rr-req-1",
        correlationId: "rr-corr-id-001",
        payload: "Request".toBytes()
    }, 10000);
    check requester->close();

    check responderListener.gracefulStop();

    test:assertTrue(reply is Message, "sendRequest should receive a reply within the timeout");
    if reply is Message {
        string content = check string:fromBytes(reply.payload);
        test:assertEquals(content, "Reply: received", "reply payload should match what the responder sent");
        test:assertEquals(reply.correlationId, "rr-corr-id-001",
            "reply correlationId should echo the request correlationId");
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 16: sendRequest — returns () when no responder replies within timeout
// ─────────────────────────────────────────────────────────────────────────────

@test:Config {
    groups: ["client", "request-reply"]
}
isolated function testClientSendRequestTimeout() returns error? {
    Client mqClient = check new (BROKER_URL);
    // Nobody is listening on this queue, so the reply never arrives.
    Message? reply = check mqClient->sendRequest("client.rr.nobody.queue", {
        messageId: "rr-timeout-1",
        payload: "orphan request".toBytes()
    }, 1500);
    check mqClient->close();
    test:assertTrue(reply is (), "sendRequest should return () when no reply arrives within the timeout");
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 17: Scheduled delivery — fields are accepted without error
// (Full delay verification requires schedulerSupport="true" in activemq.xml)
// ─────────────────────────────────────────────────────────────────────────────

@test:Config {
    groups: ["client", "scheduler"],
    enable: false
}
function testClientScheduledDelivery() returns error? {
    Client mqClient = check new (BROKER_URL);
    // Schedule delivery 4 seconds in the future.
    check mqClient->sendMessage("client.scheduled.queue", {
        messageId: "sched-1",
        payload: "scheduled message".toBytes(),
        scheduledDelay: 4000
    });

    // Immediate receive must time out — message is not yet due.
    Message? early = check mqClient->receiveMessage("client.scheduled.queue", 1000);
    test:assertTrue(early is (), "message should not be delivered before the scheduled delay");

    // Wait for the scheduler to release the message.
    runtime:sleep(6);
    Message? msg = check mqClient->receiveMessage("client.scheduled.queue", 3000);
    check mqClient->close();
    test:assertTrue(msg is Message, "message should be delivered after the scheduled delay");
}
