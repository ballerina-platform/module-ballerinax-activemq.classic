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
import ballerina/time;

@test:Config {
    groups: ["integration", "queue-consumer"]
}
function testItReceiveTextMessageFromQueue() returns error? {
    check drainQueue("it.cons.text.queue");
    MessageProducer producer = check new (brokerUrl, username = username, password = password);
    check producer->send({
        messageId: "it-cons-text-01",
        payload: "Hello Consumer".toBytes()
    }, {queueName: "it.cons.text.queue"});
    check producer->close();

    MessageConsumer consumer = check new (brokerUrl,
        username = username, password = password, destination = {queueName: "it.cons.text.queue"});
    record {|*Message; byte[] payload;|}? received = check consumer->receive(5000);
    check consumer->close();
    test:assertTrue(received is Message, "should receive the sent TextMessage");
    if received is Message {
        string content = check string:fromBytes(received.payload);
        test:assertEquals(content, "Hello Consumer", "text payload should match");
    }
}

@test:Config {
    groups: ["integration", "queue-consumer"]
}
function testItReceiveBytesMessageFromQueue() returns error? {
    check drainQueue("it.cons.bytes.queue");
    MessageProducer producer = check new (brokerUrl, username = username, password = password);
    byte[] original = [0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x01];
    check producer->send({
        messageId: "it-cons-bytes-01",
        payload: original
    }, {queueName: "it.cons.bytes.queue"});
    check producer->close();

    MessageConsumer consumer = check new (brokerUrl,
        username = username, password = password, destination = {queueName: "it.cons.bytes.queue"});
    Message? received = check consumer->receive(5000);
    check consumer->close();
    test:assertTrue(received is Message, "should receive the sent BytesMessage");
    if received is Message {
        test:assertEquals(received.payload, original,
            "binary payload should be preserved byte-for-byte");
    }
}

@test:Config {
    groups: ["integration", "queue-consumer"]
}
function testItReceiveMapMessageFromQueue() returns error? {
    check drainQueue("it.cons.map.queue");
    MessageProducer producer = check new (brokerUrl, username = username, password = password);
    check producer->send({
        messageId: "it-cons-map-01",
        payload: "{}".toBytes(),
        properties: {"environment": "test", "version": "2"}
    }, {queueName: "it.cons.map.queue"});
    check producer->close();

    MessageConsumer consumer = check new (brokerUrl,
        username = username, password = password, destination = {queueName: "it.cons.map.queue"});
    Message? received = check consumer->receive(5000);
    check consumer->close();
    test:assertTrue(received is Message, "should receive the message with properties");
    if received is Message {
        map<Property>? props = received.properties;
        test:assertTrue(props is map<Property>, "properties should be present in the received message");
        if props is map<Property> {
            test:assertEquals(props["environment"], "test");
            test:assertEquals(props["version"], "2");
        }
    }
}

@test:Config {
    groups: ["integration", "queue-consumer"]
}
function testItReceiveTimeoutEmptyQueue() returns error? {
    MessageConsumer consumer = check new (brokerUrl,
        username = username, password = password, destination = {queueName: "it.cons.empty.queue"});
    Message? received = check consumer->receive(1000);
    check consumer->close();
    test:assertTrue(received is (),
        "should return () — not an error — when no message arrives within the timeout");
}

@test:Config {
    groups: ["integration", "queue-consumer"]
}
function testItReceiveNoWaitReturnsAvailableMessage() returns error? {
    check drainQueue("it.cons.nowait.queue");
    MessageProducer producer = check new (brokerUrl, username = username, password = password);
    check producer->send({
        messageId: "it-cons-nowait-01",
        payload: "Hello NoWait".toBytes()
    }, {queueName: "it.cons.nowait.queue"});
    check producer->close();

    MessageConsumer consumer = check new (brokerUrl,
        username = username, password = password, destination = {queueName: "it.cons.nowait.queue"});

    record {|*Message; byte[] payload;|}? received = ();
    int attempts = 0;
    while attempts < 20 {
        received = check consumer->receiveNoWait();
        if received is Message {
            break;
        }
        runtime:sleep(0.1);
        attempts += 1;
    }
    check consumer->close();
    test:assertTrue(received is Message, "should receive the already-enqueued message without waiting");
    if received is Message {
        string content = check string:fromBytes(received.payload);
        test:assertEquals(content, "Hello NoWait", "text payload should match");
    }
}

@test:Config {
    groups: ["integration", "queue-consumer"]
}
function testItReceiveNoWaitEmptyQueueReturnsImmediately() returns error? {
    check drainQueue("it.cons.nowait.empty.queue");
    MessageConsumer consumer = check new (brokerUrl,
        username = username, password = password, destination = {queueName: "it.cons.nowait.empty.queue"});

    time:Utc before = time:utcNow();
    Message? received = check consumer->receiveNoWait();
    time:Utc after = time:utcNow();
    check consumer->close();

    test:assertTrue(received is (), "should return () — not an error — when nothing is immediately available");
    decimal elapsedSeconds = time:utcDiffSeconds(after, before);
    test:assertTrue(elapsedSeconds < 1d,
        "receiveNoWait() should return near-instantly, not block like receive(timeout) does");
}
