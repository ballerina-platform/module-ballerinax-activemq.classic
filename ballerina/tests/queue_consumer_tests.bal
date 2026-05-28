// Copyright (c) 2025, WSO2 LLC. (http://www.wso2.com)
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

import ballerina/test;

// TC-QUEUE-CONS-01: Receive TextMessage from queue
@test:Config {
    groups: ["integration", "queue-consumer"]
}
function testItReceiveTextMessageFromQueue() returns error? {
    Client mqClient = check new (brokerUrl, username = username, password = password);
    check mqClient->sendMessage("it.cons.text.queue", {
        messageId: "it-cons-text-01",
        payload: "Hello Consumer".toBytes()
    });
    Message? received = check mqClient->receiveMessage("it.cons.text.queue", 5000);
    check mqClient->close();
    test:assertTrue(received is Message, "should receive the sent TextMessage");
    if received is Message {
        string content = check string:fromBytes(received.payload);
        test:assertEquals(content, "Hello Consumer", "text payload should match");
    }
}

// TC-QUEUE-CONS-02: Receive BytesMessage from queue
@test:Config {
    groups: ["integration", "queue-consumer"]
}
function testItReceiveBytesMessageFromQueue() returns error? {
    Client mqClient = check new (brokerUrl, username = username, password = password);
    byte[] original = [0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x01];
    check mqClient->sendMessage("it.cons.bytes.queue", {
        messageId: "it-cons-bytes-01",
        payload: original
    });
    Message? received = check mqClient->receiveMessage("it.cons.bytes.queue", 5000);
    check mqClient->close();
    test:assertTrue(received is Message, "should receive the sent BytesMessage");
    if received is Message {
        test:assertEquals(received.payload, original,
            "binary payload should be preserved byte-for-byte");
    }
}

// TC-QUEUE-CONS-03: Receive MapMessage from queue (via message properties)
@test:Config {
    groups: ["integration", "queue-consumer"]
}
function testItReceiveMapMessageFromQueue() returns error? {
    Client mqClient = check new (brokerUrl, username = username, password = password);
    check mqClient->sendMessage("it.cons.map.queue", {
        messageId: "it-cons-map-01",
        payload: "{}".toBytes(),
        properties: {"environment": "test", "version": "2"}
    });
    Message? received = check mqClient->receiveMessage("it.cons.map.queue", 5000);
    check mqClient->close();
    test:assertTrue(received is Message, "should receive the message with properties");
    if received is Message {
        map<anydata>? props = received.properties;
        test:assertTrue(props is map<anydata>, "properties should be present in the received message");
        if props is map<anydata> {
            test:assertEquals(props["environment"], "test");
            test:assertEquals(props["version"], "2");
        }
    }
}

// TC-QUEUE-CONS-04: Receive with timeout — no message available
@test:Config {
    groups: ["integration", "queue-consumer"]
}
function testItReceiveTimeoutEmptyQueue() returns error? {
    Client mqClient = check new (brokerUrl, username = username, password = password);
    Message? received = check mqClient->receiveMessage("it.cons.empty.queue", 1000);
    check mqClient->close();
    test:assertTrue(received is (),
        "should return () — not an error — when no message arrives within the timeout");
}
