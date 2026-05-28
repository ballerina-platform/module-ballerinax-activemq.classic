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

// TC-QUEUE-PROD-01: Send TextMessage to queue
@test:Config {
    groups: ["integration", "queue-producer"]
}
function testItSendTextMessageToQueue() returns error? {
    Client mqClient = check new (brokerUrl, username = username, password = password);
    Error? result = mqClient->sendMessage("it.prod.text.queue", {
        messageId: "it-prod-text-01",
        payload: "Hello, ActiveMQ 6.2.4!".toBytes()
    });
    check mqClient->close();
    test:assertTrue(result is (), "sendMessage should succeed for a text payload");
}

// TC-QUEUE-PROD-02: Send BytesMessage to queue
@test:Config {
    groups: ["integration", "queue-producer"]
}
function testItSendBytesMessageToQueue() returns error? {
    Client mqClient = check new (brokerUrl, username = username, password = password);
    byte[] binaryPayload = [0x00, 0x01, 0x02, 0xFF, 0xFE, 0xAB, 0xCD];
    Error? result = mqClient->sendMessage("it.prod.bytes.queue", {
        messageId: "it-prod-bytes-01",
        payload: binaryPayload
    });
    check mqClient->close();
    test:assertTrue(result is (), "sendMessage should succeed for a binary payload");
}

// TC-QUEUE-PROD-03: Send MapMessage to queue
// The connector represents map-like data via the message properties field.
@test:Config {
    groups: ["integration", "queue-producer"]
}
function testItSendMapMessageToQueue() returns error? {
    Client mqClient = check new (brokerUrl, username = username, password = password);
    Error? result = mqClient->sendMessage("it.prod.map.queue", {
        messageId: "it-prod-map-01",
        payload: "{}".toBytes(),
        properties: {
            "region": "APAC",
            "priority": "high",
            "count": 42
        }
    });
    check mqClient->close();
    test:assertTrue(result is (), "sendMessage with a properties map should succeed");
}

// TC-QUEUE-PROD-04: Send to invalid / non-existent destination
// N/A — ActiveMQ Classic auto-creates queues on first access by default (the broker
// ships with destinationPolicy/@policyEntry[@wildcardIncluded=true] that enables
// auto-creation). There is no error to assert for an unknown queue name against the
// default broker configuration.
