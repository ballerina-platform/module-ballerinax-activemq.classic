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

import ballerina/test;

@test:Config {
    groups: ["integration", "queue-producer"]
}
function testItSendTextMessageToTypedQueue() returns error? {
    MessageProducer mqClient = check new (brokerUrl, username = username, password = password);
    Error? result = mqClient->send({
        messageId: "it-prod-typed-01",
        payload: "Hello, typed queue!".toBytes()
    }, {queueName: "it.prod.typed.queue"});
    check mqClient->close();
    test:assertTrue(result is (), "send should succeed for a typed queue destination");
}

@test:Config {
    groups: ["integration", "queue-producer"]
}
function testItSendTextMessageToQueue() returns error? {
    MessageProducer mqClient = check new (brokerUrl, username = username, password = password);
    Error? result = mqClient->send({
        messageId: "it-prod-text-01",
        payload: "Hello, ActiveMQ 6.2.4!".toBytes()
    }, {queueName: "it.prod.text.queue"});
    check mqClient->close();
    test:assertTrue(result is (), "send should succeed for a text payload");
}

@test:Config {
    groups: ["integration", "queue-producer"]
}
function testItSendBytesMessageToQueue() returns error? {
    MessageProducer mqClient = check new (brokerUrl, username = username, password = password);
    byte[] binaryPayload = [0x00, 0x01, 0x02, 0xFF, 0xFE, 0xAB, 0xCD];
    Error? result = mqClient->send({
        messageId: "it-prod-bytes-01",
        payload: binaryPayload
    }, {queueName: "it.prod.bytes.queue"});
    check mqClient->close();
    test:assertTrue(result is (), "send should succeed for a binary payload");
}

@test:Config {
    groups: ["integration", "queue-producer"]
}
function testItSendMapMessageToQueue() returns error? {
    MessageProducer mqClient = check new (brokerUrl, username = username, password = password);
    Error? result = mqClient->send({
        messageId: "it-prod-map-01",
        payload: "{}".toBytes(),
        properties: {
            "region": "APAC",
            "priority": "high",
            "count": 42
        }
    }, {queueName: "it.prod.map.queue"});
    check mqClient->close();
    test:assertTrue(result is (), "send with a properties map should succeed");
}
