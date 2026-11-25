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
import ballerina/time;

@test:Config {
    groups: ["integration", "queue-producer"]
}
function testItSendTextMessageToTypedQueue() returns error? {
    MessageProducer mqClient = check new (brokerUrl, username = username, password = password);
    Error? result = mqClient->send("Hello, typed queue!".toBytes(), {queueName: "it.prod.typed.queue"});
    check mqClient->close();
    test:assertTrue(result is (), "send should succeed for a typed queue destination");
}

@test:Config {
    groups: ["integration", "queue-producer"]
}
function testItSendTextMessageToQueue() returns error? {
    MessageProducer mqClient = check new (brokerUrl, username = username, password = password);
    Error? result = mqClient->send("Hello, ActiveMQ 6.2.4!".toBytes(), {queueName: "it.prod.text.queue"});
    check mqClient->close();
    test:assertTrue(result is (), "send should succeed for a text payload");
}

@test:Config {
    groups: ["integration", "queue-producer"]
}
function testItSendBytesMessageToQueue() returns error? {
    MessageProducer mqClient = check new (brokerUrl, username = username, password = password);
    byte[] binaryPayload = [0x00, 0x01, 0x02, 0xFF, 0xFE, 0xAB, 0xCD];
    Error? result = mqClient->send(binaryPayload, {queueName: "it.prod.bytes.queue"});
    check mqClient->close();
    test:assertTrue(result is (), "send should succeed for a binary payload");
}

@test:Config {
    groups: ["integration", "queue-producer"]
}
function testItSendMapMessageToQueue() returns error? {
    MessageProducer mqClient = check new (brokerUrl, username = username, password = password);
    Error? result = mqClient->send("{}".toBytes(), {queueName: "it.prod.map.queue"},
            properties = {
                "region": "APAC",
                "priority": "high",
                "count": 42
            });
    check mqClient->close();
    test:assertTrue(result is (), "send with a properties map should succeed");
}

@test:Config {
    groups: ["integration", "queue-producer"]
}
function testItSendMapWithPayloadKeySendsTheWholeMap() returns error? {
    string queueName = "it.prod.payloadkey.queue";
    check drainQueue(queueName);
    MessageProducer mqClient = check new (brokerUrl, username = username, password = password,
        destination = {queueName});
    SendOptions options = {priority: 7};
    check mqClient->send({"payload": "inner"}, (), options);
    check mqClient->close();

    MessageConsumer consumer = check new (brokerUrl, username = username, password = password,
        destination = {queueName});
    record {|*Message; map<anydata> payload;|}? received = check consumer->receive(5000);
    check consumer->close();
    if received is () {
        test:assertFail("the message should be received");
    }
    test:assertEquals(received.payload, {"payload": "inner"}, "the whole map should be the payload");
    test:assertEquals(received.priority, 7, "options passed as a record should be applied");
}

@test:Config {
    groups: ["integration", "queue-producer"]
}
function testItSendOptionsWithoutDestination() returns error? {
    string queueName = "it.prod.options.queue";
    check drainQueue(queueName);
    MessageProducer mqClient = check new (brokerUrl, username = username, password = password,
        destination = {queueName});
    int expiry = time:utcNow()[0] * 1000 + 60000;
    check mqClient->send("options", priority = 7, correlationId = "corr-1", expiry = expiry);
    check mqClient->close();

    MessageConsumer consumer = check new (brokerUrl, username = username, password = password,
        destination = {queueName});
    record {|*Message; string payload;|}? received = check consumer->receive(5000);
    check consumer->close();
    if received is () {
        test:assertFail("the message should be received");
    }
    test:assertEquals(received.payload, "options");
    test:assertEquals(received.priority, 7);
    test:assertEquals(received.correlationId, "corr-1");
    int receivedExpiry = received.expiry ?: 0;
    test:assertTrue((receivedExpiry - expiry).abs() < 5000, "the expiry should be applied");
}

@test:Config {
    groups: ["integration", "queue-producer"]
}
function testItSendRecordPayload() returns error? {
    string queueName = "it.prod.record.queue";
    check drainQueue(queueName);
    MessageProducer mqClient = check new (brokerUrl, username = username, password = password);
    record {|string name; int age;|} person = {name: "Alice", age: 25};
    check mqClient->send(person, {queueName});
    check mqClient->close();

    MessageConsumer consumer = check new (brokerUrl, username = username, password = password,
        destination = {queueName});
    record {|*Message; record {|string name; int age;|} payload;|}? received = check consumer->receive(5000);
    check consumer->close();
    if received is () {
        test:assertFail("the message should be received");
    }
    test:assertEquals(received.payload, person);
}

@test:Config {
    groups: ["integration", "queue-producer"]
}
function testItSendScheduledRepeat() returns error? {
    string queueName = "it.prod.repeat.queue";
    check drainQueue(queueName);
    MessageProducer mqClient = check new (brokerUrl, username = username, password = password);
    check mqClient->send("repeat", {queueName}, scheduledDelay = 500, scheduledPeriod = 1000, scheduledRepeat = 1);
    check mqClient->close();

    MessageConsumer consumer = check new (brokerUrl, username = username, password = password,
        destination = {queueName});
    Message? first = check consumer->receive(5000);
    Message? second = check consumer->receive(5000);
    Message? third = check consumer->receive(3000);
    check consumer->close();
    test:assertTrue(first is Message && second is Message, "the message should be delivered twice");
    test:assertTrue(third is (), "the message should not be delivered a third time");
}
