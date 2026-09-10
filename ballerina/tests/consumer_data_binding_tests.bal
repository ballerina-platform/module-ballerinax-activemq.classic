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
    groups: ["integration", "dataBinding"]
}
function testDataBindingString() returns error? {
    check drainQueue("it.databind.string.queue");
    MessageProducer producer = check new (brokerUrl, username = username, password = password);
    string payload = "This is a sample payload";
    check producer->send({payload}, {queueName: "it.databind.string.queue"});
    check producer->close();

    MessageConsumer consumer = check new (brokerUrl,
        username = username, password = password, destination = {queueName: "it.databind.string.queue"});
    record {|*Message; string payload;|}? received = check consumer->receive(5000);
    check consumer->close();
    test:assertTrue(received is Message, "should receive the sent message");
    if received is Message {
        test:assertEquals(received.payload, payload, "string payload should round-trip");
    }
}

@test:Config {
    groups: ["integration", "dataBinding"]
}
function testDataBindingUntypedTextMessageIsBytes() returns error? {
    check drainQueue("it.databind.untypedtext.queue");
    MessageProducer producer = check new (brokerUrl, username = username, password = password);
    string payload = "This is a sample payload";
    check producer->send({payload}, {queueName: "it.databind.untypedtext.queue"});
    check producer->close();

    MessageConsumer consumer = check new (brokerUrl,
        username = username, password = password, destination = {queueName: "it.databind.untypedtext.queue"});
    Message? received = check consumer->receive(5000);
    check consumer->close();
    test:assertTrue(received is Message, "should receive the sent message");
    if received is Message {
        test:assertTrue(received.payload is byte[],
            "untyped receive() on a TextMessage should return byte[], not string");
        test:assertEquals(check string:fromBytes(<byte[]>received.payload), payload);
    }
}

@test:Config {
    groups: ["integration", "dataBinding"]
}
function testDataBindingNullTextMessageBodyDoesNotPanic() returns error? {
    check drainQueue("it.databind.nulltext.queue");
    check sendTextMessageWithNullBody(brokerUrl, "it.databind.nulltext.queue");

    MessageConsumer typedConsumer = check new (brokerUrl,
        username = username, password = password, destination = {queueName: "it.databind.nulltext.queue"});
    record {|*Message; string payload;|}? typedReceived = check typedConsumer->receive(5000);
    check typedConsumer->close();
    test:assertTrue(typedReceived is Message, "should receive the null-body message");
    if typedReceived is Message {
        test:assertEquals(typedReceived.payload, "", "a null TextMessage body should bind to an empty string");
    }

    check sendTextMessageWithNullBody(brokerUrl, "it.databind.nulltext.queue");
    MessageConsumer untypedConsumer = check new (brokerUrl,
        username = username, password = password, destination = {queueName: "it.databind.nulltext.queue"});
    Message? untypedReceived = check untypedConsumer->receive(5000);
    check untypedConsumer->close();
    test:assertTrue(untypedReceived is Message, "should receive the null-body message");
    if untypedReceived is Message {
        test:assertEquals(untypedReceived.payload, [],
            "a null TextMessage body should bind to an empty byte[] on the untyped path");
    }
}

@test:Config {
    groups: ["integration", "dataBinding"]
}
function testDataBindingRecord() returns error? {
    check drainQueue("it.databind.record.queue");
    MessageProducer producer = check new (brokerUrl, username = username, password = password);
    record {|string name; int age;|} payload = {name: "Ana", age: 30};
    check producer->send({payload}, {queueName: "it.databind.record.queue"});
    check producer->close();

    MessageConsumer consumer = check new (brokerUrl,
        username = username, password = password, destination = {queueName: "it.databind.record.queue"});
    record {|*Message; record {|string name; int age;|} payload;|}? received = check consumer->receive(5000);
    check consumer->close();
    test:assertTrue(received is Message, "should receive the sent message");
    if received is Message {
        test:assertEquals(received.payload, payload, "record payload should round-trip via MapMessage");
    }
}

@test:Config {
    groups: ["integration", "dataBinding"]
}
function testDataBindingRecordWithByteArrayField() returns error? {
    check drainQueue("it.databind.mapbytes.queue");
    MessageProducer producer = check new (brokerUrl, username = username, password = password);
    record {|string name; byte[] data;|} payload = {name: "blob", data: "hello".toBytes()};
    check producer->send({payload}, {queueName: "it.databind.mapbytes.queue"});
    check producer->close();

    MessageConsumer consumer = check new (brokerUrl,
        username = username, password = password, destination = {queueName: "it.databind.mapbytes.queue"});
    record {|*Message; record {|string name; byte[] data;|} payload;|}? received = check consumer->receive(5000);
    check consumer->close();
    test:assertTrue(received is Message, "should receive the sent message");
    if received is Message {
        test:assertEquals(received.payload.name, payload.name);
        test:assertEquals(received.payload.data, payload.data,
            "a byte[] MapMessage entry should round-trip instead of being dropped");
    }
}

@test:Config {
    groups: ["integration", "dataBinding"]
}
function testDataBindingUntypedMapWithByteArrayField() returns error? {
    check drainQueue("it.databind.untypedmapbytes.queue");
    MessageProducer producer = check new (brokerUrl, username = username, password = password);
    check producer->send({payload: {"note": "x", "data": "world".toBytes()}},
        {queueName: "it.databind.untypedmapbytes.queue"});
    check producer->close();

    MessageConsumer consumer = check new (brokerUrl,
        username = username, password = password,
        destination = {queueName: "it.databind.untypedmapbytes.queue"});
    Message? received = check consumer->receive(5000);
    check consumer->close();
    test:assertTrue(received is Message, "should receive the sent message");
    if received is Message {
        anydata payload = received.payload;
        test:assertTrue(payload is map<Property>, "untyped MapMessage payload should be a map<Property>");
        if payload is map<Property> {
            test:assertEquals(payload["note"], "x");
            Property? data = payload["data"];
            test:assertTrue(data is byte[], "a byte[] map entry should round-trip, not be dropped");
            if data is byte[] {
                test:assertEquals(check string:fromBytes(data), "world");
            }
        }
    }
}

@test:Config {
    groups: ["integration", "dataBinding"]
}
function testDataBindingByteArray() returns error? {
    check drainQueue("it.databind.bytes.queue");
    MessageProducer producer = check new (brokerUrl, username = username, password = password);
    byte[] payload = [72, 101, 108, 108, 111];
    check producer->send({payload}, {queueName: "it.databind.bytes.queue"});
    check producer->close();

    MessageConsumer consumer = check new (brokerUrl,
        username = username, password = password, destination = {queueName: "it.databind.bytes.queue"});
    record {|*Message; byte[] payload;|}? received = check consumer->receive(5000);
    check consumer->close();
    test:assertTrue(received is Message, "should receive the sent message");
    if received is Message {
        test:assertEquals(received.payload, payload, "byte array payload should round-trip byte-for-byte");
    }
}

@test:Config {
    groups: ["integration", "dataBinding"]
}
function testDataBindingUntypedDefault() returns error? {
    check drainQueue("it.databind.untyped.queue");
    MessageProducer producer = check new (brokerUrl, username = username, password = password);
    check producer->send({payload: "Hello".toBytes()}, {queueName: "it.databind.untyped.queue"});
    check producer->close();

    MessageConsumer consumer = check new (brokerUrl,
        username = username, password = password, destination = {queueName: "it.databind.untyped.queue"});
    Message? received = check consumer->receive(5000);
    check consumer->close();
    test:assertTrue(received is Message, "should receive the sent message");
    if received is Message {
        test:assertTrue(received.payload is byte[],
            "untyped receive should fall back to raw bytes for a BytesMessage");
    }
}

@test:Config {
    groups: ["integration", "dataBinding"]
}
function testDataBindingTypeMismatch() returns error? {
    check drainQueue("it.databind.mismatch.queue");
    MessageProducer producer = check new (brokerUrl, username = username, password = password);
    check producer->send({payload: "not a number"}, {queueName: "it.databind.mismatch.queue"});
    check producer->close();

    MessageConsumer consumer = check new (brokerUrl,
        username = username, password = password, destination = {queueName: "it.databind.mismatch.queue"});
    record {|*Message; int payload;|}|error? received = consumer->receive(5000);
    check consumer->close();
    test:assertTrue(received is error, "should return an error for a type mismatch, not panic");
    if received is error {
        test:assertEquals(received.message(),
            "Data binding failed: Cannot bind TextMessage to type 'int'. Expected 'string' or 'xml'");
    }
}

@test:Config {
    groups: ["integration", "dataBinding"]
}
function testCreateTemporaryQueueConsumer() returns error? {
    MessageConsumer|Error result = new (brokerUrl,
        username = username, password = password, destination = {queueName: "", temporary: true});
    test:assertTrue(result is MessageConsumer,
        "a temporary: true destination should bind to a real broker-created temporary queue");
    if result is MessageConsumer {
        check result->close();
    }
}

@test:Config {
    groups: ["integration", "dataBinding"]
}
function testTemporaryReplyToRoundTripsIdentity() returns error? {
    check drainQueue("it.temp.reply.request.queue");
    MessageConsumer responder = check new (brokerUrl,
        username = username, password = password,
        destination = {queueName: "it.temp.reply.request.queue"});

    future<string> replyFuture = start sendRequestAndAwaitTemporaryReply(
        brokerUrl, "it.temp.reply.request.queue", "request payload", 8000);

    Message? request = check responder->receive(5000);
    test:assertTrue(request is Message, "responder should receive the request");
    if request is Message {
        Destination? replyTo = request.replyTo;
        test:assertTrue(replyTo is Queue, "replyTo should be a queue destination");
        if replyTo is Queue {
            test:assertTrue(replyTo.temporary, "replyTo should be marked temporary");
            MessageProducer producer = check new (brokerUrl, username = username, password = password);
            check producer->send({payload: "reply payload"}, replyTo);
            check producer->close();
        }
    }
    check responder->close();

    string|error reply = wait replyFuture;
    test:assertEquals(reply, "reply payload",
        "reply should be delivered to the original temporary queue, not a re-created regular one");
}

@test:Config {
    groups: ["integration", "dataBinding"]
}
function testUnrecognizedPropertyTypeFallsBackToString() returns error? {
    check drainQueue("it.databind.unsupportedprop.queue");
    check sendMessageWithUnsupportedPropertyType(
        brokerUrl, "it.databind.unsupportedprop.queue", "payload with odd property");

    MessageConsumer consumer = check new (brokerUrl,
        username = username, password = password,
        destination = {queueName: "it.databind.unsupportedprop.queue"});
    Message? received = check consumer->receive(5000);
    check consumer->close();
    test:assertTrue(received is Message, "should receive the sent message");
    if received is Message {
        map<Property>? props = received.properties;
        test:assertTrue(props is map<Property>, "properties should be present");
        if props is map<Property> {
            Property? unsupported = props["unsupportedProp"];
            test:assertTrue(unsupported is string,
                "an unrecognized property type should fall back to a string, not be dropped");
            if unsupported is string {
                test:assertTrue(unsupported.length() > 0, "the fallback string should be non-empty");
            }
        }
    }
}

@test:Config {
    groups: ["integration", "dataBinding"]
}
function testDataBindingIntegerPayload() returns error? {
    check drainQueue("it.databind.int.queue");
    MessageProducer producer = check new (brokerUrl, username = username, password = password);
    int payload = 42;
    check producer->send({payload}, {queueName: "it.databind.int.queue"});
    check producer->close();

    MessageConsumer consumer = check new (brokerUrl,
        username = username, password = password, destination = {queueName: "it.databind.int.queue"});
    record {|*Message; int payload;|}? received = check consumer->receive(5000);
    check consumer->close();
    test:assertTrue(received is Message, "should receive the sent message");
    if received is Message {
        test:assertEquals(received.payload, payload, "int payload should round-trip");
    }
}

@test:Config {
    groups: ["integration", "dataBinding"]
}
function testDataBindingOtherScalarPayloadTypes() returns error? {
    check drainQueue("it.databind.floatpayload.queue");
    check drainQueue("it.databind.decimalpayload.queue");
    check drainQueue("it.databind.boolpayload.queue");

    MessageProducer producer = check new (brokerUrl, username = username, password = password);
    float floatPayload = 3.5;
    decimal decimalPayload = 12.75d;
    boolean boolPayload = true;
    check producer->send({payload: floatPayload}, {queueName: "it.databind.floatpayload.queue"});
    check producer->send({payload: decimalPayload}, {queueName: "it.databind.decimalpayload.queue"});
    check producer->send({payload: boolPayload}, {queueName: "it.databind.boolpayload.queue"});
    check producer->close();

    MessageConsumer floatConsumer = check new (brokerUrl,
        username = username, password = password, destination = {queueName: "it.databind.floatpayload.queue"});
    record {|*Message; float payload;|}? receivedFloat = check floatConsumer->receive(5000);
    check floatConsumer->close();
    test:assertTrue(receivedFloat is Message, "should receive the float message");
    if receivedFloat is Message {
        test:assertEquals(receivedFloat.payload, floatPayload, "float payload should round-trip");
    }

    MessageConsumer decimalConsumer = check new (brokerUrl,
        username = username, password = password, destination = {queueName: "it.databind.decimalpayload.queue"});
    record {|*Message; decimal payload;|}? receivedDecimal = check decimalConsumer->receive(5000);
    check decimalConsumer->close();
    test:assertTrue(receivedDecimal is Message, "should receive the decimal message");
    if receivedDecimal is Message {
        test:assertEquals(receivedDecimal.payload, decimalPayload, "decimal payload should round-trip");
    }

    MessageConsumer boolConsumer = check new (brokerUrl,
        username = username, password = password, destination = {queueName: "it.databind.boolpayload.queue"});
    record {|*Message; boolean payload;|}? receivedBool = check boolConsumer->receive(5000);
    check boolConsumer->close();
    test:assertTrue(receivedBool is Message, "should receive the boolean message");
    if receivedBool is Message {
        test:assertEquals(receivedBool.payload, boolPayload, "boolean payload should round-trip");
    }
}
