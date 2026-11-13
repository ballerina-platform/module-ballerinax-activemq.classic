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

listener Listener dataBindingListener = check new Listener(brokerUrl, username = username, password = password);

isolated string listenerDataBindingStringPayload = "";
isolated record {|string name; int age;|} listenerDataBindingRecordPayload = {name: "", age: 0};
isolated string listenerDataBindingErrorMessage = "";
isolated byte[] listenerDataBindingUntypedTextPayload = [];
isolated boolean listenerDataBindingMapMessageHandled = false;

@test:Config {
    groups: ["integration", "listener", "dataBinding"]
}
function testListenerOnMessageStringPayload() returns error? {
    lock { listenerDataBindingStringPayload = ""; }

    Service stringSvc = @ServiceConfig {
        queueName: "it.listener.databind.string.queue"
    } service object {
        remote function onMessage(record {|*Message; string payload;|} message) returns error? {
            lock { listenerDataBindingStringPayload = message.payload; }
        }
    };
    check dataBindingListener.attach(stringSvc, "it-listener-databind-string-svc");

    MessageProducer producer = check new (brokerUrl, username = username, password = password);
    check producer->send({payload: "Listener typed payload"}, {queueName: "it.listener.databind.string.queue"});
    check producer->close();

    runtime:sleep(5);
    check dataBindingListener.detach(stringSvc);

    string received = "";
    lock { received = listenerDataBindingStringPayload; }
    test:assertEquals(received, "Listener typed payload",
        "onMessage should receive the payload narrowed to 'string'");
}

@test:Config {
    groups: ["integration", "listener", "dataBinding"]
}
function testListenerOnMessageRecordPayload() returns error? {
    lock { listenerDataBindingRecordPayload = {name: "", age: 0}; }

    Service recordSvc = @ServiceConfig {
        queueName: "it.listener.databind.record.queue"
    } service object {
        remote function onMessage(record {|*Message; record {|string name; int age;|} payload;|} message)
                returns error? {
            lock { listenerDataBindingRecordPayload = message.payload.clone(); }
        }
    };
    check dataBindingListener.attach(recordSvc, "it-listener-databind-record-svc");

    MessageProducer producer = check new (brokerUrl, username = username, password = password);
    record {|string name; int age;|} payload = {name: "Ana", age: 30};
    check producer->send({payload}, {queueName: "it.listener.databind.record.queue"});
    check producer->close();

    runtime:sleep(5);
    check dataBindingListener.detach(recordSvc);

    record {|string name; int age;|} received;
    lock { received = listenerDataBindingRecordPayload.clone(); }
    test:assertEquals(received, payload, "onMessage should receive the payload narrowed to a record via MapMessage");
}

@test:Config {
    groups: ["integration", "listener", "dataBinding"]
}
function testListenerOnMessageUntypedTextPayloadIsBytes() returns error? {
    lock { listenerDataBindingUntypedTextPayload = []; }

    Service untypedSvc = @ServiceConfig {
        queueName: "it.listener.databind.untypedtext.queue"
    } service object {
        remote function onMessage(Message message) returns error? {
            if message.payload is byte[] {
                lock { listenerDataBindingUntypedTextPayload = (<byte[]>message.payload).clone(); }
            }
        }
    };
    check dataBindingListener.attach(untypedSvc, "it-listener-databind-untypedtext-svc");

    MessageProducer producer = check new (brokerUrl, username = username, password = password);
    check producer->send({payload: "Untyped listener payload"},
        {queueName: "it.listener.databind.untypedtext.queue"});
    check producer->close();

    runtime:sleep(5);
    check dataBindingListener.detach(untypedSvc);

    byte[] received = [];
    lock { received = listenerDataBindingUntypedTextPayload.clone(); }
    test:assertEquals(check string:fromBytes(received), "Untyped listener payload",
        "an unnarrowed classic:Message payload for a TextMessage should be byte[], not string");
}

@test:Config {
    groups: ["integration", "listener", "dataBinding"]
}
function testListenerOnMessageUntypedMapMessageDoesNotCrash() returns error? {
    lock { listenerDataBindingMapMessageHandled = false; }
    lock { listenerDataBindingErrorMessage = ""; }

    Service untypedMapSvc = @ServiceConfig {
        queueName: "it.listener.databind.untypedmap.queue"
    } service object {
        remote function onMessage(Message message) returns error? {
            if message.payload is map<Property> {
                lock { listenerDataBindingMapMessageHandled = true; }
            }
        }
        remote function onError(Error err) returns error? {
            lock { listenerDataBindingErrorMessage = err.message(); }
        }
    };
    check dataBindingListener.attach(untypedMapSvc, "it-listener-databind-untypedmap-svc");

    MessageProducer producer = check new (brokerUrl, username = username, password = password);
    record {|string name; int age;|} payload = {name: "Ana", age: 30};
    check producer->send({payload}, {queueName: "it.listener.databind.untypedmap.queue"});
    check producer->close();

    runtime:sleep(5);
    check dataBindingListener.detach(untypedMapSvc);

    boolean handled = false;
    string errorMessage = "";
    lock { handled = listenerDataBindingMapMessageHandled; }
    lock { errorMessage = listenerDataBindingErrorMessage; }
    test:assertTrue(handled, "an unnarrowed listener should receive a MapMessage as map<Property>, not crash");
    test:assertEquals(errorMessage, "", "no onError should fire for a MapMessage with an unnarrowed parameter");
}

@test:Config {
    groups: ["integration", "listener", "dataBinding"]
}
function testListenerOnMessageTypeMismatchGoesToOnError() returns error? {
    lock { listenerDataBindingErrorMessage = ""; }

    Service mismatchSvc = @ServiceConfig {
        queueName: "it.listener.databind.mismatch.queue"
    } service object {
        remote function onMessage(record {|*Message; int payload;|} message) returns error? {
        }
        remote function onError(Error err) returns error? {
            lock { listenerDataBindingErrorMessage = err.message(); }
        }
    };
    check dataBindingListener.attach(mismatchSvc, "it-listener-databind-mismatch-svc");

    MessageProducer producer = check new (brokerUrl, username = username, password = password);
    check producer->send({payload: "not a number"}, {queueName: "it.listener.databind.mismatch.queue"});
    check producer->close();

    runtime:sleep(5);
    check dataBindingListener.detach(mismatchSvc);

    string errorMessage = "";
    lock { errorMessage = listenerDataBindingErrorMessage; }
    test:assertTrue(errorMessage.includes("Data binding failed"),
        "a payload type mismatch should be routed to onError, not silently dropped");
}
