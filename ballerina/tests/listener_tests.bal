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

import ballerina/lang.runtime;
import ballerina/test;

// Shared listener for TC-LISTENER-01 and TC-LISTENER-02.
// Closed in @AfterSuite below.
listener Listener itListener = check new Listener(brokerUrl, username = username, password = password);

isolated int itListenerTextCount = 0;
isolated string itListenerTextPayload = "";
isolated int itListenerOrderedCount = 0;
isolated int itListenerStopCount = 0;

// TC-LISTENER-01: Listener receives TextMessage from queue within 5 seconds
@test:Config {
    groups: ["integration", "listener"]
}
function testItListenerReceivesTextMessage() returns error? {
    lock { itListenerTextCount = 0; }
    lock { itListenerTextPayload = ""; }

    Service listenerTextSvc = @ServiceConfig {
        queueName: "it.listener.text.queue",
        pollingInterval: 1,
        receiveTimeout: 2
    } service object {
        remote function onMessage(Message message) returns error? {
            string payload = check string:fromBytes(message.payload);
            lock { itListenerTextCount += 1; }
            lock { itListenerTextPayload = payload; }
        }
    };
    check itListener.attach(listenerTextSvc, "it-listener-text-svc");

    Client prod = check new (brokerUrl, username = username, password = password);
    check prod->sendMessage("it.listener.text.queue", {
        messageId: "it-listener-text-01",
        payload: "Listener integration test".toBytes()
    });
    check prod->close();

    runtime:sleep(5);

    int receivedCount = 0;
    string receivedPayload = "";
    lock { receivedCount = itListenerTextCount; }
    lock { receivedPayload = itListenerTextPayload; }

    test:assertTrue(receivedCount >= 1,
        "listener service should receive the message within 5 seconds");
    test:assertEquals(receivedPayload, "Listener integration test",
        "received payload should match the sent payload");
}

// TC-LISTENER-02: Listener receives 5 messages — no loss
// (Strict ordering is verified in TC-QUEUE-CONS-01 via synchronous receive.)
@test:Config {
    groups: ["integration", "listener"],
    dependsOn: [testItListenerReceivesTextMessage]
}
function testItListenerReceivesMultipleMessages() returns error? {
    lock { itListenerOrderedCount = 0; }

    Service listenerOrderSvc = @ServiceConfig {
        queueName: "it.listener.order.queue",
        pollingInterval: 1,
        receiveTimeout: 2
    } service object {
        remote function onMessage(Message message) returns error? {
            lock { itListenerOrderedCount += 1; }
        }
    };
    check itListener.attach(listenerOrderSvc, "it-listener-order-svc");

    Client prod = check new (brokerUrl, username = username, password = password);
    foreach int i in 1 ... 5 {
        check prod->sendMessage("it.listener.order.queue", {
            messageId: string `order-${i}`,
            payload: string `msg-${i}`.toBytes()
        });
    }
    check prod->close();

    runtime:sleep(10);

    int count = 0;
    lock { count = itListenerOrderedCount; }
    test:assertEquals(count, 5, "listener should receive all 5 messages without loss");
}

// TC-LISTENER-03: Listener startup failure — unreachable broker URL
// Validates that the error is surfaced (not swallowed silently), whether the
// connection is eager (error at init) or lazy (error at first attach/dispatch).
@test:Config {
    groups: ["integration", "listener"]
}
function testItListenerStartupFailure() {
    Listener|Error result = new Listener("tcp://localhost:19999");
    if result is Error {
        // Eager connection: error surfaced at init time — expected.
        test:assertTrue(result is Error,
            "init should return Error for an unreachable broker");
    } else {
        // Lazy connection: error must surface on first attach, not be swallowed.
        Error? attachResult = result.attach(
            @ServiceConfig {
                queueName: "it.listener.fail.queue",
                pollingInterval: 1,
                receiveTimeout: 1
            } service object {
                remote function onMessage(Message message) returns error? {}
            },
            "it-fail-svc"
        );
        do { check result.immediateStop(); } on fail { }
        test:assertTrue(attachResult is Error,
            "attach should fail when the broker is unreachable (lazy connection)");
    }
}

// TC-LISTENER-04: Listener graceful stop — no messages delivered after stop
@test:Config {
    groups: ["integration", "listener"]
}
function testItListenerGracefulStop() returns error? {
    lock { itListenerStopCount = 0; }

    Listener stopListener = check new (brokerUrl, username = username, password = password);
    Service stopSvc = @ServiceConfig {
        queueName: "it.listener.stop.queue",
        pollingInterval: 1,
        receiveTimeout: 2
    } service object {
        remote function onMessage(Message message) returns error? {
            lock { itListenerStopCount += 1; }
        }
    };
    check stopListener.attach(stopSvc, "it-stop-svc");
    check stopListener.'start();

    Client prod = check new (brokerUrl, username = username, password = password);
    check prod->sendMessage("it.listener.stop.queue", {
        messageId: "it-stop-pre-01",
        payload: "before-stop".toBytes()
    });
    runtime:sleep(4);

    int countBeforeStop = 0;
    lock { countBeforeStop = itListenerStopCount; }
    test:assertTrue(countBeforeStop >= 1,
        "listener should receive at least one message before stop");

    check stopListener.gracefulStop();

    check prod->sendMessage("it.listener.stop.queue", {
        messageId: "it-stop-post-01",
        payload: "after-stop".toBytes()
    });
    check prod->close();
    runtime:sleep(3);

    int countAfterStop = 0;
    lock { countAfterStop = itListenerStopCount; }
    test:assertEquals(countAfterStop, countBeforeStop,
        "no further messages should be delivered after gracefulStop");
}

@test:AfterSuite
function cleanupItListener() {
    do { check itListener.gracefulStop(); } on fail { }
}
