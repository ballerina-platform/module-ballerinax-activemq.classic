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

listener Listener itListener = check new Listener(brokerUrl, username = username, password = password);

isolated int itListenerTextCount = 0;
isolated string itListenerTextPayload = "";
isolated int itListenerOrderedCount = 0;
isolated int itListenerStopCount = 0;
isolated int itListenerPushCount = 0;
isolated int itListenerConcurrentActive = 0;
isolated boolean itListenerOverlapDetected = false;
isolated int itListenerConcurrentProcessed = 0;
isolated int itListenerObjMsgErrorCount = 0;
isolated int itListenerObjMsgTextCount = 0;
isolated int itListenerUnrelatedCount = 0;
isolated int itListenerReturnedErrorOnErrorCount = 0;
isolated int itListenerPanicOnErrorCount = 0;
isolated int itListenerPanicFollowupCount = 0;

@test:Config {
    groups: ["integration", "listener"]
}
function testItListenerReceivesTextMessage() returns error? {
    lock { itListenerTextCount = 0; }
    lock { itListenerTextPayload = ""; }

    Service listenerTextSvc = @ServiceConfig {
        queueName: "it.listener.text.queue"
    } service object {
        remote function onMessage(Message message) returns error? {
            string payload = check string:fromBytes(check message.payload.ensureType());
            lock { itListenerTextCount += 1; }
            lock { itListenerTextPayload = payload; }
        }
    };
    check itListener.attach(listenerTextSvc, "it-listener-text-svc");

    MessageProducer prod = check new (brokerUrl, username = username, password = password);
    check prod->send({
        messageId: "it-listener-text-01",
        payload: "Listener integration test".toBytes()
    }, {queueName: "it.listener.text.queue"});
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

@test:Config {
    groups: ["integration", "listener"],
    dependsOn: [testItListenerReceivesTextMessage]
}
function testItListenerReceivesMultipleMessages() returns error? {
    lock { itListenerOrderedCount = 0; }

    Service listenerOrderSvc = @ServiceConfig {
        queueName: "it.listener.order.queue"
    } service object {
        remote function onMessage(Message message) returns error? {
            lock { itListenerOrderedCount += 1; }
        }
    };
    check itListener.attach(listenerOrderSvc, "it-listener-order-svc");

    MessageProducer prod = check new (brokerUrl, username = username, password = password);
    foreach int i in 1 ... 5 {
        check prod->send({
            messageId: string `order-${i}`,
            payload: string `msg-${i}`.toBytes()
        }, {queueName: "it.listener.order.queue"});
    }
    check prod->close();

    runtime:sleep(10);

    int count = 0;
    lock { count = itListenerOrderedCount; }
    test:assertEquals(count, 5, "listener should receive all 5 messages without loss");
}

@test:Config {
    groups: ["integration", "listener"]
}
function testItListenerStartupFailure() {
    Listener|Error result = new Listener("tcp://localhost:19999");
    if result is Error {
        test:assertTrue(result is Error,
            "init should return Error for an unreachable broker");
    } else {
        Error? attachResult = result.attach(
            @ServiceConfig {
                queueName: "it.listener.fail.queue"
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

@test:Config {
    groups: ["integration", "listener"]
}
function testItListenerGracefulStop() returns error? {
    lock { itListenerStopCount = 0; }

    Listener stopListener = check new (brokerUrl, username = username, password = password);
    Service stopSvc = @ServiceConfig {
        queueName: "it.listener.stop.queue"
    } service object {
        remote function onMessage(Message message) returns error? {
            lock { itListenerStopCount += 1; }
        }
    };
    check stopListener.attach(stopSvc, "it-stop-svc");
    check stopListener.'start();

    MessageProducer prod = check new (brokerUrl, username = username, password = password);
    check prod->send({
        messageId: "it-stop-pre-01",
        payload: "before-stop".toBytes()
    }, {queueName: "it.listener.stop.queue"});
    runtime:sleep(4);

    int countBeforeStop = 0;
    lock { countBeforeStop = itListenerStopCount; }
    test:assertTrue(countBeforeStop >= 1,
        "listener should receive at least one message before stop");

    check stopListener.gracefulStop();

    check prod->send({
        messageId: "it-stop-post-01",
        payload: "after-stop".toBytes()
    }, {queueName: "it.listener.stop.queue"});
    check prod->close();
    runtime:sleep(3);

    int countAfterStop = 0;
    lock { countAfterStop = itListenerStopCount; }
    test:assertEquals(countAfterStop, countBeforeStop,
        "no further messages should be delivered after gracefulStop");
}

@test:Config {
    groups: ["integration", "listener"]
}
function testItListenerPushDeliveryIsFast() returns error? {
    lock { itListenerPushCount = 0; }

    Service pushSvc = @ServiceConfig {
        queueName: "it.listener.push.queue"
    } service object {
        remote function onMessage(Message message) returns error? {
            lock { itListenerPushCount += 1; }
        }
    };
    check itListener.attach(pushSvc, "it-push-svc");

    MessageProducer prod = check new (brokerUrl, username = username, password = password);
    check prod->send({
        messageId: "it-push-01",
        payload: "push delivery".toBytes()
    }, {queueName: "it.listener.push.queue"});
    check prod->close();

    runtime:sleep(1);

    int count = 0;
    lock { count = itListenerPushCount; }
    test:assertTrue(count >= 1,
        "message should be delivered within 1 second via native push delivery, not polling");
}

@test:Config {
    groups: ["integration", "listener"]
}
function testItListenerSequentialProcessing() returns error? {
    check drainQueue("it.listener.sequential.queue");
    lock { itListenerConcurrentActive = 0; }
    lock { itListenerOverlapDetected = false; }
    lock { itListenerConcurrentProcessed = 0; }

    Service seqSvc = @ServiceConfig {
        queueName: "it.listener.sequential.queue"
    } service object {
        remote function onMessage(Message message) returns error? {
            int activeNow = 0;
            lock {
                itListenerConcurrentActive += 1;
                activeNow = itListenerConcurrentActive;
            }
            if activeNow > 1 {
                lock { itListenerOverlapDetected = true; }
            }
            runtime:sleep(0.3);
            lock { itListenerConcurrentActive -= 1; }
            lock { itListenerConcurrentProcessed += 1; }
        }
    };
    check itListener.attach(seqSvc, "it-seq-svc");

    MessageProducer prod = check new (brokerUrl, username = username, password = password);
    foreach int i in 1 ... 5 {
        check prod->send({
            messageId: string `seq-${i}`,
            payload: string `msg-${i}`.toBytes()
        }, {queueName: "it.listener.sequential.queue"});
    }
    check prod->close();

    runtime:sleep(4);

    int processed = 0;
    boolean overlap = false;
    lock { processed = itListenerConcurrentProcessed; }
    lock { overlap = itListenerOverlapDetected; }
    test:assertEquals(processed, 5, "all 5 messages should be processed");
    test:assertFalse(overlap, "messages for a single service must be processed sequentially, not concurrently");
}

@test:Config {
    groups: ["integration", "listener"]
}
function testItListenerUnsupportedMessageTypeDoesNotHang() returns error? {
    lock { itListenerObjMsgErrorCount = 0; }
    lock { itListenerObjMsgTextCount = 0; }

    Service objMsgSvc = @ServiceConfig {
        queueName: "it.listener.objmsg.queue"
    } service object {
        remote function onMessage(Message message) returns error? {
            lock { itListenerObjMsgTextCount += 1; }
        }

        remote function onError(Error err) returns error? {
            lock { itListenerObjMsgErrorCount += 1; }
        }
    };
    check itListener.attach(objMsgSvc, "it-objmsg-svc");

    check sendObjectMessageToQueue(brokerUrl, "it.listener.objmsg.queue", "unsupported-payload");

    MessageProducer prod = check new (brokerUrl, username = username, password = password);
    check prod->send({
        messageId: "it-objmsg-followup",
        payload: "still alive".toBytes()
    }, {queueName: "it.listener.objmsg.queue"});
    check prod->close();

    int textCount = 0;
    int attempts = 0;
    while textCount < 1 && attempts < 30 {
        runtime:sleep(1);
        lock { textCount = itListenerObjMsgTextCount; }
        attempts += 1;
    }

    int errorCount = 0;
    lock { errorCount = itListenerObjMsgErrorCount; }
    test:assertTrue(errorCount >= 1, "onError should be invoked for the unsupported ObjectMessage");
    test:assertTrue(textCount >= 1,
        "the service must still receive the follow-up text message — the delivery loop must not hang");
}

@test:Config {
    groups: ["integration", "listener"]
}
function testItListenerFailingOnErrorDoesNotCrashRuntime() returns error? {
    lock { itListenerUnrelatedCount = 0; }

    Service failingOnErrorSvc = @ServiceConfig {
        queueName: "it.listener.failingonerror.queue"
    } service object {
        remote function onMessage(Message message) returns error? {
        }

        remote function onError(Error err) returns error? {
            panic error("intentional onError failure for test coverage");
        }
    };
    check itListener.attach(failingOnErrorSvc, "it-failing-onerror-svc");

    Service unrelatedSvc = @ServiceConfig {
        queueName: "it.listener.unrelated.queue"
    } service object {
        remote function onMessage(Message message) returns error? {
            lock { itListenerUnrelatedCount += 1; }
        }
    };
    check itListener.attach(unrelatedSvc, "it-unrelated-svc");

    check sendObjectMessageToQueue(brokerUrl, "it.listener.failingonerror.queue", "trigger-onerror-failure");
    runtime:sleep(2);

    MessageProducer prod = check new (brokerUrl, username = username, password = password);
    check prod->send({
        messageId: "it-unrelated-01",
        payload: "unrelated service still alive".toBytes()
    }, {queueName: "it.listener.unrelated.queue"});
    check prod->close();

    runtime:sleep(3);

    int unrelatedCount = 0;
    lock { unrelatedCount = itListenerUnrelatedCount; }
    test:assertTrue(unrelatedCount >= 1,
        "a failing onError handler must not crash the runtime — other services must keep working");
}

@test:Config {
    groups: ["integration", "listener"]
}
function testItListenerReturnedErrorGoesToOnError() returns error? {
    lock { itListenerReturnedErrorOnErrorCount = 0; }

    Service returnedErrorSvc = @ServiceConfig {
        queueName: "it.listener.returnederror.queue"
    } service object {
        remote function onMessage(Message message) returns error? {
            return error("intentional returned error for test coverage");
        }

        remote function onError(Error err) returns error? {
            lock { itListenerReturnedErrorOnErrorCount += 1; }
        }
    };
    check itListener.attach(returnedErrorSvc, "it-returnederror-svc");

    MessageProducer prod = check new (brokerUrl, username = username, password = password);
    check prod->send({
        messageId: "it-returnederror-01",
        payload: "trigger returned error".toBytes()
    }, {queueName: "it.listener.returnederror.queue"});
    check prod->close();

    runtime:sleep(3);
    check itListener.detach(returnedErrorSvc);

    int errorCount = 0;
    lock { errorCount = itListenerReturnedErrorOnErrorCount; }
    test:assertTrue(errorCount >= 1, "onMessage returning an error should be routed to onError");
}

@test:Config {
    groups: ["integration", "listener"]
}
function testItListenerOnMessagePanicDoesNotGoToOnError() returns error? {
    lock { itListenerPanicOnErrorCount = 0; }
    lock { itListenerPanicFollowupCount = 0; }

    Service panicSvc = @ServiceConfig {
        queueName: "it.listener.panic.queue"
    } service object {
        remote function onMessage(Message message) returns error? {
            string payload = check string:fromBytes(check message.payload.ensureType());
            if payload == "trigger panic" {
                panic error("intentional onMessage panic for test coverage");
            }
            lock { itListenerPanicFollowupCount += 1; }
        }

        remote function onError(Error err) returns error? {
            lock { itListenerPanicOnErrorCount += 1; }
        }
    };
    check itListener.attach(panicSvc, "it-panic-svc");

    MessageProducer prod = check new (brokerUrl, username = username, password = password);
    check prod->send({
        messageId: "it-panic-01",
        payload: "trigger panic".toBytes()
    }, {queueName: "it.listener.panic.queue"});
    check prod->close();

    runtime:sleep(3);

    MessageProducer prod2 = check new (brokerUrl, username = username, password = password);
    check prod2->send({
        messageId: "it-panic-followup",
        payload: "still alive".toBytes()
    }, {queueName: "it.listener.panic.queue"});
    check prod2->close();

    int followupCount = 0;
    int attempts = 0;
    while followupCount < 1 && attempts < 30 {
        runtime:sleep(1);
        lock { followupCount = itListenerPanicFollowupCount; }
        attempts += 1;
    }
    check itListener.detach(panicSvc);

    int onErrorCount = 0;
    lock { onErrorCount = itListenerPanicOnErrorCount; }
    test:assertEquals(onErrorCount, 0, "an onMessage panic must not be routed to onError");
    test:assertTrue(followupCount >= 1,
        "the service must still receive the follow-up message — a panic must not hang delivery");
}

@test:AfterSuite
function cleanupItListener() {
    do { check itListener.gracefulStop(); } on fail { }
}
