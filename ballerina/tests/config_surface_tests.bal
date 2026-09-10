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

@test:Config {
    groups: ["integration", "config-surface"]
}
function testConfigSurfacePrefetchPolicy() returns error? {
    check drainQueue("it.config.prefetch.queue");
    MessageProducer producer = check new (brokerUrl, username = username, password = password);
    check producer->send({payload: "prefetch config smoke test".toBytes()},
            {queueName: "it.config.prefetch.queue"});
    check producer->close();

    MessageConsumer consumer = check new (brokerUrl,
        username = username, password = password,
        destination = {queueName: "it.config.prefetch.queue"},
        prefetchPolicy = {queuePrefetchSize: 10, topicPrefetchSize: 10});
    Message? received = check consumer->receive(5000);
    check consumer->close();
    test:assertTrue(received is Message, "send/receive should still work with a custom PrefetchPolicy");
}

@test:Config {
    groups: ["integration", "config-surface"]
}
function testConfigSurfaceOptimizeAcknowledgements() returns error? {
    check drainQueue("it.config.optack.queue");
    MessageProducer producer = check new (brokerUrl,
        username = username, password = password, optimizeAcknowledgements = true);
    check producer->send({payload: "optimizeAcknowledgements smoke test".toBytes()},
            {queueName: "it.config.optack.queue"});
    check producer->close();

    MessageConsumer consumer = check new (brokerUrl,
        username = username, password = password, optimizeAcknowledgements = true,
        destination = {queueName: "it.config.optack.queue"});
    Message? received = check consumer->receive(5000);
    check consumer->close();
    test:assertTrue(received is Message, "send/receive should still work with optimizeAcknowledgements enabled");
}

@test:Config {
    groups: ["integration", "config-surface"]
}
function testConfigSurfaceSetAlwaysSessionAsync() returns error? {
    check drainQueue("it.config.async.queue");
    MessageProducer producer = check new (brokerUrl,
        username = username, password = password, setAlwaysSessionAsync = false);
    check producer->send({payload: "setAlwaysSessionAsync smoke test".toBytes()},
            {queueName: "it.config.async.queue"});
    check producer->close();

    MessageConsumer consumer = check new (brokerUrl,
        username = username, password = password, setAlwaysSessionAsync = false,
        destination = {queueName: "it.config.async.queue"});
    Message? received = check consumer->receive(5000);
    check consumer->close();
    test:assertTrue(received is Message, "send/receive should still work with setAlwaysSessionAsync disabled");
}

isolated int itConfigNoLocalCount = 0;

@test:Config {
    groups: ["integration", "config-surface"]
}
function testConfigSurfaceNoLocalTopic() returns error? {
    lock { itConfigNoLocalCount = 0; }
    Listener noLocalListener = check new (brokerUrl, username = username, password = password);
    Service noLocalSvc = @ServiceConfig {
        topicName: "it.config.nolocal.topic",
        noLocal: true
    } service object {
        remote function onMessage(Message message) returns error? {
            lock { itConfigNoLocalCount += 1; }
        }
    };
    check noLocalListener.attach(noLocalSvc, "it-config-nolocal-svc");
    check noLocalListener.'start();

    runtime:sleep(2);

    MessageProducer producer = check new (brokerUrl, username = username, password = password);
    check producer->send({payload: "noLocal smoke test".toBytes()}, {topicName: "it.config.nolocal.topic"});
    check producer->close();

    runtime:sleep(2);
    check noLocalListener.gracefulStop();

    int received = 0;
    lock { received = itConfigNoLocalCount; }
    test:assertEquals(received, 1,
        "a message from a different connection should still be delivered with noLocal enabled");
}

isolated int itConfigRedeliveryAttempts = 0;

@test:Config {
    groups: ["integration", "config-surface"]
}
function testConfigSurfaceRedeliveryPolicyDeadLetterQueue() returns error? {
    check drainQueue("it.config.redelivery.queue");
    lock { itConfigRedeliveryAttempts = 0; }
    Listener redeliveryListener = check new (brokerUrl,
        username = username, password = password,
        redeliveryPolicy = {
            maximumRedeliveries: 2,
            initialRedeliveryDelay: 500,
            redeliveryDelay: 500,
            useExponentialBackOff: false
        });
    Service alwaysRollbackSvc = @ServiceConfig {
        sessionAckMode: SESSION_TRANSACTED,
        queueName: "it.config.redelivery.queue"
    } service object {
        remote function onMessage(Message message, Caller caller) returns error? {
            lock { itConfigRedeliveryAttempts += 1; }
            check caller->'rollback();
        }
    };
    check redeliveryListener.attach(alwaysRollbackSvc, "it-config-redelivery-svc");
    check redeliveryListener.'start();

    MessageProducer producer = check new (brokerUrl, username = username, password = password);
    check producer->send({
        payload: "redelivery policy test".toBytes(),
        properties: {"testTag": "config-redelivery-dlq"}
    }, {queueName: "it.config.redelivery.queue"});
    check producer->close();

    runtime:sleep(6);
    check redeliveryListener.gracefulStop();

    int attempts = 0;
    lock { attempts = itConfigRedeliveryAttempts; }
    test:assertEquals(attempts, 3, "should be delivered exactly 1 + maximumRedeliveries times");

    MessageConsumer dlqConsumer = check new (brokerUrl,
        username = username, password = password,
        destination = {queueName: "ActiveMQ.DLQ"}, messageSelector = "testTag = 'config-redelivery-dlq'");
    Message? deadLettered = check dlqConsumer->receive(5000);
    check dlqConsumer->close();
    test:assertTrue(deadLettered is Message,
        "a message that exhausts maximumRedeliveries should end up on ActiveMQ.DLQ");
}

isolated int itConfigClientIdReceivedCount = 0;

@test:Config {
    groups: ["integration", "config-surface"]
}
function testConfigSurfaceClientIdDurableReconnect() returns error? {
    lock { itConfigClientIdReceivedCount = 0; }
    string clientId = "it-config-clientid-durable-test";
    string subscriberName = "it-config-clientid-durable-sub";

    Listener listenerA = check new (brokerUrl, username = username, password = password, clientID = clientId);
    Service svcA = @ServiceConfig {
        topicName: "it.config.clientid.topic",
        consumerType: DURABLE,
        subscriberName
    } service object {
        remote function onMessage(Message message) returns error? {
        }
    };
    check listenerA.attach(svcA, "it-config-clientid-svc-a");
    check listenerA.'start();
    // Allow the broker to persist the durable subscription before disconnecting.
    runtime:sleep(2);
    check listenerA.gracefulStop();
    // Allow the broker to finish disconnecting before publishing offline.
    runtime:sleep(2);

    MessageProducer producer = check new (brokerUrl, username = username, password = password);
    check producer->send({payload: "published while offline".toBytes()}, {topicName: "it.config.clientid.topic"});
    check producer->close();

    Listener listenerB = check new (brokerUrl, username = username, password = password, clientID = clientId);
    Service svcB = @ServiceConfig {
        topicName: "it.config.clientid.topic",
        consumerType: DURABLE,
        subscriberName
    } service object {
        remote function onMessage(Message message) returns error? {
            lock { itConfigClientIdReceivedCount += 1; }
        }
    };
    check listenerB.attach(svcB, "it-config-clientid-svc-b");
    check listenerB.'start();

    runtime:sleep(5);
    check listenerB.gracefulStop();

    int received = 0;
    lock { received = itConfigClientIdReceivedCount; }
    test:assertEquals(received, 1,
        "reconnecting with the same clientID should resume the durable subscription and deliver " +
            "the message that arrived while offline");
}
