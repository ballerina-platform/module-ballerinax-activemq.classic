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

// Virtual Topics are an ActiveMQ Classic feature that combines topic fan-out with
// queue load-balancing. A producer publishes to "topic://VirtualTopic.<name>". Each
// logical consumer group subscribes to "Consumer.<groupId>.VirtualTopic.<name>", which
// is a regular queue — messages are stored even if the consumer is temporarily offline.
// Within a consumer group, multiple competing consumers share the load.
//
// The broker's default configuration already includes a virtual-topic interceptor that
// matches the "Consumer.*.<VirtualTopic>.*" pattern, so no extra broker configuration
// is needed for these tests.

import ballerina/lang.runtime;
import ballerina/test;

isolated int vtFanOutGroupACount = 0;
isolated int vtFanOutGroupBCount = 0;
isolated int vtLoadBalanceCount = 0;

// TC-VTOPIC-01: Two consumer groups each receive their own copy of every published message.
// This verifies the fan-out behaviour of virtual topics.
@test:Config {
    groups: ["integration", "virtual-topic"]
}
function testVirtualTopicFanOut() returns error? {
    lock { vtFanOutGroupACount = 0; }
    lock { vtFanOutGroupBCount = 0; }

    // Consumer group A — subscribes via its own consumer queue.
    Listener listenerA = check new (brokerUrl, username = username, password = password);
    Service svcA = @ServiceConfig {
        queueName: "Consumer.groupA.VirtualTopic.it.vt.fanout",
        pollingInterval: 1,
        receiveTimeout: 2
    } service object {
        remote function onMessage(Message message) returns error? {
            lock { vtFanOutGroupACount += 1; }
        }
    };
    check listenerA.attach(svcA, "vt-fanout-svc-a");
    check listenerA.'start();

    // Consumer group B — independent copy of every message.
    Listener listenerB = check new (brokerUrl, username = username, password = password);
    Service svcB = @ServiceConfig {
        queueName: "Consumer.groupB.VirtualTopic.it.vt.fanout",
        pollingInterval: 1,
        receiveTimeout: 2
    } service object {
        remote function onMessage(Message message) returns error? {
            lock { vtFanOutGroupBCount += 1; }
        }
    };
    check listenerB.attach(svcB, "vt-fanout-svc-b");
    check listenerB.'start();

    // Both consumer queues must exist before the publisher sends.
    runtime:sleep(3);

    Client prod = check new (brokerUrl, username = username, password = password);
    check prod->sendMessage("topic://VirtualTopic.it.vt.fanout", {
        messageId: "vt-fanout-01",
        payload: "VirtualTopic fan-out message".toBytes()
    });
    check prod->close();

    runtime:sleep(5);

    int countA = 0;
    int countB = 0;
    lock { countA = vtFanOutGroupACount; }
    lock { countB = vtFanOutGroupBCount; }

    test:assertEquals(countA, 1, "consumer group A should receive its copy of the message");
    test:assertEquals(countB, 1, "consumer group B should receive its independent copy");

    check listenerA.gracefulStop();
    check listenerB.gracefulStop();
}

// TC-VTOPIC-02: Two instances of the same consumer group share (load-balance) messages.
// Each message is delivered to exactly one instance; the total across both instances equals
// the number of messages published.
@test:Config {
    groups: ["integration", "virtual-topic"]
}
function testVirtualTopicLoadBalancing() returns error? {
    lock { vtLoadBalanceCount = 0; }

    // Both instances subscribe to the same consumer queue — they compete for messages.
    Listener instance1 = check new (brokerUrl, username = username, password = password);
    Service svc1 = @ServiceConfig {
        queueName: "Consumer.workers.VirtualTopic.it.vt.lb",
        pollingInterval: 1,
        receiveTimeout: 2
    } service object {
        remote function onMessage(Message message) returns error? {
            lock { vtLoadBalanceCount += 1; }
        }
    };
    check instance1.attach(svc1, "vt-lb-instance-1");
    check instance1.'start();

    Listener instance2 = check new (brokerUrl, username = username, password = password);
    Service svc2 = @ServiceConfig {
        queueName: "Consumer.workers.VirtualTopic.it.vt.lb",
        pollingInterval: 1,
        receiveTimeout: 2
    } service object {
        remote function onMessage(Message message) returns error? {
            lock { vtLoadBalanceCount += 1; }
        }
    };
    check instance2.attach(svc2, "vt-lb-instance-2");
    check instance2.'start();

    runtime:sleep(3);

    Client prod = check new (brokerUrl, username = username, password = password);
    int messageCount = 4;
    foreach int i in 1 ... messageCount {
        check prod->sendMessage("topic://VirtualTopic.it.vt.lb", {
            messageId: string `vt-lb-${i}`,
            payload: string `lb-message-${i}`.toBytes()
        });
    }
    check prod->close();

    runtime:sleep(8);

    int total = 0;
    lock { total = vtLoadBalanceCount; }

    test:assertEquals(total, messageCount,
        "all messages should be consumed exactly once across both instances");

    check instance1.gracefulStop();
    check instance2.gracefulStop();
}
