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

isolated int vtFanOutGroupACount = 0;
isolated int vtFanOutGroupBCount = 0;
isolated int vtLoadBalanceCount = 0;

@test:Config {
    groups: ["integration", "virtual-topic"]
}
function testVirtualTopicFanOut() returns error? {
    check drainQueue("Consumer.groupA.VirtualTopic.it.vt.fanout");
    check drainQueue("Consumer.groupB.VirtualTopic.it.vt.fanout");
    lock { vtFanOutGroupACount = 0; }
    lock { vtFanOutGroupBCount = 0; }

    Listener listenerA = check new (brokerUrl, username = username, password = password);
    Service svcA = @ServiceConfig {
        queueName: "Consumer.groupA.VirtualTopic.it.vt.fanout"
    } service object {
        remote function onMessage(Message message) returns error? {
            lock { vtFanOutGroupACount += 1; }
        }
    };
    check listenerA.attach(svcA, "vt-fanout-svc-a");
    check listenerA.'start();

    Listener listenerB = check new (brokerUrl, username = username, password = password);
    Service svcB = @ServiceConfig {
        queueName: "Consumer.groupB.VirtualTopic.it.vt.fanout"
    } service object {
        remote function onMessage(Message message) returns error? {
            lock { vtFanOutGroupBCount += 1; }
        }
    };
    check listenerB.attach(svcB, "vt-fanout-svc-b");
    check listenerB.'start();

    runtime:sleep(3);

    MessageProducer prod = check new (brokerUrl, username = username, password = password);
    check prod->send({
        messageId: "vt-fanout-01",
        payload: "VirtualTopic fan-out message".toBytes()
    }, {topicName: "VirtualTopic.it.vt.fanout"});
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

@test:Config {
    groups: ["integration", "virtual-topic"]
}
function testVirtualTopicLoadBalancing() returns error? {
    check drainQueue("Consumer.workers.VirtualTopic.it.vt.lb");
    lock { vtLoadBalanceCount = 0; }

    Listener instance1 = check new (brokerUrl, username = username, password = password);
    Service svc1 = @ServiceConfig {
        queueName: "Consumer.workers.VirtualTopic.it.vt.lb"
    } service object {
        remote function onMessage(Message message) returns error? {
            lock { vtLoadBalanceCount += 1; }
        }
    };
    check instance1.attach(svc1, "vt-lb-instance-1");
    check instance1.'start();

    Listener instance2 = check new (brokerUrl, username = username, password = password);
    Service svc2 = @ServiceConfig {
        queueName: "Consumer.workers.VirtualTopic.it.vt.lb"
    } service object {
        remote function onMessage(Message message) returns error? {
            lock { vtLoadBalanceCount += 1; }
        }
    };
    check instance2.attach(svc2, "vt-lb-instance-2");
    check instance2.'start();

    runtime:sleep(3);

    MessageProducer prod = check new (brokerUrl, username = username, password = password);
    int messageCount = 4;
    foreach int i in 1 ... messageCount {
        check prod->send({
            messageId: string `vt-lb-${i}`,
            payload: string `lb-message-${i}`.toBytes()
        }, {topicName: "VirtualTopic.it.vt.lb"});
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
