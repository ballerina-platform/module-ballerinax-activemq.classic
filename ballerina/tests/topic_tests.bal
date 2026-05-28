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

isolated int itTopicSingleSubCount = 0;
isolated int itTopicMultiSub1Count = 0;
isolated int itTopicMultiSub2Count = 0;

// TC-TOPIC-01: Publish a TextMessage to a topic and assert the subscriber receives it
@test:Config {
    groups: ["integration", "topic-pub-sub"]
}
function testItTopicPublishAndSubscribe() returns error? {
    lock { itTopicSingleSubCount = 0; }

    Listener topicListener = check new (brokerUrl, username = username, password = password);
    Service topicSvc = @ServiceConfig {
        topicName: "it.topic.single",
        pollingInterval: 1,
        receiveTimeout: 2
    } service object {
        remote function onMessage(Message message) returns error? {
            lock { itTopicSingleSubCount += 1; }
        }
    };
    check topicListener.attach(topicSvc, "it-topic-single-svc");
    check topicListener.'start();

    // Topics require the subscriber to be registered before the publisher sends.
    runtime:sleep(3);

    Client prod = check new (brokerUrl, username = username, password = password);
    check prod->sendMessage("topic://it.topic.single", {
        messageId: "it-topic-single-01",
        payload: "Topic publish test".toBytes()
    });
    check prod->close();

    runtime:sleep(5);

    int count = 0;
    lock { count = itTopicSingleSubCount; }
    test:assertEquals(count, 1,
        "subscriber should receive exactly the one published message");
    check topicListener.gracefulStop();
}

// TC-TOPIC-02: Two subscribers on the same topic each receive the same published message
@test:Config {
    groups: ["integration", "topic-pub-sub"]
}
function testItTopicMultipleSubscribers() returns error? {
    lock { itTopicMultiSub1Count = 0; }
    lock { itTopicMultiSub2Count = 0; }

    Listener sub1Listener = check new (brokerUrl, username = username, password = password);
    Service sub1Svc = @ServiceConfig {
        topicName: "it.topic.multi",
        pollingInterval: 1,
        receiveTimeout: 2
    } service object {
        remote function onMessage(Message message) returns error? {
            lock { itTopicMultiSub1Count += 1; }
        }
    };
    check sub1Listener.attach(sub1Svc, "it-topic-multi-sub1");
    check sub1Listener.'start();

    Listener sub2Listener = check new (brokerUrl, username = username, password = password);
    Service sub2Svc = @ServiceConfig {
        topicName: "it.topic.multi",
        pollingInterval: 1,
        receiveTimeout: 2
    } service object {
        remote function onMessage(Message message) returns error? {
            lock { itTopicMultiSub2Count += 1; }
        }
    };
    check sub2Listener.attach(sub2Svc, "it-topic-multi-sub2");
    check sub2Listener.'start();

    // Both subscribers must be registered before publishing.
    runtime:sleep(4);

    Client prod = check new (brokerUrl, username = username, password = password);
    check prod->sendMessage("topic://it.topic.multi", {
        messageId: "it-topic-multi-01",
        payload: "Topic fan-out message".toBytes()
    });
    check prod->close();

    runtime:sleep(5);

    int count1 = 0;
    int count2 = 0;
    lock { count1 = itTopicMultiSub1Count; }
    lock { count2 = itTopicMultiSub2Count; }

    test:assertEquals(count1, 1, "subscriber 1 should receive the published message");
    test:assertEquals(count2, 1, "subscriber 2 should receive the same published message");

    check sub1Listener.gracefulStop();
    check sub2Listener.gracefulStop();
}
