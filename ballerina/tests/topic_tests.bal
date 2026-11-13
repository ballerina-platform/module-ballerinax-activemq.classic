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

isolated int itTopicSingleSubCount = 0;
isolated int itTopicMultiSub1Count = 0;
isolated int itTopicMultiSub2Count = 0;

@test:Config {
    groups: ["integration", "topic-pub-sub"]
}
function testItTopicPublishAndSubscribe() returns error? {
    lock { itTopicSingleSubCount = 0; }

    Listener topicListener = check new (brokerUrl, username = username, password = password);
    Service topicSvc = @ServiceConfig {
        topicName: "it.topic.single"
    } service object {
        remote function onMessage(Message message) returns error? {
            lock { itTopicSingleSubCount += 1; }
        }
    };
    check topicListener.attach(topicSvc, "it-topic-single-svc");
    check topicListener.'start();

    runtime:sleep(3);

    MessageProducer prod = check new (brokerUrl, username = username, password = password);
    check prod->send({
        messageId: "it-topic-single-01",
        payload: "Topic publish test".toBytes()
    }, {topicName: "it.topic.single"});
    check prod->close();

    runtime:sleep(5);

    int count = 0;
    lock { count = itTopicSingleSubCount; }
    test:assertEquals(count, 1,
        "subscriber should receive exactly the one published message");
    check topicListener.gracefulStop();
}

@test:Config {
    groups: ["integration", "topic-pub-sub"]
}
function testItTopicMultipleSubscribers() returns error? {
    lock { itTopicMultiSub1Count = 0; }
    lock { itTopicMultiSub2Count = 0; }

    Listener sub1Listener = check new (brokerUrl, username = username, password = password);
    Service sub1Svc = @ServiceConfig {
        topicName: "it.topic.multi"
    } service object {
        remote function onMessage(Message message) returns error? {
            lock { itTopicMultiSub1Count += 1; }
        }
    };
    check sub1Listener.attach(sub1Svc, "it-topic-multi-sub1");
    check sub1Listener.'start();

    Listener sub2Listener = check new (brokerUrl, username = username, password = password);
    Service sub2Svc = @ServiceConfig {
        topicName: "it.topic.multi"
    } service object {
        remote function onMessage(Message message) returns error? {
            lock { itTopicMultiSub2Count += 1; }
        }
    };
    check sub2Listener.attach(sub2Svc, "it-topic-multi-sub2");
    check sub2Listener.'start();

    runtime:sleep(4);

    MessageProducer prod = check new (brokerUrl, username = username, password = password);
    check prod->send({
        messageId: "it-topic-multi-01",
        payload: "Topic fan-out message".toBytes()
    }, {topicName: "it.topic.multi"});
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
