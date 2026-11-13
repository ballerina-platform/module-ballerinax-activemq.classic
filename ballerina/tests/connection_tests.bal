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

configurable string brokerUrl = "tcp://localhost:61616";
configurable string username = "admin";
configurable string password = "admin";

isolated function drainQueue(string queueName) returns error? {
    MessageConsumer drainer = check new (brokerUrl,
        username = username, password = password, destination = {queueName: queueName});
    error? drainError = ();
    do {
        Message? msg = check drainer->receive(1000);
        while msg is Message {
            msg = check drainer->receive(500);
        }
    } on fail error e {
        drainError = e;
    }
    check drainer->close();
    if drainError is error {
        return drainError;
    }
}

@test:Config {
    groups: ["integration", "connection"]
}
function testItSuccessfulConnection() returns error? {
    MessageProducer producer = check new (brokerUrl, username = username, password = password);
    check producer->close();
}

@test:Config {
    groups: ["integration", "connection"]
}
function testItConnectionWrongHost() {
    MessageProducer|Error result = new MessageProducer("tcp://localhost:19999");
    if result is MessageProducer {
        Error? sendErr = result->send(
            {messageId: "dead-wrong-host", payload: "x".toBytes()}, {queueName: "it.conn.discard.queue"});
        do { check result->close(); } on fail { }
        test:assertTrue(sendErr is Error,
            "send should fail when the broker is unreachable");
    } else {
        test:assertTrue(result is Error,
            "init should return Error for an unreachable broker");
    }
}

@test:Config {
    groups: ["integration", "connection"],
    enable: false
}
function testItConnectionWrongCredentials() {
    MessageProducer|Error result = new MessageProducer(brokerUrl, username = "wronguser", password = "wrongpass");
    test:assertTrue(result is Error,
        "should return Error when the broker enforces authentication with wrong credentials");
    if result is MessageProducer {
        do { check result->close(); } on fail { }
    }
}

@test:Config {
    groups: ["integration", "connection"]
}
function testItConsumerInitFailureAfterConnectionOpenCleansUp() {
    MessageConsumer|Error result = new (brokerUrl, username = username, password = password,
        destination = {queueName: "it.conn.init.cleanup.queue"}, messageSelector = "not a valid ( selector");
    test:assertTrue(result is Error,
        "a malformed message selector should be rejected with a clean Error, not a panic");
    if result is MessageConsumer {
        do { check result->close(); } on fail { }
    }
}
