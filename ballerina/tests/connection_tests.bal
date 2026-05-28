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

import ballerina/test;

// Configurable variables read from ballerina/tests/Config.toml under [ballerinax.activemq].
// Copy Config.toml.example to Config.toml and adjust values for your environment.
configurable string brokerUrl = "tcp://localhost:61616";
configurable string username = "admin";
configurable string password = "admin";

// Drain all messages from a queue so tests start with a clean slate even when
// a previous run left unconsumed messages behind.
function drainQueue(string queueName) returns error? {
    Client drainer = check new (brokerUrl, username = username, password = password);
    Message? msg = check drainer->receiveMessage(queueName, 1000);
    while msg is Message {
        msg = check drainer->receiveMessage(queueName, 500);
    }
    check drainer->close();
}

// TC-CONN-01: Successful connection to broker
@test:Config {
    groups: ["integration", "connection"]
}
function testItSuccessfulConnection() returns error? {
    Client mqClient = check new (brokerUrl, username = username, password = password);
    check mqClient->close();
}

// TC-CONN-02: Failed connection — wrong host
@test:Config {
    groups: ["integration", "connection"]
}
function testItConnectionWrongHost() {
    Client|Error result = new Client("tcp://localhost:19999");
    if result is Client {
        // Lazy connection: the error surfaces on first use rather than at init time.
        Error? sendErr = result->sendMessage("it.conn.discard.queue",
            {messageId: "dead-wrong-host", payload: "x".toBytes()});
        do { check result->close(); } on fail { }
        test:assertTrue(sendErr is Error,
            "sendMessage should fail when the broker is unreachable");
    } else {
        // Eager connection: error surfaced at init time — also valid.
        test:assertTrue(result is Error,
            "init should return Error for an unreachable broker");
    }
}

// TC-CONN-03: Failed connection — wrong credentials
// DISABLED: The apache/activemq-classic:6.2.4 Docker image does not enforce OpenWire
// (JMS) authentication by default. ACTIVEMQ_ADMIN_LOGIN / ACTIVEMQ_ADMIN_PASSWORD only
// protect the web-console REST API, not JMS connections over OpenWire port 61616.
// To enable JMS credential validation, mount a custom activemq.xml that includes a
// SimpleAuthenticationPlugin or equivalent security plugin.
@test:Config {
    groups: ["integration", "connection"],
    enable: false
}
function testItConnectionWrongCredentials() {
    Client|Error result = new Client(brokerUrl, username = "wronguser", password = "wrongpass");
    test:assertTrue(result is Error,
        "should return Error when the broker enforces authentication with wrong credentials");
    if result is Client {
        do { check result->close(); } on fail { }
    }
}
