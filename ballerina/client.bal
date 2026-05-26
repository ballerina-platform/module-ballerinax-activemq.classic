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

import ballerina/jballerina.java;

# Represents an ActiveMQ client for synchronously sending and receiving messages.
#
# Use `"topic://topicName"` as the destination to address a JMS topic.
# Plain names (no prefix) address JMS queues.
public isolated client class Client {

    # Initializes the ActiveMQ client with the specified broker URL and connection configurations.
    #
    # ```ballerina
    # activemq:Client mqClient = check new ("tcp://localhost:61616",
    #     username = "admin",
    #     password = "admin"
    # );
    # ```
    #
    # + url - The URL of the ActiveMQ broker. Supported formats:
    #         - TCP: `"tcp://localhost:61616"`
    #         - SSL: `"ssl://localhost:61617"`
    #         - Failover: `"failover:(tcp://host1:61616,tcp://host2:61616)"`
    # + configurations - The connection configurations including authentication, SSL, and policies
    # + return - `activemq:Error` if the initialization fails, `()` otherwise
    public isolated function init(string url, *ConnectionConfiguration configurations) returns Error? {
        return self.initClient(url, configurations);
    }

    # Sends a message to the specified destination.
    # Use `"topic://topicName"` to send to a JMS topic; plain names go to a queue.
    #
    # ```ballerina
    # check mqClient->sendMessage("orders.queue", message);
    # check mqClient->sendMessage("topic://order.events", eventMessage);
    # ```
    #
    # + destination - Queue name or `"topic://topicName"` for a topic
    # + message - The message to send
    # + return - `activemq:Error` if sending fails, `()` otherwise
    isolated remote function sendMessage(string destination, Message message) returns Error? = @java:Method {
        'class: "io.ballerina.lib.activemq.client.Client"
    } external;

    # Receives a message from the specified destination synchronously.
    # Returns `()` if no message arrives within the timeout.
    #
    # ```ballerina
    # activemq:Message? msg = check mqClient->receiveMessage("orders.queue", 5000);
    # ```
    #
    # + destination - Queue name or `"topic://topicName"` for a topic
    # + timeoutMs - Maximum time in milliseconds to wait for a message
    # + return - The received `activemq:Message`, `()` on timeout, or `activemq:Error` on failure
    isolated remote function receiveMessage(string destination, int timeoutMs = 5000) returns Message|Error? = @java:Method {
        'class: "io.ballerina.lib.activemq.client.Client"
    } external;

    # Closes the ActiveMQ client and its underlying JMS connection.
    #
    # ```ballerina
    # check mqClient->close();
    # ```
    #
    # + return - `activemq:Error` if closing fails, `()` otherwise
    isolated remote function close() returns Error? = @java:Method {
        'class: "io.ballerina.lib.activemq.client.Client"
    } external;

    isolated function initClient(string url, ConnectionConfiguration configurations) returns Error? = @java:Method {
        name: "init",
        'class: "io.ballerina.lib.activemq.client.Client"
    } external;
}
