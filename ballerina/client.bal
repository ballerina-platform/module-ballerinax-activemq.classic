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
    # activemq:Message? filtered = check mqClient->receiveMessage("orders.queue", 5000, "region = 'APAC'");
    # ```
    #
    # + destination - Queue name or `"topic://topicName"` for a topic
    # + timeoutMs - Maximum time in milliseconds to wait for a message
    # + messageSelector - Optional JMS selector expression to filter messages by their properties.
    #                     Only messages whose properties satisfy the expression are returned.
    #                     For example: `"region = 'APAC' AND priority > 4"`.
    #                     If not provided, the first available message is returned.
    # + return - The received `activemq:Message`, `()` on timeout, or `activemq:Error` on failure
    isolated remote function receiveMessage(string destination, int timeoutMs = 5000,
            string? messageSelector = ()) returns Message|Error? = @java:Method {
        'class: "io.ballerina.lib.activemq.client.Client"
    } external;

    # Sends a request message and blocks until a reply arrives or the timeout elapses.
    # A temporary reply queue is created automatically, set as the message's `replyTo`
    # destination, and deleted once the call returns.
    # The responder must read `message.replyTo` and send a reply to that destination.
    #
    # ```ballerina
    # activemq:Message? reply = check mqClient->sendRequest("pricing.service.queue", requestMsg, 5000);
    # ```
    #
    # + destination - Queue or `"topic://name"` to send the request to
    # + message - The request message (its `replyTo` field is overwritten with the temp reply queue)
    # + timeoutMs - Maximum time in milliseconds to wait for a reply
    # + return - The reply `activemq:Message`, `()` on timeout, or `activemq:Error` on failure
    isolated remote function sendRequest(string destination, Message message, int timeoutMs = 5000)
            returns Message|Error? = @java:Method {
        'class: "io.ballerina.lib.activemq.client.Client"
    } external;

    # Opens a new transacted session for atomic multi-message sends.
    # All `sendMessage` calls on the returned `Transaction` are buffered until
    # `'commit()` is called, which delivers them atomically. Call `'rollback()` or
    # `close()` to discard the buffered messages. Always call `close()` when done.
    #
    # ```ballerina
    # activemq:Transaction tx = check mqClient->'transaction();
    # check tx->sendMessage("orders.queue", orderMsg);
    # check tx->sendMessage("audit.queue", auditMsg);
    # check tx->'commit();
    # check tx->close();
    # ```
    #
    # + return - A new `activemq:Transaction` or `activemq:Error` on failure
    isolated remote function 'transaction() returns Transaction|Error = @java:Method {
        name: "beginTransaction",
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
