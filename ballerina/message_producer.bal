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

import ballerina/jballerina.java;

# Represents an ActiveMQ message producer for sending messages to queues and topics.
public isolated client class MessageProducer {

    # Initializes the ActiveMQ message producer with the specified broker URL and configurations.
    #
    # ```ballerina
    # classic:MessageProducer producer = check new ("tcp://localhost:61616",
    #     username = "admin",
    #     password = "admin",
    #     destination = {queueName: "orders.queue"}
    # );
    # ```
    #
    # + url - The URL of the ActiveMQ broker. Supported formats:
    #         - TCP: `"tcp://localhost:61616"`
    #         - SSL: `"ssl://localhost:61617"`
    #         - Failover: `"failover:(tcp://host1:61616,tcp://host2:61616)"`
    # + configurations - The producer configurations including authentication, SSL, policies, and
    #                    an optional default destination
    # + return - `classic:Error` if the initialization fails, `()` otherwise
    public isolated function init(string url, *ProducerConfiguration configurations) returns Error? {
        check validateConnectionConfigurations(configurations);
        return self.initProducer(url, configurations);
    }

    isolated function initProducer(string url, ProducerConfiguration configurations) returns Error? = @java:Method {
        name: "init",
        'class: "io.ballerina.lib.activemq.producer.Actions"
    } external;

    # Sends a message to the specified destination, or to the producer's configured default
    # destination if `destination` is not given.
    #
    # ```ballerina
    # check producer->send(message);
    # check producer->send(message, {topicName: "order.events"});
    # ```
    #
    # + message - The message to send
    # + destination - The destination to send to for this call, overriding the producer's
    #                 configured default destination, if any
    # + return - `classic:Error` if sending fails, `()` otherwise
    isolated remote function send(Message message, Destination? destination = ()) returns Error? {
        check validateMessage(message);
        return self.externSend(message, destination);
    }

    isolated function externSend(Message message, Destination? destination) returns Error? = @java:Method {
        name: "send",
        'class: "io.ballerina.lib.activemq.producer.Actions"
    } external;

    # Commits all messages sent since the last commit or rollback, delivering them atomically.
    # Only valid when the producer is configured with `transacted: true`.
    #
    # ```ballerina
    # check producer->'commit();
    # ```
    #
    # + return - `classic:Error` if the commit fails, `()` otherwise
    isolated remote function 'commit() returns Error? = @java:Method {
        'class: "io.ballerina.lib.activemq.producer.Actions"
    } external;

    # Rolls back all messages sent since the last commit, discarding them without delivering them.
    # Only valid when the producer is configured with `transacted: true`.
    #
    # ```ballerina
    # check producer->'rollback();
    # ```
    #
    # + return - `classic:Error` if the rollback fails, `()` otherwise
    isolated remote function 'rollback() returns Error? = @java:Method {
        'class: "io.ballerina.lib.activemq.producer.Actions"
    } external;

    # Closes the producer and its underlying JMS connection.
    #
    # ```ballerina
    # check producer->close();
    # ```
    #
    # + return - `classic:Error` if closing fails, `()` otherwise
    isolated remote function close() returns Error? = @java:Method {
        'class: "io.ballerina.lib.activemq.producer.Actions"
    } external;
}
