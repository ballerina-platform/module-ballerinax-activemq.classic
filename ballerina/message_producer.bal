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

    # Sends a payload to the specified destination, or to the producer's configured default
    # destination if `destination` is not given.
    #
    # ```ballerina
    # check producer->send("hello");
    # check producer->send("hello", {topicName: "order.events"}, priority = 7);
    # ```
    #
    # A transacted producer must send inside a `transaction` block. Messages are delivered on
    # commit and discarded on rollback.
    #
    # ```ballerina
    # transaction {
    #     check producer->send("hello");
    #     check commit;
    # }
    # ```
    #
    # + payload - The message content. `string` and `xml` are sent as a `TextMessage`, `byte[]` as a
    #             `BytesMessage`, a map or record as a `MapMessage`, and anything else as JSON bytes.
    #             A `MapMessage` keeps only `string`, `int`, `float`, `boolean`, `byte` and `byte[]`
    #             values
    # + destination - The destination to send to for this call, overriding the producer's
    #                 configured default destination, if any
    # + options - The optional message settings, such as the priority
    # + return - `classic:Error` if sending fails, if a transacted producer sends outside a
    #            `transaction` block or a non-transacted one sends inside, or if the producer is in
    #            another transaction, `()` otherwise
    isolated remote function send(anydata payload, Destination? destination = (), *SendOptions options)
            returns Error? {
        Message message = {payload, ...options};
        check validateMessage(message);
        return self.externSend(message, destination);
    }

    isolated function externSend(Message message, Destination? destination) returns Error? = @java:Method {
        name: "send",
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
