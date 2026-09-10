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

# Represents an ActiveMQ message consumer for receiving messages from a fixed queue or topic.
public isolated client class MessageConsumer {

    # Initializes the ActiveMQ message consumer with the specified broker URL and configurations.
    #
    # ```ballerina
    # classic:MessageConsumer consumer = check new ("tcp://localhost:61616",
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
    # + configurations - The consumer configurations including authentication, SSL, policies,
    #                    acknowledgement mode, and the destination to consume from
    # + return - `classic:Error` if the initialization fails, `()` otherwise
    public isolated function init(string url, *ConsumerConfiguration configurations) returns Error? {
        check validateConnectionConfigurations(configurations);
        return self.initConsumer(url, configurations);
    }

    isolated function initConsumer(string url, ConsumerConfiguration configurations) returns Error? = @java:Method {
        name: "init",
        'class: "io.ballerina.lib.activemq.consumer.Actions"
    } external;

    # Receives a message from the configured destination, waiting up to `timeoutMs` milliseconds.
    # Returns `()` if no message arrives within the timeout.
    #
    # ```ballerina
    # classic:Message? msg = check consumer->receive(5000);
    # record {|*classic:Message; string payload;|}? typedMsg = check consumer->receive(5000);
    # ```
    #
    # + timeoutMs - Maximum time in milliseconds to wait for a message
    # + T - Optional type description of the expected message shape. The `payload` field's
    #       declared type in `T` determines how the received message body is converted; leave
    #       unset to get the JMS-message-appropriate default representation
    # + return - The received message (as `T`), `()` on timeout, or `classic:Error` on failure
    #            or if the payload cannot be converted to the requested type
    isolated remote function receive(int timeoutMs = 5000,
            typedesc<Message> T = <>) returns T|Error? = @java:Method {
        'class: "io.ballerina.lib.activemq.consumer.Actions"
    } external;

    # Receives a message from the configured destination if one is immediately available, without
    # waiting. Returns `()` if no message is currently available.
    #
    # ```ballerina
    # classic:Message? msg = check consumer->receiveNoWait();
    # record {|*classic:Message; string payload;|}? typedMsg = check consumer->receiveNoWait();
    # ```
    #
    # + T - Optional type description of the expected message shape. The `payload` field's
    #       declared type in `T` determines how the received message body is converted; leave
    #       unset to get the JMS-message-appropriate default representation
    # + return - The received message (as `T`), `()` if none is available, or `classic:Error` on
    #            failure or if the payload cannot be converted to the requested type
    isolated remote function receiveNoWait(typedesc<Message> T = <>) returns T|Error? = @java:Method {
        'class: "io.ballerina.lib.activemq.consumer.Actions"
    } external;

    # Acknowledges a received message. Only meaningful when the consumer is configured with
    # `ackMode: CLIENT_ACKNOWLEDGE`. Acknowledging one message acknowledges all messages received
    # by this consumer's session up to and including it.
    #
    # ```ballerina
    # classic:Message? msg = check consumer->receive(5000);
    # if msg is classic:Message {
    #     check consumer->acknowledge(msg);
    # }
    # ```
    #
    # + message - The message to acknowledge
    # + return - `classic:Error` if acknowledgement fails, `()` otherwise
    isolated remote function acknowledge(Message message) returns Error? = @java:Method {
        'class: "io.ballerina.lib.activemq.consumer.Actions"
    } external;

    # Commits all messages received since the last commit or rollback. Only valid when the
    # consumer is configured with `ackMode: SESSION_TRANSACTED`.
    #
    # ```ballerina
    # check consumer->'commit();
    # ```
    #
    # + return - `classic:Error` if the commit fails, `()` otherwise
    isolated remote function 'commit() returns Error? = @java:Method {
        'class: "io.ballerina.lib.activemq.consumer.Actions"
    } external;

    # Rolls back all messages received since the last commit, making them available for
    # redelivery. Only valid when the consumer is configured with `ackMode: SESSION_TRANSACTED`.
    #
    # ```ballerina
    # check consumer->'rollback();
    # ```
    #
    # + return - `classic:Error` if the rollback fails, `()` otherwise
    isolated remote function 'rollback() returns Error? = @java:Method {
        'class: "io.ballerina.lib.activemq.consumer.Actions"
    } external;

    # Closes the consumer and its underlying JMS connection.
    #
    # ```ballerina
    # check consumer->close();
    # ```
    #
    # + return - `classic:Error` if closing fails, `()` otherwise
    isolated remote function close() returns Error? = @java:Method {
        'class: "io.ballerina.lib.activemq.consumer.Actions"
    } external;
}
