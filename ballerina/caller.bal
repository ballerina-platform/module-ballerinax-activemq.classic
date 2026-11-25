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

# Represents an ActiveMQ caller, which provides operations for message acknowledgement and
# transaction management.
public isolated client class Caller {

    # Acknowledges an ActiveMQ message. Outside a `transaction` block, this is only meaningful in
    # `CLIENT_ACKNOWLEDGE` mode, where acknowledging one message acknowledges every message the
    # session has consumed.
    #
    # ```ballerina
    # remote function onMessage(classic:Message message, classic:Caller caller) returns error? {
    #     // Process the message
    #     check caller->acknowledge(message);
    # }
    # ```
    #
    # Inside a Ballerina `transaction` block, a service with `sessionAckMode: SESSION_TRANSACTED`
    # joins the block instead. When the block commits, every message the service's session has
    # received since its last commit or rollback is acknowledged, together with the block's other
    # work. When it rolls back, those messages are redelivered under the redelivery policy. This
    # includes earlier messages that a previous `onMessage` call left unsettled: a JMS transaction
    # belongs to the session, not to the block.
    #
    # Acknowledge first, before any work that can fail. Acknowledging doesn't settle anything until
    # the block commits, so it's safe to call early. If the block fails before reaching it, the
    # session never joined the block, and the next commit on the session acknowledges the message
    # without it having been processed.
    #
    # ```ballerina
    # remote function onMessage(classic:Message message, classic:Caller caller) returns error? {
    #     transaction {
    #         check caller->acknowledge(message);
    #         check producer->send({payload: "processed"});
    #         check commit;
    #     }
    # }
    # ```
    #
    # + message - The ActiveMQ message to acknowledge
    # + return - `classic:Error` if acknowledgement fails, if a service whose session isn't
    #            transacted acknowledges inside a `transaction` block, or if the session is
    #            already part of another transaction; `()` otherwise
    isolated remote function acknowledge(Message message) returns Error? = @java:Method {
        'class: "io.ballerina.lib.activemq.listener.Caller"
    } external;

    # Commits all messages received in this transacted session and releases any locks currently
    # held. This operation is only valid when using `SESSION_TRANSACTED` acknowledgement mode.
    # After committing, a new transaction is automatically started. Not allowed inside a
    # `transaction` block, or while the session is part of one; the block's own commit settles it.
    #
    # ```ballerina
    # check caller->'commit();
    # ```
    #
    # + return - `classic:Error` if the commit fails, or if called inside a `transaction` block
    #            or while the session is part of one; `()` otherwise
    isolated remote function 'commit() returns Error? = @java:Method {
        'class: "io.ballerina.lib.activemq.listener.Caller"
    } external;

    # Rolls back any messages received in this transacted session and releases any locks currently
    # held. This operation is only valid when using `SESSION_TRANSACTED` acknowledgement mode.
    # Rolled back messages will be redelivered according to the configured redelivery policy. Not
    # allowed inside a `transaction` block, or while the session is part of one; the block's own
    # rollback settles it.
    #
    # ```ballerina
    # check caller->'rollback();
    # ```
    #
    # + return - `classic:Error` if the rollback fails, or if called inside a `transaction` block
    #            or while the session is part of one; `()` otherwise
    isolated remote function 'rollback() returns Error? = @java:Method {
        'class: "io.ballerina.lib.activemq.listener.Caller"
    } external;
}
