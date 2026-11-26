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

    # Acknowledges an ActiveMQ message. In `CLIENT_ACKNOWLEDGE` mode, this acknowledges every
    # message the session has consumed.
    #
    # ```ballerina
    # remote function onMessage(classic:Message message, classic:Caller caller) returns error? {
    #     // Process the message
    #     check caller->acknowledge(message);
    # }
    # ```
    #
    # Inside a `transaction` block, a service with `sessionAckMode: SESSION_TRANSACTED` joins the
    # block. Commit acknowledges every message the session has received, and rollback redelivers
    # them. The session joins the block only here, so call this first in the block, before any
    # conditional logic. Otherwise a failure or `rollback` before it doesn't cover the message.
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
    # + return - `classic:Error` if it fails, if the session isn't transacted inside a `transaction`
    #            block, or if the session is in another transaction, `()` otherwise
    isolated remote function acknowledge(Message message) returns Error? = @java:Method {
        'class: "io.ballerina.lib.activemq.listener.Caller"
    } external;

    # Commits all messages received in this transacted session and releases any locks currently
    # held. This operation is only valid when using `SESSION_TRANSACTED` acknowledgement mode.
    # After committing, a new transaction is automatically started. Not allowed inside a `transaction`
    # block, or while one is using the session.
    #
    # ```ballerina
    # check caller->'commit();
    # ```
    #
    # + return - `classic:Error` if the commit fails or isn't allowed, `()` otherwise
    isolated remote function 'commit() returns Error? = @java:Method {
        'class: "io.ballerina.lib.activemq.listener.Caller"
    } external;

    # Rolls back any messages received in this transacted session and releases any locks currently
    # held. This operation is only valid when using `SESSION_TRANSACTED` acknowledgement mode.
    # Rolled back messages will be redelivered according to the configured redelivery policy. Not
    # allowed inside a `transaction` block, or while one is using the session.
    #
    # ```ballerina
    # check caller->'rollback();
    # ```
    #
    # + return - `classic:Error` if the rollback fails or isn't allowed, `()` otherwise
    isolated remote function 'rollback() returns Error? = @java:Method {
        'class: "io.ballerina.lib.activemq.listener.Caller"
    } external;
}
