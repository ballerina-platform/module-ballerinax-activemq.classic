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

# Represents a transacted JMS session for atomic multi-message sends.
# Obtain a `Transaction` by calling `mqClient->'transaction()`.
# All `sendMessage` calls are buffered until `'commit()` delivers them atomically.
# Always call `close()` when the transaction is no longer needed — even after an
# error — to release the underlying JMS session.
public isolated client class Transaction {

    # Sends a message within this transaction. The message is not visible to
    # consumers until `'commit()` is called. Calling `'rollback()` or `close()`
    # without committing discards all unsent messages.
    #
    # + destination - Queue name or `"topic://topicName"` to send the message to
    # + message - The message to enqueue in this transaction
    # + return - `activemq:Error` if sending fails, `()` otherwise
    isolated remote function sendMessage(string destination, Message message) returns Error? = @java:Method {
        'class: "io.ballerina.lib.activemq.client.Transaction"
    } external;

    # Commits all messages sent since the last commit or rollback, delivering
    # them atomically to their destinations. A new transaction starts automatically
    # on the same session after a successful commit.
    #
    # + return - `activemq:Error` if the commit fails, `()` otherwise
    isolated remote function 'commit() returns Error? = @java:Method {
        'class: "io.ballerina.lib.activemq.client.Transaction"
    } external;

    # Rolls back all messages sent since the last commit, discarding them without
    # delivering them. A new transaction starts automatically on the same session
    # after a rollback.
    #
    # + return - `activemq:Error` if the rollback fails, `()` otherwise
    isolated remote function 'rollback() returns Error? = @java:Method {
        'class: "io.ballerina.lib.activemq.client.Transaction"
    } external;

    # Closes the underlying JMS session and releases broker resources. Any
    # uncommitted messages are implicitly rolled back. Calling `close()` more
    # than once is safe (idempotent).
    #
    # + return - `activemq:Error` if closing fails, `()` otherwise
    isolated remote function close() returns Error? = @java:Method {
        'class: "io.ballerina.lib.activemq.client.Transaction"
    } external;
}
