/*
 * Copyright (c) 2026, WSO2 LLC. (http://www.wso2.com)
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.ballerina.lib.activemq.listener;

import io.ballerina.lib.activemq.util.SessionTransactionContext;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.transactions.TransactionResourceManager;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.Session;

import java.util.Objects;

import static io.ballerina.lib.activemq.util.CommonUtils.createError;

/**
 * Native implementation of the Ballerina ActiveMQ Caller. Provides operations for message
 * acknowledgement and transaction management (commit/rollback).
 *
 * @since 0.1.0
 */
public class Caller {
    static final String NATIVE_MESSAGE = "native.message";
    static final String NATIVE_SESSION = "native.session";
    // Set only for a transacted session.
    static final String NATIVE_TRANSACTION_CONTEXT = "native.transaction.context";

    private Caller() {
    }

    /**
     * Commits the JMS session. Not allowed while a Ballerina transaction is using it.
     *
     * @param caller  the Ballerina caller object
     * @return null on success, BError on failure
     */
    public static Object commit(BObject caller) {
        return settleExplicitly(caller, "commit", "Error while committing the transaction: ", Session::commit);
    }

    /**
     * Rolls back the JMS session. Not allowed while a Ballerina transaction is using it.
     *
     * @param caller  the Ballerina caller object
     * @return null on success, BError on failure
     */
    public static Object rollback(BObject caller) {
        return settleExplicitly(caller, "roll back", "Error while rolling back the transaction: ",
                Session::rollback);
    }

    /**
     * Acknowledges a message. Inside a transaction, the session joins it and the commit acknowledges.
     *
     * @param caller   the Ballerina caller object
     * @param message  the Ballerina message record
     * @return null on success, BError on failure
     */
    public static Object acknowledge(BObject caller, BMap<BString, Object> message) {
        // Must run on the calling strand.
        if (TransactionResourceManager.getInstance().isInTransaction()) {
            SessionTransactionContext transactionContext =
                    (SessionTransactionContext) caller.getNativeData(NATIVE_TRANSACTION_CONTEXT);
            if (transactionContext == null) {
                return createError("Error", "Cannot acknowledge inside a transaction block: the service's session "
                        + "isn't transacted. Use `sessionAckMode: SESSION_TRANSACTED` to acknowledge in a "
                        + "transaction.");
            }
            return transactionContext.joinCurrentTransaction();
        }
        try {
            Object nativeMessage = message.getNativeData(NATIVE_MESSAGE);
            if (Objects.nonNull(nativeMessage)) {
                ((Message) nativeMessage).acknowledge();
            }
        } catch (JMSException exception) {
            return createError("Error",
                    String.format("Error occurred while sending acknowledgement for the message: %s",
                            exception.getMessage()), exception);
        }
        return null;
    }

    @FunctionalInterface
    private interface Settlement {
        void run(Session session) throws JMSException;
    }

    private static Object settleExplicitly(BObject caller, String verb, String errorPrefix, Settlement settlement) {
        Session nativeSession = (Session) caller.getNativeData(NATIVE_SESSION);
        SessionTransactionContext transactionContext =
                (SessionTransactionContext) caller.getNativeData(NATIVE_TRANSACTION_CONTEXT);
        // Null if the session isn't transacted. The JMS client then rejects the call.
        if (transactionContext != null) {
            BError notAllowed = transactionContext.beginExplicitSettlement(verb);
            if (notAllowed != null) {
                return notAllowed;
            }
        }
        try {
            settlement.run(nativeSession);
        } catch (JMSException exception) {
            return createError("Error", errorPrefix + exception.getMessage(), exception);
        } finally {
            if (transactionContext != null) {
                transactionContext.endOutsideCall();
            }
        }
        return null;
    }
}
