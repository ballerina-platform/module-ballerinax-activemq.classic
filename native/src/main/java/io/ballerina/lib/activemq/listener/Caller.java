/*
 * Copyright (c) 2025, WSO2 LLC. (http://www.wso2.com)
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

import io.ballerina.lib.activemq.util.MessageMapper;
import io.ballerina.lib.activemq.util.SessionTransactionContext;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.transactions.TransactionResourceManager;
import jakarta.jms.JMSException;
import jakarta.jms.Message;

import java.util.Objects;

import static io.ballerina.lib.activemq.util.ActiveMQConstants.ACTIVEMQ_ERROR;
import static io.ballerina.lib.activemq.util.CommonUtils.createError;

/**
 * Native implementation of the Ballerina ActiveMQ Caller. Provides operations for message
 * acknowledgement and transaction management (commit/rollback).
 *
 * @since 0.1.0
 */
public class Caller {
    static final String NATIVE_TRANSACTION_CONTEXT = "native.transaction.context";

    private Caller() {
    }

    /**
     * Commits the JMS session. Not allowed inside a Ballerina transaction, or while one is using it.
     *
     * @param caller  the Ballerina caller object
     * @return null on success, BError on failure
     */
    public static Object commit(BObject caller) {
        return settleExplicitly(caller, "commit", "Error while committing the transaction: ",
                SessionTransactionContext::commitSession);
    }

    /**
     * Rolls back the JMS session. Not allowed inside a Ballerina transaction, or while one is using it.
     *
     * @param caller  the Ballerina caller object
     * @return null on success, BError on failure
     */
    public static Object rollback(BObject caller) {
        return settleExplicitly(caller, "roll back", "Error while rolling back the transaction: ",
                SessionTransactionContext::rollbackSession);
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
                return createError(ACTIVEMQ_ERROR, "Cannot acknowledge inside a transaction block: the service's "
                        + "session isn't transacted. Use `sessionAckMode: SESSION_TRANSACTED` to acknowledge in "
                        + "a transaction.");
            }
            if (message.getNativeData(MessageMapper.NATIVE_MESSAGE) != null
                    && message.getNativeData(NATIVE_TRANSACTION_CONTEXT) != transactionContext) {
                return createError(ACTIVEMQ_ERROR, "Cannot acknowledge a message that another service or consumer "
                        + "received.");
            }
            return transactionContext.joinCurrentTransaction();
        }
        try {
            Object nativeMessage = message.getNativeData(MessageMapper.NATIVE_MESSAGE);
            if (Objects.nonNull(nativeMessage)) {
                ((Message) nativeMessage).acknowledge();
            }
        } catch (JMSException exception) {
            return createError(ACTIVEMQ_ERROR,
                    String.format("Error occurred while sending acknowledgement for the message: %s",
                            exception.getMessage()), exception);
        }
        return null;
    }

    private static Object settleExplicitly(BObject caller, String operation, String errorPrefix,
                                           SessionTransactionContext.Settlement settlement) {
        SessionTransactionContext transactionContext =
                (SessionTransactionContext) caller.getNativeData(NATIVE_TRANSACTION_CONTEXT);
        if (transactionContext == null) {
            return createError(ACTIVEMQ_ERROR, errorPrefix + "Not a transacted session");
        }
        return transactionContext.settleExplicitly(operation, () -> {
            try {
                settlement.run(transactionContext);
            } catch (JMSException exception) {
                return createError(ACTIVEMQ_ERROR, errorPrefix + exception.getMessage(), exception);
            }
            return null;
        });
    }
}
