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

package io.ballerina.lib.activemq.consumer;

import io.ballerina.lib.activemq.util.ActiveMQDatabindingException;
import io.ballerina.lib.activemq.util.ConnectionFactoryUtils;
import io.ballerina.lib.activemq.util.MessageMapper;
import io.ballerina.lib.activemq.util.SessionResourceUtils;
import io.ballerina.lib.activemq.util.SessionTransactionContext;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.api.values.BTypedesc;
import io.ballerina.runtime.transactions.TransactionResourceManager;
import jakarta.jms.Connection;
import jakarta.jms.Destination;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Session;
import org.apache.activemq.ActiveMQConnectionFactory;

import static io.ballerina.lib.activemq.util.ActiveMQConstants.ACTIVEMQ_ERROR;
import static io.ballerina.lib.activemq.util.CommonUtils.createError;
import static io.ballerina.lib.activemq.util.CommonUtils.getAcknowledgementMode;

/** Native MessageConsumer impl; sessionLock serializes session-level calls since a JMS Session isn't thread-safe. */
public final class Actions {

    private Actions() {
    }

    static final String NATIVE_STATE = "native.state";

    private static final class ConsumerState {
        final Connection connection;
        final MessageConsumer consumer;
        final Object sessionLock = new Object();
        // Non-null only for a transacted consumer.
        final SessionTransactionContext transactionContext;
        volatile boolean closed = false;

        ConsumerState(Connection connection, Session session, MessageConsumer consumer, boolean transacted) {
            this.connection = connection;
            this.consumer = consumer;
            this.transactionContext = transacted
                    ? new SessionTransactionContext(session, sessionLock, "consumer", true) : null;
        }
    }

    public static Object init(BObject bConsumer, BString url, BMap<BString, Object> configurations) {
        Connection connection = null;
        Session session = null;
        try {
            ConsumerConfig config = new ConsumerConfig(configurations);
            ActiveMQConnectionFactory factory =
                    ConnectionFactoryUtils.createConnectionFactory(url.getValue(), config.connectionConfig());
            connection = factory.createConnection();
            connection.start();
            int sessionMode = getAcknowledgementMode(config.ackMode());
            boolean transacted = sessionMode == Session.SESSION_TRANSACTED;
            session = connection.createSession(sessionMode);
            Destination destination = MessageMapper.toJmsDestination(session, config.destination());
            MessageConsumer consumer = config.messageSelector() != null
                    ? session.createConsumer(destination, config.messageSelector())
                    : session.createConsumer(destination);
            ConsumerState state = new ConsumerState(connection, session, consumer, transacted);
            bConsumer.addNativeData(NATIVE_STATE, state);
        } catch (Exception e) {
            SessionResourceUtils.cleanupOnInitFailure(connection, session);
            return createError(ACTIVEMQ_ERROR, "Failed to initialize consumer: " + e.getMessage(), e);
        }
        return null;
    }

    public static Object receive(BObject bConsumer, long timeoutMs, BTypedesc bTypedesc) {
        if (timeoutMs < 0) {
            return createError(ACTIVEMQ_ERROR,
                    "Invalid timeout: " + timeoutMs + " ms. The timeout must not be negative");
        }
        return receiveFrom(bConsumer, bTypedesc, state -> state.consumer.receive(timeoutMs));
    }

    public static Object receiveNoWait(BObject bConsumer, BTypedesc bTypedesc) {
        return receiveFrom(bConsumer, bTypedesc, state -> state.consumer.receiveNoWait());
    }

    @FunctionalInterface
    private interface BlockingReceive {
        Message run(ConsumerState state) throws JMSException;
    }

    private static Object receiveFrom(BObject bConsumer, BTypedesc bTypedesc, BlockingReceive blockingReceive) {
        ConsumerState state = (ConsumerState) bConsumer.getNativeData(NATIVE_STATE);
        BError notOpen = checkOpen(state);
        if (notOpen != null) {
            return notOpen;
        }
        if (state.transactionContext == null) {
            return doReceive(state, bTypedesc, blockingReceive);
        }
        // Must run on the calling strand. Join before receiving, so that a later failure rolls the message back.
        if (TransactionResourceManager.getInstance().isInTransaction()) {
            BError joinError = state.transactionContext.joinCurrentTransaction();
            return joinError != null ? joinError : doReceive(state, bTypedesc, blockingReceive);
        }
        return state.transactionContext.receiveOutside(() -> doReceive(state, bTypedesc, blockingReceive));
    }

    private static Object doReceive(ConsumerState state, BTypedesc bTypedesc, BlockingReceive blockingReceive) {
        try {
            Message jmsMsg;
            synchronized (state.sessionLock) {
                jmsMsg = blockingReceive.run(state);
            }
            if (jmsMsg == null) {
                return null;
            }
            BMap<BString, Object> bMsg = MessageMapper.toBallerinaMessage(jmsMsg, bTypedesc);
            bMsg.addNativeData(NATIVE_STATE, state);
            return bMsg;
        } catch (JMSException e) {
            synchronized (state) {
                if (state.closed) {
                    return null;
                }
            }
            return createError(ACTIVEMQ_ERROR, "Failed to receive message: " + e.getMessage(), e);
        } catch (ActiveMQDatabindingException e) {
            return createError(ACTIVEMQ_ERROR, e.getMessage(), e);
        }
    }

    /** Acknowledges a message. Inside a transaction, the consumer joins it and the commit acknowledges. */
    public static Object acknowledge(BObject bConsumer, BMap<BString, Object> message) {
        // Must run on the calling strand.
        if (TransactionResourceManager.getInstance().isInTransaction()) {
            return joinTransaction(bConsumer, message);
        }
        Object nativeMessage = message.getNativeData(MessageMapper.NATIVE_MESSAGE);
        if (!(nativeMessage instanceof Message jmsMsg)) {
            return null;
        }
        Object nativeState = message.getNativeData(NATIVE_STATE);
        if (nativeState instanceof ConsumerState state) {
            synchronized (state.sessionLock) {
                return doAcknowledge(jmsMsg);
            }
        }
        return doAcknowledge(jmsMsg);
    }

    private static Object doAcknowledge(Message jmsMsg) {
        try {
            jmsMsg.acknowledge();
        } catch (JMSException e) {
            return createError(ACTIVEMQ_ERROR, "Failed to acknowledge message: " + e.getMessage(), e);
        }
        return null;
    }

    public static Object commit(BObject bConsumer) {
        return settleExplicitly(bConsumer, "commit", "commit", SessionTransactionContext::commitSession);
    }

    public static Object rollback(BObject bConsumer) {
        return settleExplicitly(bConsumer, "rollback", "roll back", SessionTransactionContext::rollbackSession);
    }

    public static Object close(BObject bConsumer) {
        ConsumerState state = (ConsumerState) bConsumer.getNativeData(NATIVE_STATE);
        if (state == null) {
            return null;
        }
        return SessionResourceUtils.close(state, state.connection, () -> state.closed,
                () -> state.closed = true, "consumer");
    }

    @FunctionalInterface
    private interface JmsAction {
        Object run(ConsumerState state) throws JMSException;
    }

    private static Object execute(BObject bConsumer, String operation, JmsAction action) {
        ConsumerState state = (ConsumerState) bConsumer.getNativeData(NATIVE_STATE);
        if (state == null) {
            return createError(ACTIVEMQ_ERROR, "ActiveMQ consumer is not initialized");
        }
        return SessionResourceUtils.execute(state, state.sessionLock, () -> state.closed, "consumer", operation,
                () -> action.run(state));
    }

    /** Joins the caller's transaction, or returns an error if the consumer can't. */
    private static Object joinTransaction(BObject bConsumer, BMap<BString, Object> message) {
        ConsumerState state = (ConsumerState) bConsumer.getNativeData(NATIVE_STATE);
        BError notOpen = checkOpen(state);
        if (notOpen != null) {
            return notOpen;
        }
        if (state.transactionContext == null) {
            return createError(ACTIVEMQ_ERROR, "Cannot acknowledge inside a transaction block: the consumer isn't "
                    + "transacted. Create the consumer with `ackMode: SESSION_TRANSACTED` to acknowledge in a "
                    + "transaction.");
        }
        if (message.getNativeData(NATIVE_STATE) instanceof ConsumerState receiver && receiver != state) {
            return createError(ACTIVEMQ_ERROR, "Cannot acknowledge a message that was received by another consumer. "
                    + "Acknowledge it with the consumer that received it.");
        }
        return state.transactionContext.joinCurrentTransaction();
    }

    private static Object settleExplicitly(BObject bConsumer, String method, String verb,
                                           SessionTransactionContext.Settlement settlement) {
        ConsumerState state = (ConsumerState) bConsumer.getNativeData(NATIVE_STATE);
        if (state == null || state.transactionContext == null) {
            return execute(bConsumer, method + " transaction", consumerState -> {
                throw new JMSException("'" + method
                        + "' is only valid when the consumer is configured with ackMode: SESSION_TRANSACTED");
            });
        }
        return state.transactionContext.settleExplicitly(verb,
                () -> execute(bConsumer, method + " transaction", consumerState -> {
                    settlement.run(consumerState.transactionContext);
                    return null;
                }));
    }

    private static BError checkOpen(ConsumerState state) {
        if (state == null) {
            return createError(ACTIVEMQ_ERROR, "ActiveMQ consumer is not initialized");
        }
        synchronized (state) {
            return state.closed ? createError(ACTIVEMQ_ERROR, "ActiveMQ consumer is already closed") : null;
        }
    }
}
