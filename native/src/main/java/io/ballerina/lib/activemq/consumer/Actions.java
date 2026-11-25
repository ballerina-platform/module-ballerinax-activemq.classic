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
        final Session session;
        final MessageConsumer consumer;
        final boolean transacted;
        final Object sessionLock = new Object();
        // Non-null only for a transacted consumer.
        final SessionTransactionContext transactionContext;
        volatile boolean closed = false;

        ConsumerState(Connection connection, Session session, MessageConsumer consumer, boolean transacted) {
            this.connection = connection;
            this.session = session;
            this.consumer = consumer;
            this.transacted = transacted;
            this.transactionContext = transacted
                    ? new SessionTransactionContext(session, sessionLock, "consumer") : null;
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
        if (state == null) {
            return createError(ACTIVEMQ_ERROR, "ActiveMQ consumer is not initialized");
        }
        synchronized (state) {
            if (state.closed) {
                return createError(ACTIVEMQ_ERROR, "ActiveMQ consumer is already closed");
            }
        }
        // Must run here, on the calling strand. A transacted consumer joins the caller's transaction before it
        // receives, so a failure anywhere after the receive rolls the message back with the block.
        if (state.transactionContext != null && TransactionResourceManager.getInstance().isInTransaction()) {
            BError joinError = state.transactionContext.joinCurrentTransaction();
            if (joinError != null) {
                return joinError;
            }
        }
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

    /**
     * Acknowledges a received message. Inside a Ballerina transaction block, a transacted consumer's session joins
     * the block instead: the block's commit acknowledges every message the session has received, and its rollback
     * has them redelivered. A consumer that isn't transacted can't acknowledge inside a block, because the
     * acknowledgement couldn't be rolled back with it.
     */
    public static Object acknowledge(BObject bConsumer, BMap<BString, Object> message) {
        // Must run here, on the calling strand: the transaction state belongs to that strand.
        if (TransactionResourceManager.getInstance().isInTransaction()) {
            return joinTransaction(bConsumer);
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
        BError notAllowed = checkExplicitSettlementAllowed(bConsumer, "commit");
        if (notAllowed != null) {
            return notAllowed;
        }
        return execute(bConsumer, "commit transaction", state -> {
            if (!state.transacted) {
                throw new JMSException(
                        "'commit' is only valid when the consumer is configured with ackMode: SESSION_TRANSACTED");
            }
            state.session.commit();
            return null;
        });
    }

    public static Object rollback(BObject bConsumer) {
        BError notAllowed = checkExplicitSettlementAllowed(bConsumer, "roll back");
        if (notAllowed != null) {
            return notAllowed;
        }
        return execute(bConsumer, "rollback transaction", state -> {
            if (!state.transacted) {
                throw new JMSException(
                        "'rollback' is only valid when the consumer is configured with ackMode: SESSION_TRANSACTED");
            }
            state.session.rollback();
            return null;
        });
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

    /** Joins a transacted consumer's session to the caller's transaction, or rejects a non-transacted one. */
    private static Object joinTransaction(BObject bConsumer) {
        ConsumerState state = (ConsumerState) bConsumer.getNativeData(NATIVE_STATE);
        if (state == null) {
            return createError(ACTIVEMQ_ERROR, "ActiveMQ consumer is not initialized");
        }
        if (state.closed) {
            return createError(ACTIVEMQ_ERROR, "ActiveMQ consumer is already closed");
        }
        if (state.transactionContext == null) {
            return createError(ACTIVEMQ_ERROR, "Cannot acknowledge inside a transaction block: the consumer isn't "
                    + "transacted. Create the consumer with `ackMode: SESSION_TRANSACTED` to acknowledge in a "
                    + "transaction.");
        }
        return state.transactionContext.joinCurrentTransaction();
    }

    private static BError checkExplicitSettlementAllowed(BObject bConsumer, String operation) {
        ConsumerState state = (ConsumerState) bConsumer.getNativeData(NATIVE_STATE);
        if (state == null || state.transactionContext == null) {
            return null;
        }
        return state.transactionContext.checkExplicitSettlementAllowed(operation);
    }
}
