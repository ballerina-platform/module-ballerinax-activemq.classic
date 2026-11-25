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

package io.ballerina.lib.activemq.producer;

import io.ballerina.lib.activemq.util.ConnectionFactoryUtils;
import io.ballerina.lib.activemq.util.MessageMapper;
import io.ballerina.lib.activemq.util.SessionResourceUtils;
import io.ballerina.lib.activemq.util.SessionTransactionContext;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.transactions.TransactionResourceManager;
import jakarta.jms.Connection;
import jakarta.jms.Destination;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import org.apache.activemq.ActiveMQConnectionFactory;

import static io.ballerina.lib.activemq.util.ActiveMQConstants.ACTIVEMQ_ERROR;
import static io.ballerina.lib.activemq.util.CommonUtils.createError;

/** Native MessageProducer impl; sessionLock serializes session-level calls since a JMS Session isn't thread-safe. */
public final class Actions {

    private Actions() {
    }

    static final String NATIVE_STATE = "native.state";

    private static final class ProducerState {
        final Connection connection;
        final Session session;
        final MessageProducer producer;
        final BMap<BString, Object> defaultDestination;
        final Object sessionLock = new Object();
        // Non-null only for a transacted producer.
        final SessionTransactionContext transactionContext;
        volatile boolean closed = false;

        ProducerState(Connection connection, Session session, MessageProducer producer, boolean transacted,
                      BMap<BString, Object> defaultDestination) {
            this.connection = connection;
            this.session = session;
            this.producer = producer;
            this.defaultDestination = defaultDestination;
            this.transactionContext = transacted
                    ? new SessionTransactionContext(session, sessionLock, "producer", false) : null;
        }
    }

    public static Object init(BObject bProducer, BString url, BMap<BString, Object> configurations) {
        Connection connection = null;
        Session session = null;
        try {
            ProducerConfig config = new ProducerConfig(configurations);
            ActiveMQConnectionFactory factory =
                    ConnectionFactoryUtils.createConnectionFactory(url.getValue(), config.connectionConfig());
            connection = factory.createConnection();
            connection.start();
            int sessionMode = config.transacted() ? Session.SESSION_TRANSACTED : Session.AUTO_ACKNOWLEDGE;
            session = connection.createSession(sessionMode);
            // A null destination creates a JMS unidentified producer.
            MessageProducer producer = session.createProducer(null);
            ProducerState state = new ProducerState(
                    connection, session, producer, config.transacted(), config.destination());
            bProducer.addNativeData(NATIVE_STATE, state);
        } catch (Exception e) {
            SessionResourceUtils.cleanupOnInitFailure(connection, session);
            return createError(ACTIVEMQ_ERROR, "Failed to initialize producer: " + e.getMessage(), e);
        }
        return null;
    }

    public static Object send(BObject bProducer, BMap<BString, Object> bMessage, Object destinationObj) {
        ProducerState producerState = (ProducerState) bProducer.getNativeData(NATIVE_STATE);
        if (producerState != null && !producerState.closed) {
            // Must run on the calling strand.
            BError transactionError = joinTransactionIfAny(producerState);
            if (transactionError != null) {
                return transactionError;
            }
        }
        return execute(bProducer, "send message", state -> {
            BMap<BString, Object> destination = destinationObj instanceof BMap<?, ?> rawDest
                    ? castToDestinationMap(rawDest) : state.defaultDestination;
            if (destination == null) {
                throw new JMSException(
                        "No destination specified for this send call, and no default destination configured");
            }
            Destination dest = MessageMapper.toJmsDestination(state.session, destination);
            Message jmsMsg = MessageMapper.toJmsMessage(state.session, bMessage);
            state.producer.send(dest, jmsMsg, MessageMapper.getDeliveryMode(bMessage),
                    MessageMapper.getPriority(bMessage), MessageMapper.getTTL(bMessage));
            return null;
        });
    }

    /** Joins the caller's transaction. A producer must be transacted inside one, and not transacted outside. */
    private static BError joinTransactionIfAny(ProducerState state) {
        boolean inTransaction = TransactionResourceManager.getInstance().isInTransaction();
        SessionTransactionContext transactionContext = state.transactionContext;
        if (transactionContext == null) {
            return inTransaction
                    ? createError(ACTIVEMQ_ERROR, "Cannot send inside a transaction block: the producer isn't "
                            + "transacted. Create the producer with `transacted = true` to send in a transaction.")
                    : null;
        }
        if (!inTransaction) {
            return createError(ACTIVEMQ_ERROR, "A transacted producer can only send inside a transaction block. "
                    + "Its messages are delivered when the block commits.");
        }
        return transactionContext.joinCurrentTransaction();
    }

    public static Object close(BObject bProducer) {
        ProducerState state = (ProducerState) bProducer.getNativeData(NATIVE_STATE);
        if (state == null) {
            return null;
        }
        return SessionResourceUtils.close(state, state.connection, () -> state.closed,
                () -> state.closed = true, "producer");
    }

    @SuppressWarnings("unchecked")
    private static BMap<BString, Object> castToDestinationMap(BMap<?, ?> rawDest) {
        return (BMap<BString, Object>) rawDest;
    }

    @FunctionalInterface
    private interface JmsAction {
        Object run(ProducerState state) throws JMSException;
    }

    private static Object execute(BObject bProducer, String operation, JmsAction action) {
        ProducerState state = (ProducerState) bProducer.getNativeData(NATIVE_STATE);
        if (state == null) {
            return createError(ACTIVEMQ_ERROR, "ActiveMQ producer is not initialized");
        }
        return SessionResourceUtils.execute(state, state.sessionLock, () -> state.closed, "producer", operation,
                () -> action.run(state));
    }
}
