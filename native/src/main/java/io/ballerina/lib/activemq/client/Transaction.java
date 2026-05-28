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

package io.ballerina.lib.activemq.client;

import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import jakarta.jms.Destination;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;

import static io.ballerina.lib.activemq.util.ActiveMQConstants.ACTIVEMQ_ERROR;
import static io.ballerina.lib.activemq.util.CommonUtils.createError;

/**
 * Native implementation of the Ballerina ActiveMQ Transaction client. Wraps a single
 * transacted JMS session so that multiple sends can be committed or rolled back atomically.
 *
 * @since 0.1.0
 */
public final class Transaction {

    static final String NATIVE_SESSION = "native.tx_session";

    private Transaction() {
    }

    /**
     * Sends a message within the transacted session. The message is not visible to consumers
     * until {@link #commit(BObject)} is called.
     *
     * @param bTransaction the Ballerina Transaction object
     * @param destination  the destination name; prefix with {@code "topic://"} for a JMS topic
     * @param bMessage     the Ballerina Message record to send
     * @return null on success, BError on failure
     */
    public static Object sendMessage(BObject bTransaction, BString destination,
                                     BMap<BString, Object> bMessage) {
        Session session = (Session) bTransaction.getNativeData(NATIVE_SESSION);
        if (session == null) {
            return createError(ACTIVEMQ_ERROR, "Transaction session is not initialized or already closed");
        }
        try {
            Destination dest = Client.toJmsDestination(session, destination.getValue());
            MessageProducer producer = session.createProducer(dest);
            try {
                Message jmsMsg = Client.toJmsMessage(session, bMessage);
                producer.send(jmsMsg, Client.getDeliveryMode(bMessage),
                        Client.getPriority(bMessage), Client.getTTL(bMessage));
            } finally {
                producer.close();
            }
        } catch (JMSException e) {
            return createError(ACTIVEMQ_ERROR,
                    "Failed to send message in transaction: " + e.getMessage(), e);
        }
        return null;
    }

    /**
     * Commits all messages sent since the last commit or rollback, delivering them atomically.
     *
     * @param bTransaction the Ballerina Transaction object
     * @return null on success, BError on failure
     */
    public static Object commit(BObject bTransaction) {
        Session session = (Session) bTransaction.getNativeData(NATIVE_SESSION);
        if (session == null) {
            return createError(ACTIVEMQ_ERROR, "Transaction session is not initialized or already closed");
        }
        try {
            session.commit();
        } catch (JMSException e) {
            return createError(ACTIVEMQ_ERROR, "Failed to commit transaction: " + e.getMessage(), e);
        }
        return null;
    }

    /**
     * Rolls back all messages sent since the last commit, discarding them.
     *
     * @param bTransaction the Ballerina Transaction object
     * @return null on success, BError on failure
     */
    public static Object rollback(BObject bTransaction) {
        Session session = (Session) bTransaction.getNativeData(NATIVE_SESSION);
        if (session == null) {
            return createError(ACTIVEMQ_ERROR, "Transaction session is not initialized or already closed");
        }
        try {
            session.rollback();
        } catch (JMSException e) {
            return createError(ACTIVEMQ_ERROR, "Failed to rollback transaction: " + e.getMessage(), e);
        }
        return null;
    }

    /**
     * Closes the transacted JMS session. Any uncommitted messages are implicitly rolled back.
     * Calling close on an already-closed transaction is a no-op.
     *
     * @param bTransaction the Ballerina Transaction object
     * @return null on success, BError on failure
     */
    public static Object close(BObject bTransaction) {
        Session session = (Session) bTransaction.getNativeData(NATIVE_SESSION);
        if (session == null) {
            return null; // idempotent — already closed
        }
        try {
            session.close();
        } catch (JMSException e) {
            return createError(ACTIVEMQ_ERROR,
                    "Failed to close transaction session: " + e.getMessage(), e);
        } finally {
            bTransaction.addNativeData(NATIVE_SESSION, null);
        }
        return null;
    }
}
