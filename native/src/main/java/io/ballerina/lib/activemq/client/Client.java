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

import io.ballerina.lib.activemq.listener.ConnectionConfig;
import io.ballerina.lib.activemq.listener.MessageMapper;
import io.ballerina.lib.activemq.listener.PrefetchPolicyConfig;
import io.ballerina.lib.activemq.listener.RedeliveryPolicyConfig;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import jakarta.jms.BytesMessage;
import jakarta.jms.Connection;
import jakarta.jms.DeliveryMode;
import jakarta.jms.Destination;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TemporaryQueue;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQPrefetchPolicy;
import org.apache.activemq.ActiveMQSslConnectionFactory;
import org.apache.activemq.RedeliveryPolicy;
import org.apache.activemq.command.ActiveMQTempQueue;
import org.apache.activemq.command.ActiveMQTempTopic;

import java.security.SecureRandom;
import java.util.Objects;
import java.util.Properties;

import javax.net.ssl.KeyManager;
import javax.net.ssl.TrustManager;

import static io.ballerina.lib.activemq.util.ActiveMQConstants.ACTIVEMQ_ERROR;
import static io.ballerina.lib.activemq.util.ActiveMQConstants.AMQ_SCHEDULED_CRON;
import static io.ballerina.lib.activemq.util.ActiveMQConstants.AMQ_SCHEDULED_DELAY;
import static io.ballerina.lib.activemq.util.ActiveMQConstants.AMQ_SCHEDULED_PERIOD;
import static io.ballerina.lib.activemq.util.ActiveMQConstants.AMQ_SCHEDULED_REPEAT;
import static io.ballerina.lib.activemq.util.ActiveMQConstants.BTRANSACTION_NAME;
import static io.ballerina.lib.activemq.util.ActiveMQConstants.CERT;
import static io.ballerina.lib.activemq.util.ActiveMQConstants.CORRELATION_ID;
import static io.ballerina.lib.activemq.util.ActiveMQConstants.EXPIRY_FIELD;
import static io.ballerina.lib.activemq.util.ActiveMQConstants.KEY;
import static io.ballerina.lib.activemq.util.ActiveMQConstants.MESSAGE_PAYLOAD;
import static io.ballerina.lib.activemq.util.ActiveMQConstants.MESSAGE_PROPERTIES;
import static io.ballerina.lib.activemq.util.ActiveMQConstants.PERSISTENT_FIELD;
import static io.ballerina.lib.activemq.util.ActiveMQConstants.PRIORITY_FIELD;
import static io.ballerina.lib.activemq.util.ActiveMQConstants.PROPERTIES;
import static io.ballerina.lib.activemq.util.ActiveMQConstants.REPLY_TO;
import static io.ballerina.lib.activemq.util.ActiveMQConstants.SCHEDULED_CRON;
import static io.ballerina.lib.activemq.util.ActiveMQConstants.SCHEDULED_DELAY;
import static io.ballerina.lib.activemq.util.ActiveMQConstants.SCHEDULED_PERIOD;
import static io.ballerina.lib.activemq.util.ActiveMQConstants.SCHEDULED_REPEAT;
import static io.ballerina.lib.activemq.util.ActiveMQConstants.TYPE_FIELD;
import static io.ballerina.lib.activemq.util.CommonUtils.createError;
import static io.ballerina.lib.activemq.util.ModuleUtils.getModule;
import static io.ballerina.lib.activemq.util.SslUtils.getKeyManagers;
import static io.ballerina.lib.activemq.util.SslUtils.getTrustmanagers;

/**
 * Native implementation of the Ballerina ActiveMQ Client. Provides synchronous send and receive
 * operations over a persistent JMS connection.
 *
 * @since 0.1.0
 */
public final class Client {

    static final String NATIVE_CONNECTION = "native.connection";
    // Prefix used in destination strings to indicate a JMS Topic vs Queue.
    static final String TOPIC_PREFIX = "topic://";

    private Client() {
    }

    /**
     * Initializes the ActiveMQ client by creating a JMS connection factory and starting the
     * connection.
     *
     * @param bClient        the Ballerina client object
     * @param url            the broker URL
     * @param configurations the connection configurations
     * @return null on success, BError on failure
     */
    @SuppressWarnings("unchecked")
    public static Object init(BObject bClient, BString url, BMap<BString, Object> configurations) {
        try {
            String brokerURL = url.getValue();
            ConnectionConfig config = new ConnectionConfig(configurations);
            ActiveMQConnectionFactory factory;

            if (Objects.nonNull(config.secureSocket())) {
                ActiveMQSslConnectionFactory sslFactory = new ActiveMQSslConnectionFactory(brokerURL);
                BMap<BString, Object> secureSocket = config.secureSocket();
                Object bCert = secureSocket.get(CERT);
                BMap<BString, BString> keyRecord = (BMap<BString, BString>) secureSocket.getMapValue(KEY);
                KeyManager[] keyManagers = getKeyManagers(keyRecord);
                TrustManager[] trustManagers = getTrustmanagers(bCert);
                sslFactory.setKeyAndTrustManagers(keyManagers, trustManagers, new SecureRandom());
                factory = sslFactory;
            } else {
                factory = new ActiveMQConnectionFactory(brokerURL);
            }

            String username = config.username();
            String password = config.password();
            if ((username != null && password == null) || (username == null && password != null)) {
                throw new IllegalArgumentException(
                        "Username and password must both be provided or both be omitted for anonymous access");
            }
            if (username != null) {
                factory.setUserName(username);
                factory.setPassword(password);
            }

            factory.setOptimizeAcknowledge(config.optimizeAcknowledgements());
            factory.setAlwaysSessionAsync(config.setAlwaysSessionAsync());

            if (config.prefetchPolicyConfig() != null) {
                factory.setPrefetchPolicy(buildPrefetchPolicy(config.prefetchPolicyConfig()));
            }
            if (config.redeliveryPolicyConfig() != null) {
                factory.setRedeliveryPolicy(buildRedeliveryPolicy(config.redeliveryPolicyConfig()));
            }
            factory.setProperties(buildConnectionProperties(configurations));

            Connection connection = factory.createConnection();
            connection.start();
            bClient.addNativeData(NATIVE_CONNECTION, connection);
        } catch (Exception e) {
            return createError(ACTIVEMQ_ERROR, "Failed to initialize client: " + e.getMessage(), e);
        }
        return null;
    }

    /**
     * Sends a Ballerina message to the specified destination. A new session is created for each
     * send and closed after the operation to ensure thread-safety.
     *
     * @param bClient     the Ballerina client object
     * @param destination the destination name; prefix with `"topic://"` for a JMS topic
     * @param bMessage    the Ballerina Message record to send
     * @return null on success, BError on failure
     */
    public static Object sendMessage(BObject bClient, BString destination, BMap<BString, Object> bMessage) {
        Connection connection = (Connection) bClient.getNativeData(NATIVE_CONNECTION);
        if (connection == null) {
            return createError(ACTIVEMQ_ERROR, "ActiveMQ client is not initialized");
        }
        try {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            try {
                Destination dest = toJmsDestination(session, destination.getValue());
                MessageProducer producer = session.createProducer(dest);
                try {
                    Message jmsMsg = toJmsMessage(session, bMessage);
                    producer.send(jmsMsg,
                            getDeliveryMode(bMessage),
                            getPriority(bMessage),
                            getTTL(bMessage));
                } finally {
                    producer.close();
                }
            } finally {
                session.close();
            }
        } catch (JMSException e) {
            return createError(ACTIVEMQ_ERROR, "Failed to send message: " + e.getMessage(), e);
        }
        return null;
    }

    /**
     * Receives a message from the specified destination synchronously, waiting up to
     * {@code timeoutMs} milliseconds. When {@code messageSelector} is non-null only
     * messages whose properties match the JMS selector expression are returned.
     * Returns null (Ballerina nil) on timeout.
     *
     * @param bClient         the Ballerina client object
     * @param destination     the destination name; prefix with {@code "topic://"} for a topic
     * @param timeoutMs       maximum wait time in milliseconds
     * @param messageSelector JMS selector expression (BString) or null/Ballerina-nil for none
     * @return BMap (Ballerina Message), null on timeout, or BError on failure
     */
    public static Object receiveMessage(BObject bClient, BString destination, long timeoutMs,
                                        Object messageSelector) {
        Connection connection = (Connection) bClient.getNativeData(NATIVE_CONNECTION);
        if (connection == null) {
            return createError(ACTIVEMQ_ERROR, "ActiveMQ client is not initialized");
        }
        try {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            try {
                Destination dest = toJmsDestination(session, destination.getValue());
                String selector = messageSelector instanceof BString bs ? bs.getValue() : null;
                MessageConsumer consumer = selector != null
                        ? session.createConsumer(dest, selector)
                        : session.createConsumer(dest);
                try {
                    Message jmsMsg = consumer.receive(timeoutMs);
                    if (jmsMsg == null) {
                        return null; // timeout — Ballerina nil
                    }
                    return MessageMapper.toBallerinaMessage(jmsMsg);
                } finally {
                    consumer.close();
                }
            } finally {
                session.close();
            }
        } catch (JMSException e) {
            return createError(ACTIVEMQ_ERROR, "Failed to receive message: " + e.getMessage(), e);
        }
    }

    /**
     * Implements the request-reply pattern. Creates a temporary reply queue, sends
     * the request message with its {@code JMSReplyTo} set to that queue, then blocks
     * for a reply. The temporary queue is deleted before this method returns.
     *
     * @param bClient     the Ballerina client object
     * @param destination the destination name for the request
     * @param bMessage    the Ballerina Message record to send as the request
     * @param timeoutMs   maximum wait time for the reply in milliseconds
     * @return BMap (Ballerina Message reply), null on timeout, or BError on failure
     */
    public static Object sendRequest(BObject bClient, BString destination,
                                     BMap<BString, Object> bMessage, long timeoutMs) {
        Connection connection = (Connection) bClient.getNativeData(NATIVE_CONNECTION);
        if (connection == null) {
            return createError(ACTIVEMQ_ERROR, "ActiveMQ client is not initialized");
        }
        try {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            try {
                TemporaryQueue replyQueue = session.createTemporaryQueue();
                try {
                    MessageConsumer replyConsumer = session.createConsumer(replyQueue);
                    try {
                        Destination dest = toJmsDestination(session, destination.getValue());
                        MessageProducer producer = session.createProducer(dest);
                        try {
                            Message jmsMsg = toJmsMessage(session, bMessage);
                            jmsMsg.setJMSReplyTo(replyQueue);
                            producer.send(jmsMsg, getDeliveryMode(bMessage),
                                    getPriority(bMessage), getTTL(bMessage));
                        } finally {
                            producer.close();
                        }
                        Message reply = replyConsumer.receive(timeoutMs);
                        if (reply == null) {
                            return null; // timeout — Ballerina nil
                        }
                        return MessageMapper.toBallerinaMessage(reply);
                    } finally {
                        replyConsumer.close();
                    }
                } finally {
                    replyQueue.delete();
                }
            } finally {
                session.close();
            }
        } catch (JMSException e) {
            return createError(ACTIVEMQ_ERROR, "Failed to send request: " + e.getMessage(), e);
        }
    }

    /**
     * Opens a new transacted JMS session and wraps it in a Ballerina Transaction object.
     * All sends on the transaction are buffered until commit() is called.
     *
     * @param bClient the Ballerina client object
     * @return a Ballerina Transaction object or BError on failure
     */
    public static Object beginTransaction(BObject bClient) {
        Connection connection = (Connection) bClient.getNativeData(NATIVE_CONNECTION);
        if (connection == null) {
            return createError(ACTIVEMQ_ERROR, "ActiveMQ client is not initialized");
        }
        try {
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            BObject bTransaction = ValueCreator.createObjectValue(getModule(), BTRANSACTION_NAME);
            bTransaction.addNativeData(Transaction.NATIVE_SESSION, session);
            return bTransaction;
        } catch (JMSException e) {
            return createError(ACTIVEMQ_ERROR, "Failed to begin transaction: " + e.getMessage(), e);
        }
    }

    /**
     * Stops and closes the underlying JMS connection.
     *
     * @param bClient the Ballerina client object
     * @return null on success, BError on failure
     */
    public static Object close(BObject bClient) {
        Connection connection = (Connection) bClient.getNativeData(NATIVE_CONNECTION);
        if (connection == null) {
            return null;
        }
        try {
            connection.stop();
            connection.close();
            bClient.addNativeData(NATIVE_CONNECTION, null);
        } catch (JMSException e) {
            return createError(ACTIVEMQ_ERROR, "Failed to close client: " + e.getMessage(), e);
        }
        return null;
    }

    // Destination string prefixes — keep in sync with MessageMapper.toDestinationString().
    static final String QUEUE_PREFIX = "queue://";
    static final String TEMP_QUEUE_PREFIX = "temp-queue://";
    static final String TEMP_TOPIC_PREFIX = "temp-topic://";

    /**
     * Creates a JMS Destination from a destination string. Recognised prefixes:
     * <ul>
     *   <li>{@code "topic://"} — JMS Topic</li>
     *   <li>{@code "temp-queue://"} — ActiveMQ TemporaryQueue (used for request-reply)</li>
     *   <li>{@code "temp-topic://"} — ActiveMQ TemporaryTopic</li>
     *   <li>{@code "queue://"} — regular JMS Queue (prefix stripped)</li>
     *   <li>anything else — treated as a plain queue name</li>
     * </ul>
     */
    static Destination toJmsDestination(Session session, String dest) throws JMSException {
        if (dest.startsWith(TOPIC_PREFIX)) {
            return session.createTopic(dest.substring(TOPIC_PREFIX.length()));
        }
        if (dest.startsWith(TEMP_QUEUE_PREFIX)) {
            return new ActiveMQTempQueue(dest.substring(TEMP_QUEUE_PREFIX.length()));
        }
        if (dest.startsWith(TEMP_TOPIC_PREFIX)) {
            return new ActiveMQTempTopic(dest.substring(TEMP_TOPIC_PREFIX.length()));
        }
        if (dest.startsWith(QUEUE_PREFIX)) {
            return session.createQueue(dest.substring(QUEUE_PREFIX.length()));
        }
        return session.createQueue(dest);
    }

    /**
     * Converts a Ballerina Message record to a JMS BytesMessage, mapping all relevant headers,
     * custom properties, and ActiveMQ scheduler properties when present.
     */
    @SuppressWarnings("unchecked")
    static Message toJmsMessage(Session session, BMap<BString, Object> bMsg) throws JMSException {
        BArray payload = (BArray) bMsg.get(MESSAGE_PAYLOAD);
        BytesMessage jmsMsg = session.createBytesMessage();
        jmsMsg.writeBytes(payload.getBytes());

        Object corrId = bMsg.get(CORRELATION_ID);
        if (corrId instanceof BString bCorrId) {
            jmsMsg.setJMSCorrelationID(bCorrId.getValue());
        }

        Object replyTo = bMsg.get(REPLY_TO);
        if (replyTo instanceof BString bReplyTo) {
            jmsMsg.setJMSReplyTo(toJmsDestination(session, bReplyTo.getValue()));
        }

        Object type = bMsg.get(TYPE_FIELD);
        if (type instanceof BString bType) {
            jmsMsg.setJMSType(bType.getValue());
        }

        Object propsObj = bMsg.get(MESSAGE_PROPERTIES);
        if (propsObj instanceof BMap<?, ?> rawProps) {
            @SuppressWarnings("unchecked")
            BMap<BString, Object> props = (BMap<BString, Object>) rawProps;
            for (BString key : props.getKeys()) {
                Object val = props.get(key);
                String propName = key.getValue();
                if (val instanceof BString bStr) {
                    jmsMsg.setStringProperty(propName, bStr.getValue());
                } else if (val instanceof Long l) {
                    jmsMsg.setLongProperty(propName, l);
                } else if (val instanceof Double d) {
                    jmsMsg.setDoubleProperty(propName, d);
                } else if (val instanceof Boolean b) {
                    jmsMsg.setBooleanProperty(propName, b);
                } else if (val != null) {
                    jmsMsg.setStringProperty(propName, val.toString());
                }
            }
        }

        // Scheduled delivery — ActiveMQ Classic scheduler properties.
        // These take effect only when schedulerSupport="true" is set in the broker.
        Object scheduledDelay = bMsg.get(SCHEDULED_DELAY);
        if (scheduledDelay instanceof Long l) {
            jmsMsg.setLongProperty(AMQ_SCHEDULED_DELAY, l);
        }
        Object scheduledPeriod = bMsg.get(SCHEDULED_PERIOD);
        if (scheduledPeriod instanceof Long l) {
            jmsMsg.setLongProperty(AMQ_SCHEDULED_PERIOD, l);
        }
        Object scheduledRepeat = bMsg.get(SCHEDULED_REPEAT);
        if (scheduledRepeat instanceof Long l) {
            jmsMsg.setIntProperty(AMQ_SCHEDULED_REPEAT, l.intValue());
        }
        Object scheduledCron = bMsg.get(SCHEDULED_CRON);
        if (scheduledCron instanceof BString bs) {
            jmsMsg.setStringProperty(AMQ_SCHEDULED_CRON, bs.getValue());
        }

        return jmsMsg;
    }

    static int getDeliveryMode(BMap<BString, Object> bMsg) {
        Object persistent = bMsg.get(PERSISTENT_FIELD);
        if (persistent instanceof Boolean b) {
            return b ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT;
        }
        return Message.DEFAULT_DELIVERY_MODE;
    }

    static int getPriority(BMap<BString, Object> bMsg) {
        if (bMsg.containsKey(PRIORITY_FIELD)) {
            Object priority = bMsg.get(PRIORITY_FIELD);
            if (priority instanceof Long l) {
                return l.intValue();
            }
        }
        return Message.DEFAULT_PRIORITY;
    }

    static long getTTL(BMap<BString, Object> bMsg) {
        if (bMsg.containsKey(EXPIRY_FIELD)) {
            Object expiry = bMsg.get(EXPIRY_FIELD);
            if (expiry instanceof Long l) {
                long ttl = l - System.currentTimeMillis();
                return Math.max(0L, ttl);
            }
        }
        return Message.DEFAULT_TIME_TO_LIVE;
    }

    private static ActiveMQPrefetchPolicy buildPrefetchPolicy(PrefetchPolicyConfig config) {
        ActiveMQPrefetchPolicy policy = new ActiveMQPrefetchPolicy();
        policy.setQueuePrefetch(config.queuePrefetchSize());
        policy.setTopicPrefetch(config.topicPrefetchSize());
        policy.setDurableTopicPrefetch(config.durableTopicPrefetchSize());
        policy.setOptimizeDurableTopicPrefetch(config.optimizeDurableTopicPrefetchSize());
        return policy;
    }

    private static RedeliveryPolicy buildRedeliveryPolicy(RedeliveryPolicyConfig config) {
        RedeliveryPolicy policy = new RedeliveryPolicy();
        policy.setCollisionAvoidancePercent(config.collisionAvoidancePercent());
        policy.setMaximumRedeliveries(config.maximumRedeliveries());
        policy.setMaximumRedeliveryDelay(config.maximumRedeliveryDelay());
        policy.setInitialRedeliveryDelay(config.initialRedeliveryDelay());
        policy.setUseCollisionAvoidance(config.useCollisionAvoidance());
        policy.setUseExponentialBackOff(config.useExponentialBackOff());
        policy.setBackOffMultiplier(config.backOffMultiplier());
        policy.setRedeliveryDelay(config.redeliveryDelay());
        policy.setPreDispatchCheck(config.preDispatchCheck());
        return policy;
    }

    @SuppressWarnings("unchecked")
    private static Properties buildConnectionProperties(BMap<BString, Object> configurations) {
        BMap<BString, BString> additionalProperties =
                (BMap<BString, BString>) configurations.getMapValue(PROPERTIES);
        Properties properties = new Properties();
        for (BString key : additionalProperties.getKeys()) {
            properties.put(key.getValue(), additionalProperties.getStringValue(key).getValue());
        }
        return properties;
    }
}
