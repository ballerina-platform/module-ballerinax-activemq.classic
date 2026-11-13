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

package io.ballerina.lib.activemq.util;

import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;
import jakarta.jms.Connection;
import jakarta.jms.Destination;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TemporaryQueue;
import jakarta.jms.TextMessage;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQSslConnectionFactory;

import java.io.FileInputStream;
import java.security.KeyStore;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

/**
 * Test utility for producing messages to ActiveMQ queues and topics.
 * This is a simple producer implementation for testing purposes only.
 */
public final class TestProducer {

    private TestProducer() {
    }

    /**
     * Creates a ConnectionFactory based on the broker URL.
     * For SSL URLs, creates an ActiveMQSslConnectionFactory with trust configuration.
     *
     * @param brokerUrl The broker URL
     * @return Configured ConnectionFactory
     * @throws Exception If SSL configuration fails
     */
    private static ActiveMQConnectionFactory createConnectionFactory(String brokerUrl) throws Exception {
        if (brokerUrl.startsWith("ssl://")) {
            ActiveMQSslConnectionFactory sslFactory = new ActiveMQSslConnectionFactory(brokerUrl);

            String trustStorePath = "./tests/resources/secrets/client-truststore.p12";
            String trustStorePassword = "password";

            KeyStore trustStore = KeyStore.getInstance("PKCS12");
            try (FileInputStream is = new FileInputStream(trustStorePath)) {
                trustStore.load(is, trustStorePassword.toCharArray());
            }

            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(trustStore);

            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, tmf.getTrustManagers(), null);

            sslFactory.setTrustStore(trustStorePath);
            sslFactory.setTrustStorePassword(trustStorePassword);

            return sslFactory;
        } else {
            return new ActiveMQConnectionFactory(brokerUrl);
        }
    }

    /**
     * Send a text message to a queue.
     *
     * @param brokerUrl The broker URL (e.g., "tcp://localhost:61616")
     * @param queueName The queue name
     * @param message   The message text
     * @throws JMSException If message sending fails
     */
    public static void sendToQueue(BString brokerUrl, BString queueName, BString message) throws JMSException {
        sendToQueue(brokerUrl, queueName, message, null);
    }

    /**
     * Send a text message to a queue with properties.
     *
     * @param brokerUrl  The broker URL (e.g., "tcp://localhost:61616")
     * @param queueName  The queue name
     * @param message    The message text
     * @param properties Message properties (headers)
     * @throws JMSException If message sending fails
     */
    public static void sendToQueue(BString brokerUrl, BString queueName, BString message,
                                   BMap<BString, BString> properties) throws JMSException {
        Connection connection = null;
        Session session = null;
        MessageProducer producer = null;

        try {
            ActiveMQConnectionFactory connectionFactory = createConnectionFactory(brokerUrl.getValue());
            connection = connectionFactory.createConnection();
            connection.start();

            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue(queueName.getValue());
            producer = session.createProducer(destination);

            TextMessage textMessage = session.createTextMessage(message.getValue());

            if (properties != null) {
                for (BString key : properties.getKeys()) {
                    String value = properties.get(key).getValue();
                    textMessage.setStringProperty(key.getValue(), value);
                }
            }

            producer.send(textMessage);
        } catch (JMSException e) {
            throw e;
        } catch (Exception e) {
            throw new JMSException("Failed to create connection: " + e.getMessage());
        } finally {
            if (producer != null) {
                producer.close();
            }
            if (session != null) {
                session.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }

    /**
     * Send a text message to a topic.
     *
     * @param brokerUrl The broker URL (e.g., "tcp://localhost:61616")
     * @param topicName The topic name
     * @param message   The message text
     * @throws JMSException If message sending fails
     */
    public static void sendToTopic(BString brokerUrl, BString topicName, BString message) throws JMSException {
        sendToTopic(brokerUrl, topicName, message, null);
    }

    /**
     * Send a text message to a topic with properties.
     *
     * @param brokerUrl  The broker URL (e.g., "tcp://localhost:61616")
     * @param topicName  The topic name
     * @param message    The message text
     * @param properties Message properties (headers)
     * @throws JMSException If message sending fails
     */
    public static void sendToTopic(BString brokerUrl, BString topicName, BString message,
                                   BMap<BString, Object> properties) throws JMSException {
        Connection connection = null;
        Session session = null;
        MessageProducer producer = null;

        try {
            ActiveMQConnectionFactory connectionFactory = createConnectionFactory(brokerUrl.getValue());
            connection = connectionFactory.createConnection();
            connection.start();

            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createTopic(topicName.getValue());
            producer = session.createProducer(destination);

            TextMessage textMessage = session.createTextMessage(message.getValue());

            if (properties != null) {
                for (BString key : properties.getKeys()) {
                    Object value = properties.get(key);
                    textMessage.setObjectProperty(key.getValue(), value);
                }
            }

            producer.send(textMessage);
        } catch (JMSException e) {
            throw e;
        } catch (Exception e) {
            throw new JMSException("Failed to create connection: " + e.getMessage());
        } finally {
            if (producer != null) {
                producer.close();
            }
            if (session != null) {
                session.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }

    /**
     * Send a byte message to a queue.
     *
     * @param brokerUrl The broker URL (e.g., "tcp://localhost:61616")
     * @param queueName The queue name
     * @param payload   The message payload as byte array
     * @throws JMSException If message sending fails
     */
    public static void sendBytesToQueue(BString brokerUrl, BString queueName, BArray payload) throws JMSException {
        Connection connection = null;
        Session session = null;
        MessageProducer producer = null;

        try {
            ActiveMQConnectionFactory connectionFactory = createConnectionFactory(brokerUrl.getValue());
            connection = connectionFactory.createConnection();
            connection.start();

            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue(queueName.getValue());
            producer = session.createProducer(destination);

            jakarta.jms.BytesMessage bytesMessage = session.createBytesMessage();
            bytesMessage.writeBytes(payload.getBytes());

            producer.send(bytesMessage);
        } catch (JMSException e) {
            throw e;
        } catch (Exception e) {
            throw new JMSException("Failed to create connection: " + e.getMessage());
        } finally {
            if (producer != null) {
                producer.close();
            }
            if (session != null) {
                session.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }

    /**
     * Send an ObjectMessage to a queue. Used to test that message types the connector cannot map
     * to a Ballerina record (ObjectMessage/MapMessage/StreamMessage) don't hang the consuming
     * service's delivery loop.
     *
     * @param brokerUrl The broker URL (e.g., "tcp://localhost:61616")
     * @param queueName The queue name
     * @param payload   The serializable object payload
     * @throws JMSException If message sending fails
     */
    public static void sendObjectMessageToQueue(BString brokerUrl, BString queueName, BString payload)
            throws JMSException {
        Connection connection = null;
        Session session = null;
        MessageProducer producer = null;

        try {
            ActiveMQConnectionFactory connectionFactory = createConnectionFactory(brokerUrl.getValue());
            connection = connectionFactory.createConnection();
            connection.start();

            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue(queueName.getValue());
            producer = session.createProducer(destination);

            jakarta.jms.ObjectMessage objectMessage = session.createObjectMessage();
            objectMessage.setObject(new java.util.ArrayList<>(java.util.List.of(payload.getValue())));

            producer.send(objectMessage);
        } catch (JMSException e) {
            throw e;
        } catch (Exception e) {
            throw new JMSException("Failed to create connection: " + e.getMessage());
        } finally {
            if (producer != null) {
                producer.close();
            }
            if (session != null) {
                session.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }

    /**
     * Send a message with all JMS headers set for testing purposes.
     *
     * @param brokerUrl The broker URL
     * @param queueName The queue name
     * @param message   The message text
     * @throws JMSException If message sending fails
     */
    public static void sendMessageWithAllHeaders(BString brokerUrl, BString queueName, BString message)
            throws JMSException {
        Connection connection = null;
        Session session = null;
        MessageProducer producer = null;

        try {
            ActiveMQConnectionFactory connectionFactory = createConnectionFactory(brokerUrl.getValue());
            connection = connectionFactory.createConnection();
            connection.start();

            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue(queueName.getValue());
            producer = session.createProducer(destination);

            TextMessage textMessage = session.createTextMessage(message.getValue());

            textMessage.setJMSCorrelationID("test-correlation-id");
            textMessage.setJMSType("TestMessageType");
            textMessage.setJMSPriority(5);
            textMessage.setJMSReplyTo(destination);

            producer.send(textMessage, jakarta.jms.DeliveryMode.PERSISTENT, 5, 60000);
        } catch (JMSException e) {
            throw e;
        } catch (Exception e) {
            throw new JMSException("Failed to create connection: " + e.getMessage());
        } finally {
            if (producer != null) {
                producer.close();
            }
            if (session != null) {
                session.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }

    /**
     * Send a message with JMSType set.
     *
     * @param brokerUrl   The broker URL
     * @param queueName   The queue name
     * @param message     The message text
     * @param messageType The JMS message type
     * @throws JMSException If message sending fails
     */
    public static void sendMessageWithType(BString brokerUrl, BString queueName, BString message,
                                           BString messageType) throws JMSException {
        Connection connection = null;
        Session session = null;
        MessageProducer producer = null;

        try {
            ActiveMQConnectionFactory connectionFactory = createConnectionFactory(brokerUrl.getValue());
            connection = connectionFactory.createConnection();
            connection.start();

            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue(queueName.getValue());
            producer = session.createProducer(destination);

            TextMessage textMessage = session.createTextMessage(message.getValue());
            textMessage.setJMSType(messageType.getValue());

            producer.send(textMessage);
        } catch (JMSException e) {
            throw e;
        } catch (Exception e) {
            throw new JMSException("Failed to create connection: " + e.getMessage());
        } finally {
            if (producer != null) {
                producer.close();
            }
            if (session != null) {
                session.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }

    /**
     * Send a persistent message.
     *
     * @param brokerUrl The broker URL
     * @param queueName The queue name
     * @param message   The message text
     * @throws JMSException If message sending fails
     */
    public static void sendPersistentMessage(BString brokerUrl, BString queueName, BString message)
            throws JMSException {
        Connection connection = null;
        Session session = null;
        MessageProducer producer = null;

        try {
            ActiveMQConnectionFactory connectionFactory = createConnectionFactory(brokerUrl.getValue());
            connection = connectionFactory.createConnection();
            connection.start();

            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue(queueName.getValue());
            producer = session.createProducer(destination);

            TextMessage textMessage = session.createTextMessage(message.getValue());

            producer.send(textMessage, jakarta.jms.DeliveryMode.PERSISTENT,
                    jakarta.jms.Message.DEFAULT_PRIORITY, jakarta.jms.Message.DEFAULT_TIME_TO_LIVE);
        } catch (JMSException e) {
            throw e;
        } catch (Exception e) {
            throw new JMSException("Failed to create connection: " + e.getMessage());
        } finally {
            if (producer != null) {
                producer.close();
            }
            if (session != null) {
                session.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }

    /**
     * Send a non-persistent message.
     *
     * @param brokerUrl The broker URL
     * @param queueName The queue name
     * @param message   The message text
     * @throws JMSException If message sending fails
     */
    public static void sendNonPersistentMessage(BString brokerUrl, BString queueName, BString message)
            throws JMSException {
        Connection connection = null;
        Session session = null;
        MessageProducer producer = null;

        try {
            ActiveMQConnectionFactory connectionFactory = createConnectionFactory(brokerUrl.getValue());
            connection = connectionFactory.createConnection();
            connection.start();

            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue(queueName.getValue());
            producer = session.createProducer(destination);

            TextMessage textMessage = session.createTextMessage(message.getValue());

            producer.send(textMessage, jakarta.jms.DeliveryMode.NON_PERSISTENT,
                    jakarta.jms.Message.DEFAULT_PRIORITY, jakarta.jms.Message.DEFAULT_TIME_TO_LIVE);
        } catch (JMSException e) {
            throw e;
        } catch (Exception e) {
            throw new JMSException("Failed to create connection: " + e.getMessage());
        } finally {
            if (producer != null) {
                producer.close();
            }
            if (session != null) {
                session.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }

    /**
     * Sends a request with a real JMS TemporaryQueue as JMSReplyTo, then waits for a reply on
     * that same temporary queue. Used to verify that a responder preserves and reuses the
     * temporary destination's identity instead of recreating a same-named regular queue.
     *
     * @param brokerUrl       The broker URL
     * @param requestQueueName The queue to send the request to
     * @param requestPayload  The request message text
     * @param timeoutMs       How long to wait for a reply
     * @return The reply text, or "" if no reply arrived within the timeout or the round trip failed
     */
    public static BString sendRequestAndAwaitTemporaryReply(BString brokerUrl, BString requestQueueName,
            BString requestPayload, long timeoutMs) {
        Connection connection = null;
        try {
            ActiveMQConnectionFactory connectionFactory = createConnectionFactory(brokerUrl.getValue());
            connection = connectionFactory.createConnection();
            connection.start();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination requestQueue = session.createQueue(requestQueueName.getValue());
            TemporaryQueue replyQueue = session.createTemporaryQueue();
            MessageConsumer replyConsumer = session.createConsumer(replyQueue);

            MessageProducer producer = session.createProducer(requestQueue);
            TextMessage request = session.createTextMessage(requestPayload.getValue());
            request.setJMSReplyTo(replyQueue);
            producer.send(request);

            Message reply = replyConsumer.receive(timeoutMs);
            String replyText = reply instanceof TextMessage textReply ? textReply.getText() : "";
            return StringUtils.fromString(replyText);
        } catch (Exception e) {
            return StringUtils.fromString("");
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (JMSException ignored) {
                }
            }
        }
    }

    /**
     * Sends a text message with one property set to a type outside the 8 the connector's
     * {@code MessageMapper} explicitly handles (boolean, byte, short, int, long, float, double,
     * String) — a {@code java.util.List}, which ActiveMQ's {@code setObjectProperty} accepts as
     * a broker-level extension even though the JMS spec itself doesn't require support for it.
     * Used to verify that an unrecognized property type degrades to a best-effort string instead
     * of being silently dropped.
     *
     * @param brokerUrl The broker URL
     * @param queueName The queue name
     * @param message   The message text
     * @throws JMSException If message sending fails
     */
    public static void sendMessageWithUnsupportedPropertyType(BString brokerUrl, BString queueName, BString message)
            throws JMSException {
        Connection connection = null;
        Session session = null;
        MessageProducer producer = null;

        try {
            ActiveMQConnectionFactory connectionFactory = createConnectionFactory(brokerUrl.getValue());
            connection = connectionFactory.createConnection();
            connection.start();

            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue(queueName.getValue());
            producer = session.createProducer(destination);

            TextMessage textMessage = session.createTextMessage(message.getValue());
            textMessage.setObjectProperty("unsupportedProp", java.util.List.of("a", "b"));

            producer.send(textMessage);
        } catch (JMSException e) {
            throw e;
        } catch (Exception e) {
            throw new JMSException("Failed to create connection: " + e.getMessage());
        } finally {
            if (producer != null) {
                producer.close();
            }
            if (session != null) {
                session.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }

    /**
     * Sends a TextMessage with no body set - {@code TextMessage.getText()} legally returns null
     * for this. Used to verify a null body is handled instead of throwing a NullPointerException.
     *
     * @param brokerUrl The broker URL
     * @param queueName The queue name
     * @throws JMSException If message sending fails
     */
    public static void sendTextMessageWithNullBody(BString brokerUrl, BString queueName) throws JMSException {
        Connection connection = null;
        Session session = null;
        MessageProducer producer = null;

        try {
            ActiveMQConnectionFactory connectionFactory = createConnectionFactory(brokerUrl.getValue());
            connection = connectionFactory.createConnection();
            connection.start();

            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue(queueName.getValue());
            producer = session.createProducer(destination);

            TextMessage textMessage = session.createTextMessage();
            producer.send(textMessage);
        } catch (JMSException e) {
            throw e;
        } catch (Exception e) {
            throw new JMSException("Failed to create connection: " + e.getMessage());
        } finally {
            if (producer != null) {
                producer.close();
            }
            if (session != null) {
                session.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }
}
