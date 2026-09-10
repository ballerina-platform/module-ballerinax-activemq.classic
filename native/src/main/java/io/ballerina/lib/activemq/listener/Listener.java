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

import io.ballerina.lib.activemq.util.ConnectionConfig;
import io.ballerina.lib.activemq.util.ConnectionFactoryUtils;
import io.ballerina.runtime.api.Environment;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.jms.Topic;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQMessageConsumer;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static io.ballerina.lib.activemq.util.ActiveMQConstants.ACTIVEMQ_ERROR;
import static io.ballerina.lib.activemq.util.ActiveMQConstants.QUERY_PARAM_EXCLUSIVE_CONSUMER;
import static io.ballerina.lib.activemq.util.CommonUtils.createError;
import static io.ballerina.lib.activemq.util.CommonUtils.getAcknowledgementMode;

/**
 * Native implementation of the Ballerina ActiveMQ Listener. This class manages JMS connections,
 * service attachments, and message consumer lifecycle.
 *
 * @since 0.1.0
 */
public final class Listener {
    static final String NATIVE_CONNECTION = "native.connection";
    static final String NATIVE_SERVICE_LIST = "native.service.list";
    static final String NATIVE_SERVICE = "native.service";
    static final String NATIVE_RECEIVER = "native.receiver";
    static final String LISTENER_STARTED = "listener.started";
    static final String DURABLE = "DURABLE";

    private Listener() {
    }

    /**
     * Initializes the ActiveMQ listener by creating a JMS connection factory and connection.
     *
     * @param bListener       the Ballerina listener object
     * @param url             the broker URL
     * @param configurations  the connection configurations
     * @return null on success, BError on failure
     */
    public static Object init(BObject bListener, BString url, BMap<BString, Object> configurations) {
        try {
            ConnectionConfig config = new ConnectionConfig(configurations);
            ActiveMQConnectionFactory factory = ConnectionFactoryUtils.createConnectionFactory(
                    url.getValue(), config);

            Connection connection = factory.createConnection();
            bListener.addNativeData(NATIVE_CONNECTION, connection);
            bListener.addNativeData(NATIVE_SERVICE_LIST, new ArrayList<BObject>());
        } catch (Exception e) {
            return createError(ACTIVEMQ_ERROR, String.format("Failed to initialize listener: %s", e.getMessage()), e);
        }
        return null;
    }

    /**
     * Attaches a Ballerina service to the listener. Creates a JMS session and consumer for the
     * service based on its configuration.
     *
     * @param env        the Ballerina runtime environment
     * @param bListener  the Ballerina listener object
     * @param bService   the Ballerina service to attach
     * @param name       reserved for future use (currently unused)
     * @return null on success, BError on failure
     */
    public static Object attach(Environment env, BObject bListener, BObject bService, Object name) {
        Connection connection = (Connection) bListener.getNativeData(NATIVE_CONNECTION);
        Object started = bListener.getNativeData(LISTENER_STARTED);
        Session session = null;
        MessageConsumer consumer = null;
        try {
            Service.validateService(bService);
            Service nativeService = new Service(bService);
            ServiceConfig svcConfig = nativeService.getServiceConfig();

            int sessionAckMode = getAcknowledgementMode(svcConfig.ackMode());
            session = connection.createSession(sessionAckMode);
            consumer = getConsumer(session, svcConfig);

            MessageDispatcher messageDispatcher = new MessageDispatcher(env.getRuntime(), nativeService, session);
            MessageReceiver receiver = new MessageReceiver(session, consumer, messageDispatcher);
            messageDispatcher.setReceiver(receiver);
            completeAttachment(Boolean.TRUE.equals(started), receiver, consumer, session, () -> {
                bService.addNativeData(NATIVE_SERVICE, nativeService);
                bService.addNativeData(NATIVE_RECEIVER, receiver);
                getBServices(bListener).add(bService);
            });
        } catch (BError | JMSException e) {
            String errorMsg = Objects.isNull(e.getMessage()) ? "Unknown error" : e.getMessage();
            return createError(ACTIVEMQ_ERROR, String.format("Failed to attach service to listener: %s", errorMsg), e);
        }
        return null;
    }

    static void completeAttachment(boolean started, MessageReceiver receiver, MessageConsumer consumer, Session session,
                                   Runnable publish) throws JMSException {
        try {
            if (started) {
                receiver.consume();
            }
            publish.run();
        } catch (JMSException | RuntimeException e) {
            closeAttachResources(consumer, session, e);
            throw e;
        }
    }

    private static void closeAttachResources(MessageConsumer consumer, Session session, Throwable cause) {
        if (consumer != null) {
            try {
                consumer.close();
            } catch (JMSException e) {
                cause.addSuppressed(e);
            }
        }
        if (session != null) {
            try {
                session.close();
            } catch (JMSException e) {
                cause.addSuppressed(e);
            }
        }
    }

    /**
     * Detaches a Ballerina service from the listener by stopping its message receiver and
     * removing it from the listener's bookkeeping.
     *
     * @param bListener  the Ballerina listener
     * @param bService   the Ballerina service to detach
     * @return null on success, BError on failure
     */
    public static Object detach(BObject bListener, BObject bService) {
        Object receiver = bService.getNativeData(NATIVE_RECEIVER);
        try {
            if (Objects.isNull(receiver)) {
                return createError(ACTIVEMQ_ERROR, "Could not find the native ActiveMQ message receiver");
            }
            ((MessageReceiver) receiver).stop();
        } catch (Exception e) {
            String errorMsg = Objects.isNull(e.getMessage()) ? "Unknown error" : e.getMessage();
            return createError(ACTIVEMQ_ERROR,
                    String.format("Failed to detach a service from the listener: %s", errorMsg), e);
        }
        getBServices(bListener).remove(bService);
        bService.addNativeData(NATIVE_SERVICE, null);
        bService.addNativeData(NATIVE_RECEIVER, null);
        return null;
    }

    /**
     * Starts the ActiveMQ listener by starting the JMS connection and initiating message
     * consumption for all attached services.
     *
     * @param bListener  the Ballerina listener object
     * @return null on success, BError on failure
     */
    public static Object start(BObject bListener) {
        Connection connection = (Connection) bListener.getNativeData(NATIVE_CONNECTION);
        List<BObject> bServices = getBServices(bListener);
        try {
            connection.start();
            for (BObject bService: bServices) {
                MessageReceiver receiver = (MessageReceiver) bService.getNativeData(NATIVE_RECEIVER);
                receiver.consume();
            }
            bListener.addNativeData(LISTENER_STARTED, Boolean.TRUE);
        } catch (JMSException e) {
            String errorMsg = Objects.isNull(e.getMessage()) ? "Unknown error" : e.getMessage();
            return createError(ACTIVEMQ_ERROR,
                    String.format("Error occurred while starting the Ballerina ActiveMQ listener: %s", errorMsg), e);
        }
        return null;
    }

    /**
     * Gracefully stops the ActiveMQ listener. Stops all message receivers first, then stops and
     * closes the JMS connection, allowing in-flight messages to complete processing.
     *
     * @param bListener  the Ballerina listener object
     * @return null on success, BError on failure
     */
    public static Object gracefulStop(BObject bListener) {
        Connection nativeConnection = (Connection) bListener.getNativeData(NATIVE_CONNECTION);
        List<BObject> bServices = getBServices(bListener);
        try {
            for (BObject bService: bServices) {
                MessageReceiver receiver = (MessageReceiver) bService.getNativeData(NATIVE_RECEIVER);
                receiver.stop();
            }
            nativeConnection.stop();
            nativeConnection.close();
        } catch (Exception e) {
            String errorMsg = Objects.isNull(e.getMessage()) ? "Unknown error" : e.getMessage();
            return createError(ACTIVEMQ_ERROR,
                    String.format(
                            "Error occurred while gracefully stopping the Ballerina ActiveMQ listener: %s", errorMsg),
                    e);
        }
        return null;
    }

    /**
     * Immediately stops the ActiveMQ listener. Stops all message receivers, then stops and closes
     * the JMS connection. In-flight messages may not complete processing.
     *
     * @param bListener  the Ballerina listener object
     * @return null on success, BError on failure
     */
    public static Object immediateStop(BObject bListener) {
        Connection nativeConnection = (Connection) bListener.getNativeData(NATIVE_CONNECTION);
        List<BObject> bServices = getBServices(bListener);
        try {
            for (BObject bService: bServices) {
                MessageReceiver receiver = (MessageReceiver) bService.getNativeData(NATIVE_RECEIVER);
                receiver.stop();
            }
            nativeConnection.stop();
            nativeConnection.close();
        } catch (Exception e) {
            String errorMsg = Objects.isNull(e.getMessage()) ? "Unknown error" : e.getMessage();
            return createError(ACTIVEMQ_ERROR,
                    String.format("Error occurred while immediately stopping the Ballerina ActiveMQ listener: %s",
                            errorMsg), e);
        }
        return null;
    }

    /**
     * Creates a JMS message consumer based on the service configuration (queue or topic).
     *
     * @param session    the JMS session
     * @param svcConfig  the service configuration
     * @return the configured message consumer
     * @throws JMSException if consumer creation fails
     */
    private static MessageConsumer getConsumer(Session session, ServiceConfig svcConfig)
            throws JMSException {
        MessageConsumer baseConsumer;
        if (svcConfig instanceof QueueConfig queueConfig) {
            String queueName = queueConfig.queueName();
            if (queueConfig.exclusive()) {
                queueName = queueName + QUERY_PARAM_EXCLUSIVE_CONSUMER;
            }
            Queue queue = session.createQueue(queueName);
            baseConsumer = session.createConsumer(queue, queueConfig.messageSelector());
        } else {
            TopicConfig topicConfig = (TopicConfig) svcConfig;
            String topicName = topicConfig.topicName();
            if (topicConfig.exclusive()) {
                topicName = topicName + QUERY_PARAM_EXCLUSIVE_CONSUMER;
            }
            Topic topic = session.createTopic(topicName);
            if (topicConfig.consumerType().equals(DURABLE)) {
                baseConsumer = session.createDurableSubscriber(
                        topic, topicConfig.subscriberName(), topicConfig.messageSelector(), topicConfig.noLocal());
            } else {
                baseConsumer = session.createConsumer(topic, topicConfig.messageSelector(), topicConfig.noLocal());
            }
        }
        ActiveMQMessageConsumer consumer = (ActiveMQMessageConsumer) baseConsumer;
        if (svcConfig.redeliveryPolicyConfig() != null) {
            consumer.setRedeliveryPolicy(ConnectionFactoryUtils.buildRedeliveryPolicy(
                    svcConfig.redeliveryPolicyConfig()));
        }
        return consumer;
    }

    /**
     * Retrieves the list of attached Ballerina services from the listener's native data.
     *
     * @param bListener  the Ballerina listener object
     * @return list of attached Ballerina service objects
     * @throws IllegalStateException if the native data is not a List<BObject>
     */
    @SuppressWarnings("unchecked")
    private static List<BObject> getBServices(BObject bListener) {
        Object nativeData = bListener.getNativeData(NATIVE_SERVICE_LIST);
        if (nativeData instanceof List<?>) {
            return (List<BObject>) nativeData;
        } else {
            throw new IllegalStateException("Expected List<BObject> but got: " +
                    (nativeData != null ? nativeData.getClass() : "null"));
        }
    }
}
