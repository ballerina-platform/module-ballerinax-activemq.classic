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

import jakarta.jms.IllegalStateException;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageListener;
import jakarta.jms.Session;

import java.io.PrintStream;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A message receiver that registers as a native JMS {@code MessageListener} and dispatches
 * delivered messages to the Ballerina service using the message dispatcher. {@code onMessage}
 * dispatches synchronously, so a single JMS session's inherent serial-delivery guarantee keeps
 * messages for this service processed one at a time.
 *
 * @since 0.1.0
 */
public class MessageReceiver implements MessageListener {
    private static final long STOP_TIMEOUT_MS = 30000;
    private static final PrintStream ERR_OUT = System.err;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final Session session;
    private final MessageConsumer consumer;
    private final MessageDispatcher messageDispatcher;

    /**
     * Creates a new message receiver.
     *
     * @param session            the JMS session
     * @param consumer           the JMS message consumer
     * @param messageDispatcher  the dispatcher for delivering messages to Ballerina service
     */
    public MessageReceiver(Session session, MessageConsumer consumer, MessageDispatcher messageDispatcher) {
        this.session = session;
        this.consumer = consumer;
        this.messageDispatcher = messageDispatcher;
    }

    /**
     * Invoked by the JMS provider when a message is delivered. Dispatches the message to the
     * Ballerina service synchronously on this delivery thread.
     * <p>
     * Every delivered message is dispatched, even one that arrives while {@link #stop()} is running.
     * Returning without dispatching would make the provider treat the message as consumed, so in
     * {@code AUTO_ACKNOWLEDGE} and {@code DUPS_OK_ACKNOWLEDGE} it would be acknowledged and lost.
     * {@link #stop()} prevents new deliveries by clearing this listener instead.
     *
     * @param message  the JMS message delivered by the provider
     */
    @Override
    public void onMessage(Message message) {
        this.messageDispatcher.onMessage(message);
    }

    /**
     * Starts consuming messages by registering this receiver as the consumer's native JMS
     * message listener.
     *
     * @throws JMSException if registering the listener fails
     */
    public void consume() throws JMSException {
        this.consumer.setMessageListener(this);
    }

    /**
     * Stops the message receiver.
     * <p>
     * First clears this receiver as the consumer's message listener, so the service is given no new
     * messages. A message the provider has already handed to {@link #onMessage} still completes.
     * Messages that arrive after the listener is cleared stay unacknowledged in the consumer's buffer,
     * and closing the consumer returns them to the broker for redelivery.
     * <p>
     * Then closes the JMS consumer and session on a dedicated thread, bounded by a 30-second timeout,
     * since closing them from the JMS delivery thread itself (e.g. from within {@code onMessage}) can
     * deadlock in some providers. Closing the consumer waits for a message in progress to complete.
     *
     * @throws Exception if an error occurs during shutdown
     */
    public void stop() throws Exception {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        try {
            this.consumer.setMessageListener(null);
        } catch (IllegalStateException e) {
            // The consumer is already closed, for example because its connection was closed, so nothing
            // more can be delivered to the service.
        } catch (JMSException e) {
            // Closing the consumer below still stops delivery, so this is only reported.
            ERR_OUT.println("Failed to clear the message listener while stopping: " + e.getMessage());
        }
        Thread closer = new Thread(() -> {
            try {
                this.consumer.close();
                this.session.close();
            } catch (JMSException e) {
                this.messageDispatcher.onError(e);
            }
        }, "activemq-listener-closer");
        closer.start();
        try {
            closer.join(STOP_TIMEOUT_MS);
            if (closer.isAlive()) {
                throw new JMSException("Timed out while closing the ActiveMQ consumer and session");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
