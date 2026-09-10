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

package io.ballerina.lib.activemq.util;

import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import jakarta.jms.Session;

import java.util.function.BooleanSupplier;

import static io.ballerina.lib.activemq.util.ActiveMQConstants.ACTIVEMQ_ERROR;
import static io.ballerina.lib.activemq.util.CommonUtils.createError;

/** Shared close()/execute() guarding for producer/Actions and consumer/Actions native state. */
public final class SessionResourceUtils {

    private SessionResourceUtils() {
    }

    @FunctionalInterface
    public interface Action {
        Object run() throws JMSException;
    }

    /** Best-effort session/connection cleanup after a failed init(), shared by producer and consumer. */
    public static void cleanupOnInitFailure(Connection connection, Session session) {
        if (session != null) {
            CommonUtils.closeQuietly(session::close);
        }
        if (connection != null) {
            CommonUtils.closeQuietly(connection::close);
        }
    }

    /** Idempotently stops/closes connection under stateMonitor's lock, then runs markClosed. */
    public static Object close(Object stateMonitor, Connection connection, BooleanSupplier isClosed,
            Runnable markClosed, String resourceName) {
        synchronized (stateMonitor) {
            if (isClosed.getAsBoolean()) {
                return null;
            }
            try {
                connection.stop();
            } catch (JMSException ignored) {
            }
            try {
                connection.close();
            } catch (JMSException e) {
                return createError(ACTIVEMQ_ERROR, "Failed to close " + resourceName + ": " + e.getMessage(), e);
            }
            markClosed.run();
        }
        return null;
    }

    /** Runs action under sessionLock once neither stateMonitor's nor sessionLock's closed check trips. */
    public static Object execute(Object stateMonitor, Object sessionLock, BooleanSupplier isClosed,
            String resourceName, String operation, Action action) {
        synchronized (stateMonitor) {
            if (isClosed.getAsBoolean()) {
                return createError(ACTIVEMQ_ERROR, "ActiveMQ " + resourceName + " is already closed");
            }
        }
        synchronized (sessionLock) {
            if (isClosed.getAsBoolean()) {
                return createError(ACTIVEMQ_ERROR, "ActiveMQ " + resourceName + " is already closed");
            }
            try {
                return action.run();
            } catch (JMSException e) {
                return createError(ACTIVEMQ_ERROR, String.format("Failed to %s: %s", operation, e.getMessage()), e);
            } catch (ActiveMQDatabindingException e) {
                return createError(ACTIVEMQ_ERROR, e.getMessage(), e);
            }
        }
    }
}
