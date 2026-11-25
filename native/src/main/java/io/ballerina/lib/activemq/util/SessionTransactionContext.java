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

import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.transactions.TransactionResourceManager;
import jakarta.jms.JMSException;
import jakarta.jms.Session;

import java.util.function.Supplier;

import static io.ballerina.lib.activemq.util.ActiveMQConstants.ACTIVEMQ_ERROR;
import static io.ballerina.lib.activemq.util.CommonUtils.createError;

/**
 * Lets a transacted JMS session join a Ballerina {@code transaction} block. The block settles it.
 *
 * @since 0.1.0
 */
public final class SessionTransactionContext {

    private final Session session;
    private final Object sessionLock;
    private final String resourceName;
    private final boolean receives;
    // The transaction using the session, or null. Guarded by this.
    private String activeTransactionId;
    // Whether the transaction using the session must roll back. Guarded by this.
    private boolean rollbackOnly = false;
    // Calls using the session outside a transaction. Guarded by this.
    private int outsideCalls = 0;

    /** A commit or rollback of the session. */
    @FunctionalInterface
    public interface Settlement {
        void run(SessionTransactionContext transactionContext) throws JMSException;
    }

    /**
     * @param session      the transacted JMS session
     * @param sessionLock  the lock that guards the session
     * @param resourceName the name used in error messages, e.g. {@code "producer"}
     * @param receives     whether the session receives messages
     */
    public SessionTransactionContext(Session session, Object sessionLock, String resourceName, boolean receives) {
        this.session = session;
        this.sessionLock = sessionLock;
        this.resourceName = resourceName;
        this.receives = receives;
    }

    /**
     * Joins the caller's transaction. Must be called on the calling strand.
     *
     * @return an error if the session is in use elsewhere, or null
     */
    public synchronized BError joinCurrentTransaction() {
        String transactionId = TransactionBlock.currentTransactionId();
        if (transactionId.equals(activeTransactionId)) {
            return null;
        }
        if (activeTransactionId != null) {
            return createError(ACTIVEMQ_ERROR, "The " + resourceName + " is already part of another "
                    + "transaction. Use a separate " + resourceName + " for each transaction that runs at the "
                    + "same time.");
        }
        if (outsideCalls > 0) {
            return createError(ACTIVEMQ_ERROR, "The " + resourceName + " is being used outside the transaction at "
                    + "the same time. Use a separate " + resourceName + " for the transaction.");
        }
        TransactionBlock.joinCurrent(this);
        activeTransactionId = transactionId;
        return null;
    }

    /**
     * Runs an explicit commit or rollback. Must be called on the calling strand.
     *
     * @param operation  {@code "commit"} or {@code "roll back"}, used in the error message
     * @param settlement the commit or rollback
     * @return the result, or an error inside a transaction or while one is using the session
     */
    public Object settleExplicitly(String operation, Supplier<Object> settlement) {
        BError notAllowed = beginExplicitSettlement(operation);
        return notAllowed != null ? notAllowed : runOutsideCall(settlement);
    }

    /**
     * Runs a receive outside a transaction.
     *
     * @param receive the receive
     * @return the result, or an error while a transaction is using the session
     */
    public Object receiveOutside(Supplier<Object> receive) {
        BError notAllowed = beginOutsideReceive();
        return notAllowed != null ? notAllowed : runOutsideCall(receive);
    }

    private synchronized BError beginExplicitSettlement(String operation) {
        if (activeTransactionId != null || TransactionResourceManager.getInstance().isInTransaction()) {
            return createError(ACTIVEMQ_ERROR, "Cannot " + operation + " explicitly inside a transaction block or "
                    + "while the " + resourceName + " is part of one. The block's commit or rollback settles it.");
        }
        outsideCalls++;
        return null;
    }

    private synchronized BError beginOutsideReceive() {
        if (activeTransactionId != null) {
            return createError(ACTIVEMQ_ERROR, "Cannot receive outside a transaction while the " + resourceName
                    + " is part of one. Use a separate " + resourceName + ".");
        }
        outsideCalls++;
        return null;
    }

    private Object runOutsideCall(Supplier<Object> call) {
        try {
            return call.get();
        } finally {
            endOutsideCall();
        }
    }

    private synchronized void endOutsideCall() {
        outsideCalls--;
    }

    /**
     * Rolls back the session. If a transaction is using it, that transaction can then only roll back.
     *
     * @throws JMSException if the rollback fails
     */
    public void rollbackUnsettled() throws JMSException {
        synchronized (this) {
            if (activeTransactionId != null) {
                rollbackOnly = true;
                return;
            }
            outsideCalls++;
        }
        try {
            rollbackSession();
        } finally {
            endOutsideCall();
        }
    }

    /**
     * Commits the session.
     *
     * @throws JMSException if the commit fails
     */
    public void commitSession() throws JMSException {
        synchronized (sessionLock) {
            session.commit();
        }
    }

    /**
     * Rolls back the session.
     *
     * @throws JMSException if the rollback fails
     */
    public void rollbackSession() throws JMSException {
        synchronized (sessionLock) {
            session.rollback();
        }
    }

    boolean receives() {
        return receives;
    }

    synchronized boolean isRollbackOnly() {
        return rollbackOnly;
    }

    /** Lets the session join another transaction. */
    synchronized void leaveTransaction() {
        activeTransactionId = null;
        rollbackOnly = false;
    }
}
