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
import io.ballerina.runtime.transactions.BallerinaTransactionContext;
import io.ballerina.runtime.transactions.TransactionLocalContext;
import io.ballerina.runtime.transactions.TransactionResourceManager;
import jakarta.jms.JMSException;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import javax.transaction.xa.XAResource;

import static io.ballerina.lib.activemq.util.ActiveMQConstants.ACTIVEMQ_ERROR;
import static io.ballerina.lib.activemq.util.CommonUtils.createError;

/**
 * The JMS sessions that joined a Ballerina {@code transaction} block. Sessions that only send commit first, so a
 * failed send rolls back the received messages. One session's failure doesn't affect the others.
 */
final class TransactionBlock implements BallerinaTransactionContext {

    private static final String PARTICIPANT_ID = "ballerinax/activemq.classic";
    private static final PrintStream ERR_OUT = System.err;
    private static final Object REGISTRATION_LOCK = new Object();

    private final String transactionId;
    // Guarded by this.
    private final List<SessionTransactionContext> senders = new ArrayList<>();
    private final List<SessionTransactionContext> receivers = new ArrayList<>();
    private boolean ended = false;

    private TransactionBlock(String transactionId) {
        this.transactionId = transactionId;
    }

    static String currentTransactionId() {
        TransactionLocalContext localContext =
                TransactionResourceManager.getInstance().getCurrentTransactionContext();
        return localContext.getGlobalTransactionId() + ":" + localContext.getCurrentTransactionBlockId();
    }

    /** Adds a session to the caller's transaction. Must be called on the calling strand. */
    static void joinCurrent(SessionTransactionContext sessionContext) {
        TransactionResourceManager manager = TransactionResourceManager.getInstance();
        TransactionLocalContext localContext = manager.getCurrentTransactionContext();
        String transactionId = currentTransactionId();
        // Strands of one transaction can join at the same time.
        synchronized (REGISTRATION_LOCK) {
            if (localContext.getTransactionContext(PARTICIPANT_ID) instanceof TransactionBlock current
                    && current.add(sessionContext, transactionId)) {
                return;
            }
            TransactionBlock block = new TransactionBlock(transactionId);
            block.add(sessionContext, transactionId);
            localContext.registerTransactionContext(PARTICIPANT_ID, block);
            manager.register(localContext.getGlobalTransactionId(), localContext.getCurrentTransactionBlockId(),
                    block);
        }
    }

    private synchronized boolean add(SessionTransactionContext sessionContext, String currentTransactionId) {
        if (ended || !transactionId.equals(currentTransactionId)) {
            return false;
        }
        (sessionContext.receives() ? receivers : senders).add(sessionContext);
        return true;
    }

    /** Ends the block and returns its sessions, senders first. Empty if already ended. */
    private synchronized List<SessionTransactionContext> end() {
        List<SessionTransactionContext> sessions = new ArrayList<>(senders);
        sessions.addAll(receivers);
        senders.clear();
        receivers.clear();
        ended = true;
        return sessions;
    }

    /** Commits the sessions, senders first. If one fails, the rest are rolled back. */
    @Override
    public void commit() {
        BError failure = null;
        for (SessionTransactionContext sessionContext : end()) {
            try {
                if (failure == null) {
                    sessionContext.commitSession();
                } else {
                    rollbackSession(sessionContext);
                }
            } catch (JMSException | RuntimeException e) {
                failure = createError(ACTIVEMQ_ERROR, "Failed to commit the transaction: " + e.getMessage(), e);
            } finally {
                sessionContext.leaveTransaction();
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    /** Rolls back every session. A failure is only logged, so that the others still roll back. */
    @Override
    public void rollback() {
        for (SessionTransactionContext sessionContext : end()) {
            try {
                rollbackSession(sessionContext);
            } finally {
                sessionContext.leaveTransaction();
            }
        }
    }

    private static void rollbackSession(SessionTransactionContext sessionContext) {
        try {
            sessionContext.rollbackSession();
        } catch (JMSException | RuntimeException e) {
            ERR_OUT.println("Failed to roll back the transaction: " + e.getMessage());
        }
    }

    @Override
    public void close() {
        // commit() and rollback() release the sessions.
    }

    @Override
    public XAResource getXAResource() {
        return null;
    }
}
