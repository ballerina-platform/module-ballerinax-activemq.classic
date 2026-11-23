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

import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.transactions.BallerinaTransactionContext;
import io.ballerina.runtime.transactions.TransactionLocalContext;
import io.ballerina.runtime.transactions.TransactionResourceManager;
import jakarta.jms.JMSException;
import jakarta.jms.Session;

import java.util.UUID;

import javax.transaction.xa.XAResource;

import static io.ballerina.lib.activemq.util.ActiveMQConstants.ACTIVEMQ_ERROR;
import static io.ballerina.lib.activemq.util.CommonUtils.createError;

/**
 * Lets a transacted producer take part in a Ballerina {@code transaction} block.
 * <p>
 * The first {@code send} inside a block registers this context with the current transaction. When the block
 * commits or rolls back, the Ballerina runtime calls {@link #commit()} or {@link #rollback()}, which commit or
 * roll back the producer's transacted JMS session. The session isn't an XA resource, so the commit isn't atomic
 * with other resources in the same block.
 */
final class ProducerTransactionContext implements BallerinaTransactionContext {

    private final String connectorId = UUID.randomUUID().toString();
    private final Session session;
    private final Object sessionLock;
    // The transaction this producer currently belongs to, or null. Guarded by this.
    private String activeTransactionId;

    ProducerTransactionContext(Session session, Object sessionLock) {
        this.session = session;
        this.sessionLock = sessionLock;
    }

    /**
     * Registers this context with the caller's current transaction, if it isn't registered yet. Must be called on
     * the calling strand, inside a transaction.
     *
     * @return a {@code BError} if the producer already belongs to a different open transaction, or null
     */
    BError joinCurrentTransaction() {
        TransactionResourceManager manager = TransactionResourceManager.getInstance();
        TransactionLocalContext localContext = manager.getCurrentTransactionContext();
        String globalId = localContext.getGlobalTransactionId();
        String blockId = localContext.getCurrentTransactionBlockId();
        String transactionId = globalId + ":" + blockId;
        synchronized (this) {
            if (activeTransactionId != null && !activeTransactionId.equals(transactionId)) {
                return createError(ACTIVEMQ_ERROR, "The producer is already part of another transaction. "
                        + "Use a separate producer for each transaction that runs at the same time.");
            }
            if (localContext.getTransactionContext(connectorId) == null) {
                localContext.registerTransactionContext(connectorId, this);
                manager.register(globalId, blockId, this);
                activeTransactionId = transactionId;
            }
        }
        return null;
    }

    @Override
    public void commit() {
        synchronized (sessionLock) {
            try {
                session.commit();
            } catch (JMSException e) {
                throw createError(ACTIVEMQ_ERROR, "Failed to commit the transaction: " + e.getMessage(), e);
            }
        }
    }

    @Override
    public void rollback() {
        synchronized (sessionLock) {
            try {
                session.rollback();
            } catch (JMSException e) {
                throw createError(ACTIVEMQ_ERROR, "Failed to roll back the transaction: " + e.getMessage(), e);
            }
        }
    }

    @Override
    public void close() {
        synchronized (this) {
            activeTransactionId = null;
        }
    }

    @Override
    public XAResource getXAResource() {
        return null;
    }
}
