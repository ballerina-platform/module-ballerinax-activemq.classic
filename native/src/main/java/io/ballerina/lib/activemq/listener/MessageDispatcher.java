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

import io.ballerina.lib.activemq.util.ActiveMQDatabindingException;
import io.ballerina.lib.activemq.util.MessageMapper;
import io.ballerina.runtime.api.Runtime;
import io.ballerina.runtime.api.concurrent.StrandMetadata;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.types.Parameter;
import io.ballerina.runtime.api.types.RecordType;
import io.ballerina.runtime.api.types.RemoteMethodType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.types.TypeTags;
import io.ballerina.runtime.api.utils.TypeUtils;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BObject;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.Session;

import java.io.PrintStream;
import java.util.Optional;

import static io.ballerina.lib.activemq.listener.Caller.NATIVE_SESSION;
import static io.ballerina.lib.activemq.util.ActiveMQConstants.BCALLER_NAME;
import static io.ballerina.lib.activemq.util.ActiveMQConstants.ON_ERROR_METHOD;
import static io.ballerina.lib.activemq.util.ActiveMQConstants.ON_MESSAGE_METHOD;
import static io.ballerina.lib.activemq.util.CommonUtils.createError;
import static io.ballerina.lib.activemq.util.ModuleUtils.getModule;

/** Dispatches JMS messages to the service; onMessage runs on the calling thread (ActiveMQ ties redelivery to it). */
public class MessageDispatcher {
    private static final PrintStream ERR_OUT = System.err;

    private final Runtime ballerinaRuntime;
    private final Service nativeService;
    private final Session session;
    private final OnErrorCallback onErrorCallback = new OnErrorCallback();

    /**
     * Creates a new message dispatcher.
     *
     * @param ballerinaRuntime  the Ballerina runtime for invoking service methods
     * @param nativeService     the wrapped Ballerina service
     * @param session           the JMS session for creating caller objects
     */
    MessageDispatcher(Runtime ballerinaRuntime, Service nativeService, Session session) {
        this.ballerinaRuntime = ballerinaRuntime;
        this.nativeService = nativeService;
        this.session = session;
    }

    /** Associates this dispatcher with its MessageReceiver, so a failing onError handler can stop delivery. */
    public void setReceiver(MessageReceiver receiver) {
        this.onErrorCallback.setReceiver(receiver);
    }

    /** Dispatches a JMS message to onMessage; a returned error goes to onError, a panic is only logged. */
    public void onMessage(Message message) {
        try {
            boolean isConcurrentSafe = nativeService.isOnMessageMethodIsolated();
            StrandMetadata metadata = new StrandMetadata(isConcurrentSafe, null);
            Object[] params = getOnMessageParams(message);
            Object result = ballerinaRuntime.callMethod(
                    nativeService.getConsumerService(), ON_MESSAGE_METHOD, metadata, params);
            if (result instanceof BError bError) {
                onError(bError);
            }
        } catch (BError bError) {
            bError.printStackTrace();
        } catch (JMSException e) {
            onError(e);
            throw new ActiveMQMessageProcessingException(e);
        } catch (ActiveMQDatabindingException e) {
            onError(e);
            throw new ActiveMQMessageProcessingException(e);
        }
    }

    private static final class ActiveMQMessageProcessingException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        ActiveMQMessageProcessingException(Throwable cause) {
            super(cause);
        }
    }

    /** Builds the onMessage argument array, converting Message via the same typed binding receive() uses. */
    private Object[] getOnMessageParams(Message message) throws JMSException {
        Parameter[] parameters = this.nativeService.getOnMessageMethod().getParameters();
        Object[] args = new Object[parameters.length];
        int idx = 0;
        for (Parameter param: parameters) {
            Type referredType = TypeUtils.getReferredType(param.type);
            switch (referredType.getTag()) {
                case TypeTags.OBJECT_TYPE_TAG:
                    args[idx++] = getCaller();
                    break;
                case TypeTags.RECORD_TYPE_TAG:
                    args[idx++] = MessageMapper.toBallerinaMessage(message, (RecordType) referredType);
                    break;
                default:
                    throw new IllegalStateException("Unsupported parameter type: " + referredType);

            }
        }
        return args;
    }

    /**
     * Creates a Ballerina Caller object with the JMS session attached as native data.
     *
     * @return the Ballerina Caller object
     */
    private BObject getCaller() {
        BObject caller = ValueCreator.createObjectValue(getModule(), BCALLER_NAME);
        caller.addNativeData(NATIVE_SESSION, session);
        return caller;
    }

    /**
     * Dispatches an error to the Ballerina service's onError method (if defined). This method
     * spawns a virtual thread to invoke the error handler asynchronously. If no error handler is
     * defined, the error is printed to stderr.
     *
     * @param t  the throwable/error that occurred
     */
    public void onError(Throwable t) {
        Thread.startVirtualThread(() -> {
            try {
                ERR_OUT.println("Unexpected error occurred while message processing: " + t.getMessage());
                Optional<RemoteMethodType> onError = nativeService.getOnError();
                if (onError.isEmpty()) {
                    t.printStackTrace();
                    return;
                }
                String message = t.getMessage() != null ? t.getMessage() : "Failed to fetch the message";
                BError error = createError("Error", message, t);
                boolean isConcurrentSafe = nativeService.isOnErrorMethodIsolated();
                StrandMetadata metadata = new StrandMetadata(isConcurrentSafe, null);
                Object result = ballerinaRuntime.callMethod(
                        nativeService.getConsumerService(), ON_ERROR_METHOD, metadata, error);
                onErrorCallback.notifySuccess(result);
            } catch (BError err) {
                onErrorCallback.notifyFailure(err);
            }
        });
    }
}
