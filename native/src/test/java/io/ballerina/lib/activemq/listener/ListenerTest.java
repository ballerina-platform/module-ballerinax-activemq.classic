/*
 * Copyright (c) 2026, WSO2 LLC. (http://www.wso2.com)
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.ballerina.lib.activemq.listener;

import jakarta.jms.JMSException;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Session;
import org.testng.annotations.Test;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.expectThrows;

public class ListenerTest {
    @Test
    public void failedPostStartRegistrationClosesResourcesBeforePublishing() {
        List<String> closeOrder = new ArrayList<>();
        MessageConsumer consumer = closeTrackingProxy(MessageConsumer.class, "consumer", closeOrder);
        Session session = closeTrackingProxy(Session.class, "session", closeOrder);
        JMSException registrationFailure = new JMSException("registration failed");
        MessageReceiver receiver = new MessageReceiver(session, consumer, null) {
            @Override
            public void consume() throws JMSException {
                throw registrationFailure;
            }
        };
        AtomicBoolean published = new AtomicBoolean();

        JMSException result = expectThrows(JMSException.class,
                () -> Listener.completeAttachment(true, receiver, consumer, session, () -> published.set(true)));

        assertSame(result, registrationFailure);
        assertFalse(published.get());
        assertEquals(closeOrder, List.of("consumer", "session"));
    }

    private static <T> T closeTrackingProxy(Class<T> type, String name, List<String> closeOrder) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type},
                (proxy, method, args) -> {
                    if ("close".equals(method.getName())) {
                        closeOrder.add(name);
                        return null;
                    }
                    throw new AssertionError("Unexpected JMS operation: " + method.getName());
                }));
    }
}
