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

import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQPrefetchPolicy;
import org.apache.activemq.ActiveMQSslConnectionFactory;
import org.apache.activemq.RedeliveryPolicy;

import java.security.SecureRandom;
import java.util.Objects;
import java.util.Properties;

import javax.net.ssl.KeyManager;
import javax.net.ssl.TrustManager;

import static io.ballerina.lib.activemq.util.ActiveMQConstants.CERT;
import static io.ballerina.lib.activemq.util.ActiveMQConstants.KEY;
import static io.ballerina.lib.activemq.util.SslUtils.getKeyManagers;
import static io.ballerina.lib.activemq.util.SslUtils.getTrustmanagers;

/**
 * Builds an {@link ActiveMQConnectionFactory} from a {@link ConnectionConfig}. Shared by the
 * producer and consumer native implementations, which each open their own JMS connection.
 *
 * @since 0.1.0
 */
public final class ConnectionFactoryUtils {

    private ConnectionFactoryUtils() {
    }

    @SuppressWarnings("unchecked")
    public static ActiveMQConnectionFactory createConnectionFactory(String url, ConnectionConfig config)
            throws Exception {
        ActiveMQConnectionFactory factory;
        if (Objects.nonNull(config.secureSocket())) {
            ActiveMQSslConnectionFactory sslFactory = new ActiveMQSslConnectionFactory(url);
            BMap<BString, Object> secureSocket = config.secureSocket();
            Object bCert = secureSocket.get(CERT);
            BMap<BString, BString> keyRecord = (BMap<BString, BString>) secureSocket.getMapValue(KEY);
            KeyManager[] keyManagers = getKeyManagers(keyRecord);
            TrustManager[] trustManagers = getTrustmanagers(bCert);
            sslFactory.setKeyAndTrustManagers(keyManagers, trustManagers, new SecureRandom());
            factory = sslFactory;
        } else {
            factory = new ActiveMQConnectionFactory(url);
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

        factory.setClientID(config.clientId());
        factory.setOptimizeAcknowledge(config.optimizeAcknowledgements());
        factory.setAlwaysSessionAsync(config.setAlwaysSessionAsync());

        if (config.prefetchPolicyConfig() != null) {
            factory.setPrefetchPolicy(buildPrefetchPolicy(config.prefetchPolicyConfig()));
        }
        if (config.redeliveryPolicyConfig() != null) {
            factory.setRedeliveryPolicy(buildRedeliveryPolicy(config.redeliveryPolicyConfig()));
        }
        factory.setProperties(buildConnectionProperties(config.properties()));
        return factory;
    }

    private static ActiveMQPrefetchPolicy buildPrefetchPolicy(PrefetchPolicyConfig config) {
        ActiveMQPrefetchPolicy policy = new ActiveMQPrefetchPolicy();
        policy.setQueuePrefetch(config.queuePrefetchSize());
        policy.setTopicPrefetch(config.topicPrefetchSize());
        policy.setDurableTopicPrefetch(config.durableTopicPrefetchSize());
        policy.setOptimizeDurableTopicPrefetch(config.optimizeDurableTopicPrefetchSize());
        return policy;
    }

    /** Builds an ActiveMQ redelivery policy; shared by the connection factory and per-consumer setup. */
    public static RedeliveryPolicy buildRedeliveryPolicy(RedeliveryPolicyConfig config) {
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

    private static Properties buildConnectionProperties(BMap<BString, BString> additionalProperties) {
        Properties properties = new Properties();
        for (BString key : additionalProperties.getKeys()) {
            properties.put(key.getValue(), additionalProperties.getStringValue(key).getValue());
        }
        return properties;
    }
}
