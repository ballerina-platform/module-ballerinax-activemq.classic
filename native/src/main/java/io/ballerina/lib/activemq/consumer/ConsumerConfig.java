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

package io.ballerina.lib.activemq.consumer;

import io.ballerina.lib.activemq.util.ConnectionConfig;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;

import static io.ballerina.lib.activemq.util.ActiveMQConstants.ACK_MODE;
import static io.ballerina.lib.activemq.util.ActiveMQConstants.DESTINATION_FIELD;
import static io.ballerina.lib.activemq.util.ActiveMQConstants.MESSAGE_SELECTOR;

record ConsumerConfig(ConnectionConfig connectionConfig, String ackMode, BMap<BString, Object> destination,
                      String messageSelector) {
    @SuppressWarnings("unchecked")
    ConsumerConfig(BMap<BString, Object> configurations) {
        this(
                new ConnectionConfig(configurations),
                configurations.getStringValue(ACK_MODE).getValue(),
                (BMap<BString, Object>) configurations.getMapValue(DESTINATION_FIELD),
                configurations.containsKey(MESSAGE_SELECTOR) ?
                        configurations.getStringValue(MESSAGE_SELECTOR).getValue() : null
        );
    }
}
