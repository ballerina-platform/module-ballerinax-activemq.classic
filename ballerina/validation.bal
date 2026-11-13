// Copyright (c) 2026, WSO2 LLC. (http://www.wso2.com)
//
// WSO2 LLC. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

isolated function validateConnectionConfigurations(ConnectionConfiguration config) returns Error? {
    PrefetchPolicy? prefetchPolicy = config.prefetchPolicy;
    if prefetchPolicy is PrefetchPolicy {
        if prefetchPolicy.queuePrefetchSize < 0 {
            return error Error("queuePrefetchSize cannot be negative");
        }
        if prefetchPolicy.topicPrefetchSize < 0 {
            return error Error("topicPrefetchSize cannot be negative");
        }
        if prefetchPolicy.durableTopicPrefetchSize < 0 {
            return error Error("durableTopicPrefetchSize cannot be negative");
        }
        if prefetchPolicy.optimizeDurableTopicPrefetchSize < 0 {
            return error Error("optimizeDurableTopicPrefetchSize cannot be negative");
        }
    }

    RedeliveryPolicy? redeliveryPolicy = config.redeliveryPolicy;
    if redeliveryPolicy is RedeliveryPolicy {
        if redeliveryPolicy.collisionAvoidancePercent < 0 || redeliveryPolicy.collisionAvoidancePercent > 100 {
            return error Error("collisionAvoidancePercent must be between 0 and 100");
        }
        if redeliveryPolicy.maximumRedeliveries < -1 {
            return error Error("maximumRedeliveries must be -1 (infinite) or greater");
        }
        if redeliveryPolicy.maximumRedeliveryDelay < -1 {
            return error Error("maximumRedeliveryDelay must be -1 (no maximum) or greater");
        }
        if redeliveryPolicy.initialRedeliveryDelay < 0 {
            return error Error("initialRedeliveryDelay cannot be negative");
        }
        if redeliveryPolicy.redeliveryDelay < 0 {
            return error Error("redeliveryDelay cannot be negative");
        }
        if redeliveryPolicy.backOffMultiplier <= 0.0 {
            return error Error("backOffMultiplier must be greater than 0");
        }
    }
}

isolated function validateMessage(Message message) returns Error? {
    int? priority = message.priority;
    if priority is int && (priority < 0 || priority > 9) {
        return error Error("priority must be between 0 and 9");
    }
}
