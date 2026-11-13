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

import ballerina/test;

@test:Config {
    groups: ["validations"]
}
isolated function testSendRejectsInvalidPriority() returns error? {
    MessageProducer producer = check new (BROKER_URL);
    Error? result = producer->send({
        payload: "invalid priority".toBytes(),
        priority: 15
    }, {queueName: "validation.priority.queue"});
    check producer->close();
    test:assertTrue(result is Error, "priority outside 0-9 should be rejected");
    if result is Error {
        test:assertEquals(result.message(), "priority must be between 0 and 9");
    }
}

@test:Config {
    groups: ["validations"]
}
isolated function testInitRejectsInvalidCollisionAvoidancePercent() returns error? {
    MessageProducer|Error result = new (BROKER_URL, redeliveryPolicy = {collisionAvoidancePercent: 150});
    test:assertTrue(result is Error, "collisionAvoidancePercent outside 0-100 should be rejected");
    if result is Error {
        test:assertEquals(result.message(), "collisionAvoidancePercent must be between 0 and 100");
    }
}

@test:Config {
    groups: ["validations"]
}
isolated function testInitRejectsNegativePrefetchSize() returns error? {
    MessageProducer|Error result = new (BROKER_URL, prefetchPolicy = {queuePrefetchSize: -5});
    test:assertTrue(result is Error, "a negative prefetch size should be rejected");
    if result is Error {
        test:assertEquals(result.message(), "queuePrefetchSize cannot be negative");
    }
}

@test:Config {
    groups: ["validations"]
}
isolated function testInitRejectsInvalidBackOffMultiplier() returns error? {
    MessageProducer|Error result = new (BROKER_URL, redeliveryPolicy = {backOffMultiplier: 0.0});
    test:assertTrue(result is Error, "a zero backOffMultiplier should be rejected");
    if result is Error {
        test:assertEquals(result.message(), "backOffMultiplier must be greater than 0");
    }
}
