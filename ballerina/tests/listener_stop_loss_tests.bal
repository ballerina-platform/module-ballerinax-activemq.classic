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

import ballerina/io;
import ballerina/lang.runtime;
import ballerina/test;
import ballerina/time;

const string STOP_LOSS_QUEUE = "it.listener.stop.loss.queue";
const string ACTIVEMQ_DLQ = "ActiveMQ.DLQ";
const int STOP_LOSS_MESSAGE_COUNT = 200;
const int STOP_LOSS_PROCESSED_BEFORE_STOP = 3;

isolated map<boolean> stopLossProcessedIds = {};

// Every message sent before a listener stops must end up somewhere: handled by onMessage, still on
// the queue, or in the dead-letter queue. A message in none of these was acknowledged without
// reaching the handler, so it's lost.
@test:Config {
    groups: ["integration", "listener", "listener-stop-loss"]
}
function testListenerStopDoesNotLoseAutoAckMessages() returns error? {
    string runId = time:utcNow()[0].toString();
    string idPrefix = string `stoploss-${runId}-`;

    // Leftovers from earlier runs would skew the counts.
    _ = check drainStopLossQueue(STOP_LOSS_QUEUE, idPrefix);
    lock {
        stopLossProcessedIds = {};
    }

    Listener stopLossListener = check new (brokerUrl, username = username, password = password);
    Service stopLossSvc = @ServiceConfig {
        queueName: STOP_LOSS_QUEUE
    } service object {
        remote function onMessage(Message message) returns error? {
            string id = check string:fromBytes(check message.payload.ensureType());
            // A slow handler keeps most messages waiting in the consumer's prefetch buffer when the
            // listener stops.
            runtime:sleep(0.1);
            lock {
                stopLossProcessedIds[id] = true;
            }
        }
    };
    check stopLossListener.attach(stopLossSvc, "it-listener-stop-loss-svc");
    check stopLossListener.'start();

    MessageProducer prod = check new (brokerUrl, username = username, password = password);
    foreach int i in 1 ... STOP_LOSS_MESSAGE_COUNT {
        check prod->send({payload: string `${idPrefix}${i}`.toBytes(), properties: {"stopLossRun": runId}},
            {queueName: STOP_LOSS_QUEUE});
    }
    check prod->close();

    // Stop the listener while it's still working through the batch.
    int waitedMs = 0;
    while processedCount() < STOP_LOSS_PROCESSED_BEFORE_STOP && waitedMs < 30000 {
        runtime:sleep(0.05);
        waitedMs += 50;
    }
    int processedAtStop = processedCount();
    check stopLossListener.gracefulStop();
    runtime:sleep(TEST_WAIT_TIME);

    map<boolean> processed = {};
    lock {
        processed = stopLossProcessedIds.clone();
    }
    map<boolean> remaining = check drainStopLossQueue(STOP_LOSS_QUEUE, idPrefix);
    // ActiveMQ.DLQ is shared, so only this run's messages are received from it.
    map<boolean> deadLettered = check drainStopLossQueue(ACTIVEMQ_DLQ, idPrefix, string `stopLossRun = '${runId}'`);

    string[] lost = [];
    foreach int i in 1 ... STOP_LOSS_MESSAGE_COUNT {
        string id = string `${idPrefix}${i}`;
        if !processed.hasKey(id) && !remaining.hasKey(id) && !deadLettered.hasKey(id) {
            lost.push(id.substring(idPrefix.length()));
        }
    }

    io:println(string `[stop-loss] sent=${STOP_LOSS_MESSAGE_COUNT} processedAtStop=${processedAtStop} ` +
            string `processed=${processed.length()} remainingOnQueue=${remaining.length()} ` +
            string `deadLettered=${deadLettered.length()} lost=${lost.length()}`);
    if lost.length() > 0 {
        io:println(string `[stop-loss] lost message numbers: ${string:'join(",", ...lost)}`);
    }

    test:assertEquals(lost.length(), 0,
        string `${lost.length()} of ${STOP_LOSS_MESSAGE_COUNT} messages were acknowledged ` +
        "while the listener was stopping and never reached onMessage");
}

isolated function processedCount() returns int {
    lock {
        return stopLossProcessedIds.length();
    }
}

// Receives every message on the queue that matches the selector, if one is given, and returns the
// ids that belong to this run. Other matching messages are consumed but ignored.
function drainStopLossQueue(string queueName, string idPrefix, string? messageSelector = ())
        returns map<boolean>|error {
    MessageConsumer consumer = messageSelector is string
        ? check new (brokerUrl, username = username, password = password, destination = {queueName},
            messageSelector = messageSelector)
        : check new (brokerUrl, username = username, password = password, destination = {queueName});
    map<boolean> ids = {};
    while true {
        Message? message = check consumer->receive(2000);
        if message is () {
            break;
        }
        anydata payload = message.payload;
        if payload is byte[] {
            string|error id = string:fromBytes(payload);
            if id is string && id.startsWith(idPrefix) {
                ids[id] = true;
            }
        }
    }
    check consumer->close();
    return ids;
}
