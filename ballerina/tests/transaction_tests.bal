// Copyright (c) 2026, WSO2 LLC. (http://www.wso2.com)
//
// WSO2 LLC. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import ballerina/lang.runtime;
import ballerina/test;

// Tests for transacted producers taking part in Ballerina `transaction` blocks.

type TextMessage record {|
    *Message;
    string payload;
|};

// Receives every message currently on the queue and returns their payloads in order.
isolated function receiveTexts(string queueName) returns string[]|error {
    MessageConsumer consumer = check new (BROKER_URL, destination = {queueName});
    string[] texts = [];
    error? receiveError = ();
    do {
        TextMessage? msg = check consumer->receive(2000);
        while msg is TextMessage {
            texts.push(msg.payload);
            msg = check consumer->receive(500);
        }
    } on fail error e {
        receiveError = e;
    }
    check consumer->close();
    if receiveError is error {
        return receiveError;
    }
    return texts;
}

// Fails the surrounding block without the compiler treating the rest of the block as unreachable.
isolated function failStep() returns error? {
    return error("simulated failure");
}

// Like `failStep`, but `retry transaction` only retries `error:Retriable` errors.
isolated function failRetriableStep() returns error? {
    return error error:Retriable("simulated transient failure");
}

// Sends one message in its own transaction, holding the transaction open for `holdSeconds`.
function sendInSlowTransaction(MessageProducer producer, string payload, decimal holdSeconds) returns error? {
    transaction {
        check producer->send({payload});
        runtime:sleep(holdSeconds);
        check commit;
    }
}

@test:Config {
    groups: ["transaction", "producer-transaction"]
}
function testTransactionCommitDeliversAllSends() returns error? {
    string queueName = "tx.block.commit.queue";
    check drainQueue(queueName);
    MessageProducer producer = check new (BROKER_URL, transacted = true, destination = {queueName});

    transaction {
        check producer->send({payload: "a"});
        check producer->send({payload: "b"});
        check commit;
    }
    check producer->close();

    test:assertEquals(check receiveTexts(queueName), ["a", "b"],
            "both sends should be delivered when the block commits");
}

@test:Config {
    groups: ["transaction", "producer-transaction"]
}
function testTransactionRollbackDiscardsSends() returns error? {
    string queueName = "tx.block.rollback.queue";
    check drainQueue(queueName);
    MessageProducer producer = check new (BROKER_URL, transacted = true, destination = {queueName});
    boolean shouldRollback = true;

    transaction {
        check producer->send({payload: "discard-me"});
        if shouldRollback {
            rollback;
        } else {
            check commit;
        }
    }
    check producer->close();

    test:assertEquals(check receiveTexts(queueName), [], "a rolled-back send must not be delivered");
}

@test:Config {
    groups: ["transaction", "producer-transaction"]
}
function testTransactionFailedBlockDiscardsSends() returns error? {
    string queueName = "tx.block.fail.queue";
    check drainQueue(queueName);
    MessageProducer producer = check new (BROKER_URL, transacted = true, destination = {queueName});

    error? blockError = ();
    do {
        transaction {
            check producer->send({payload: "discard-me"});
            check failStep();
            check commit;
        }
    } on fail error e {
        blockError = e;
    }
    check producer->close();

    test:assertTrue(blockError is error, "the block should fail");
    test:assertEquals(check receiveTexts(queueName), [], "sends in a failed block must not be delivered");
}

@test:Config {
    groups: ["transaction", "producer-transaction"]
}
function testTransactionFailedSendRollsBackEarlierSends() returns error? {
    string queueName = "tx.block.sendfailure.queue";
    check drainQueue(queueName);
    MessageProducer producer = check new (BROKER_URL, transacted = true, destination = {queueName});

    error? blockError = ();
    do {
        transaction {
            check producer->send({payload: "sent-before-failure"});
            // Priority 15 is invalid, so this send fails and `check` fails the block.
            check producer->send({payload: "invalid", priority: 15});
            check commit;
        }
    } on fail error e {
        blockError = e;
    }
    check producer->close();

    test:assertTrue(blockError is Error, "the invalid send should fail the block");
    test:assertEquals(check receiveTexts(queueName), [],
            "a failed send must roll back the earlier sends in the same block");
}

@test:Config {
    groups: ["transaction", "producer-transaction"]
}
function testNonTransactedProducerInTransactionReturnsError() returns error? {
    string queueName = "tx.block.nontransacted.queue";
    check drainQueue(queueName);
    MessageProducer producer = check new (BROKER_URL, destination = {queueName});

    Error? sendResult = ();
    transaction {
        sendResult = producer->send({payload: "should-not-be-sent"});
        check commit;
    }
    check producer->close();

    if sendResult is () {
        test:assertFail("a producer that isn't transacted must not send inside a transaction block");
    }
    test:assertTrue(sendResult.message().includes("isn't transacted"),
            "unexpected error message: " + sendResult.message());
    test:assertEquals(check receiveTexts(queueName), [], "the rejected send must not be delivered");
}

@test:Config {
    groups: ["transaction", "producer-transaction"]
}
function testTransactedProducerOutsideTransactionReturnsError() returns error? {
    string queueName = "tx.block.outside.queue";
    check drainQueue(queueName);
    MessageProducer producer = check new (BROKER_URL, transacted = true, destination = {queueName});

    Error? sendResult = producer->send({payload: "should-not-be-sent"});
    check producer->close();

    if sendResult is () {
        test:assertFail("a transacted producer must not send outside a transaction block");
    }
    test:assertTrue(sendResult.message().includes("only send inside a transaction block"),
            "unexpected error message: " + sendResult.message());
    test:assertEquals(check receiveTexts(queueName), [], "the rejected send must not be delivered");
}

@test:Config {
    groups: ["transaction", "producer-transaction"]
}
function testNonTransactedProducerStillSendsOutsideTransaction() returns error? {
    string queueName = "tx.block.plain.queue";
    check drainQueue(queueName);
    MessageProducer producer = check new (BROKER_URL, destination = {queueName});

    check producer->send({payload: "plain"});
    check producer->close();

    test:assertEquals(check receiveTexts(queueName), ["plain"],
            "a producer that isn't transacted should keep sending immediately outside a block");
}

@test:Config {
    groups: ["transaction", "producer-transaction"]
}
function testTransactionRetryDeliversOnlyTheCommittedAttempt() returns error? {
    string queueName = "tx.block.retry.queue";
    check drainQueue(queueName);
    MessageProducer producer = check new (BROKER_URL, transacted = true, destination = {queueName});

    int attempts = 0;
    retry transaction {
        attempts += 1;
        check producer->send({payload: "attempt-" + attempts.toString()});
        if attempts < 2 {
            check failRetriableStep();
        }
        check commit;
    }
    check producer->close();

    test:assertEquals(attempts, 2, "the block should succeed on the second attempt");
    test:assertEquals(check receiveTexts(queueName), ["attempt-2"],
            "only the committed attempt's send should be delivered");
}

@test:Config {
    groups: ["transaction", "producer-transaction"]
}
function testSequentialTransactionsReuseProducer() returns error? {
    string queueName = "tx.block.sequential.queue";
    check drainQueue(queueName);
    MessageProducer producer = check new (BROKER_URL, transacted = true, destination = {queueName});
    boolean shouldRollback = true;

    transaction {
        check producer->send({payload: "first-rolled-back"});
        if shouldRollback {
            rollback;
        } else {
            check commit;
        }
    }
    transaction {
        check producer->send({payload: "second-committed"});
        check commit;
    }
    transaction {
        check producer->send({payload: "third-committed"});
        check commit;
    }
    check producer->close();

    test:assertEquals(check receiveTexts(queueName), ["second-committed", "third-committed"],
            "each block should commit or roll back only its own sends");
}

@test:Config {
    groups: ["transaction", "producer-transaction"]
}
function testTwoProducersInOneBlock() returns error? {
    string ordersQueue = "tx.block.two.orders.queue";
    string auditQueue = "tx.block.two.audit.queue";
    check drainQueue(ordersQueue);
    check drainQueue(auditQueue);
    MessageProducer orders = check new (BROKER_URL, transacted = true, destination = {queueName: ordersQueue});
    MessageProducer audit = check new (BROKER_URL, transacted = true, destination = {queueName: auditQueue});

    do {
        transaction {
            check orders->send({payload: "order-rolled-back"});
            check audit->send({payload: "audit-rolled-back"});
            check failStep();
            check commit;
        }
    } on fail {
    }
    transaction {
        check orders->send({payload: "order-committed"});
        check audit->send({payload: "audit-committed"});
        check commit;
    }
    check orders->close();
    check audit->close();

    test:assertEquals(check receiveTexts(ordersQueue), ["order-committed"]);
    test:assertEquals(check receiveTexts(auditQueue), ["audit-committed"]);
}

@test:Config {
    groups: ["transaction", "producer-transaction"]
}
function testProducerSharedByConcurrentTransactionsReturnsError() returns error? {
    string queueName = "tx.block.shared.queue";
    check drainQueue(queueName);
    MessageProducer producer = check new (BROKER_URL, transacted = true, destination = {queueName});

    future<error?> first = start sendInSlowTransaction(producer, "first", 3);
    runtime:sleep(1);
    Error? secondResult = ();
    transaction {
        secondResult = producer->send({payload: "second"});
        check commit;
    }
    check wait first;
    check producer->close();

    if secondResult is () {
        test:assertFail("a producer already in one transaction must not join another one at the same time");
    }
    test:assertTrue(secondResult.message().includes("already part of another transaction"),
            "unexpected error message: " + secondResult.message());
    test:assertEquals(check receiveTexts(queueName), ["first"], "only the first transaction's send is delivered");
}

// ---- JMS transacted-session behaviour ----

// Sends one message from a function that isn't itself a transaction block. It is `transactional`,
// so it can only be called inside a transaction, and its send joins the caller's transaction.
transactional function sendFromHelper(MessageProducer producer, string payload) returns error? {
    check producer->send({payload});
}

@test:Config {
    groups: ["transaction", "producer-transaction"]
}
function testUncommittedSendsAreNotVisibleBeforeCommit() returns error? {
    string queueName = "tx.jms.isolation.queue";
    check drainQueue(queueName);
    MessageProducer producer = check new (BROKER_URL, transacted = true, destination = {queueName});
    MessageConsumer observer = check new (BROKER_URL, destination = {queueName});

    TextMessage? seenBeforeCommit = ();
    transaction {
        check producer->send({payload: "pending"});
        // A JMS transacted send isn't visible to consumers until the session commits.
        seenBeforeCommit = check observer->receive(1000);
        check commit;
    }
    TextMessage? seenAfterCommit = check observer->receive(5000);
    check observer->close();
    check producer->close();

    test:assertTrue(seenBeforeCommit is (), "an uncommitted send must not be visible to consumers");
    if seenAfterCommit is () {
        test:assertFail("the send should be visible once the block commits");
    }
    test:assertEquals(seenAfterCommit.payload, "pending");
}

@test:Config {
    groups: ["transaction", "producer-transaction"]
}
function testCommitPreservesSendOrder() returns error? {
    string queueName = "tx.jms.order.queue";
    check drainQueue(queueName);
    MessageProducer producer = check new (BROKER_URL, transacted = true, destination = {queueName});

    string[] sent = ["m1", "m2", "m3", "m4", "m5"];
    transaction {
        foreach string payload in sent {
            check producer->send({payload});
        }
        check commit;
    }
    check producer->close();

    test:assertEquals(check receiveTexts(queueName), sent, "committed messages should arrive in send order");
}

@test:Config {
    groups: ["transaction", "producer-transaction"]
}
function testQueueAndTopicSendsCommitTogether() returns error? {
    string queueName = "tx.jms.mixed.queue";
    string topicName = "tx.jms.mixed.topic";
    check drainQueue(queueName);
    // A non-durable topic subscriber only receives messages published after it subscribes.
    MessageConsumer topicConsumer = check new (BROKER_URL, destination = {topicName});
    MessageProducer producer = check new (BROKER_URL, transacted = true);

    do {
        transaction {
            check producer->send({payload: "queue-rolled-back"}, {queueName});
            check producer->send({payload: "topic-rolled-back"}, {topicName});
            check failStep();
            check commit;
        }
    } on fail {
    }
    transaction {
        check producer->send({payload: "queue-committed"}, {queueName});
        check producer->send({payload: "topic-committed"}, {topicName});
        check commit;
    }
    check producer->close();

    TextMessage? topicMessage = check topicConsumer->receive(5000);
    TextMessage? extraTopicMessage = check topicConsumer->receive(1000);
    check topicConsumer->close();

    test:assertEquals(check receiveTexts(queueName), ["queue-committed"]);
    if topicMessage is () {
        test:assertFail("the committed topic message should be published");
    }
    test:assertEquals(topicMessage.payload, "topic-committed");
    test:assertTrue(extraTopicMessage is (), "the rolled-back topic message must not be published");
}

@test:Config {
    groups: ["transaction", "producer-transaction"]
}
function testNonPersistentSendsAreTransactional() returns error? {
    string queueName = "tx.jms.nonpersistent.queue";
    check drainQueue(queueName);
    MessageProducer producer = check new (BROKER_URL, transacted = true, destination = {queueName});
    boolean shouldRollback = true;

    transaction {
        check producer->send({payload: "non-persistent-rolled-back", persistent: false});
        if shouldRollback {
            rollback;
        } else {
            check commit;
        }
    }
    transaction {
        check producer->send({payload: "non-persistent-committed", persistent: false});
        check commit;
    }
    check producer->close();

    test:assertEquals(check receiveTexts(queueName), ["non-persistent-committed"],
            "non-persistent sends should commit and roll back like persistent ones");
}

@test:Config {
    groups: ["transaction", "producer-transaction"]
}
function testSendFromTransactionalFunctionJoinsBlock() returns error? {
    string queueName = "tx.jms.helper.queue";
    check drainQueue(queueName);
    MessageProducer producer = check new (BROKER_URL, transacted = true, destination = {queueName});
    boolean shouldRollback = true;

    transaction {
        check sendFromHelper(producer, "helper-rolled-back");
        if shouldRollback {
            rollback;
        } else {
            check commit;
        }
    }
    transaction {
        check sendFromHelper(producer, "helper-committed");
        check commit;
    }
    check producer->close();

    test:assertEquals(check receiveTexts(queueName), ["helper-committed"],
            "a send from a called function should commit and roll back with the caller's block");
}

@test:Config {
    groups: ["transaction", "producer-transaction"]
}
function testSeparateProducersInConcurrentTransactionsCommitIndependently() returns error? {
    string queueName = "tx.jms.concurrent.queue";
    check drainQueue(queueName);
    MessageProducer first = check new (BROKER_URL, transacted = true, destination = {queueName});
    MessageProducer second = check new (BROKER_URL, transacted = true, destination = {queueName});

    future<error?> slow = start sendInSlowTransaction(first, "from-first", 2);
    runtime:sleep(0.5);
    transaction {
        check second->send({payload: "from-second"});
        check commit;
    }
    // The second transaction committed while the first is still open.
    MessageConsumer observer = check new (BROKER_URL, destination = {queueName});
    TextMessage? early = check observer->receive(1000);
    check observer->close();
    check wait slow;
    check first->close();
    check second->close();

    if early is () {
        test:assertFail("the second transaction's message should be visible before the first commits");
    }
    test:assertEquals(early.payload, "from-second");
    test:assertEquals(check receiveTexts(queueName), ["from-first"]);
}
