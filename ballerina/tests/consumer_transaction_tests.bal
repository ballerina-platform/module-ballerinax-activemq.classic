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

import ballerina/test;

// Tests for pull consumers whose acknowledgements take part in Ballerina `transaction` blocks.

// Creates a transacted pull consumer that redelivers rolled-back messages quickly.
isolated function newTransactedConsumer(string queueName) returns MessageConsumer|error {
    return new (BROKER_URL, ackMode = SESSION_TRANSACTED, destination = {queueName},
        redeliveryPolicy = {maximumRedeliveries: 5, initialRedeliveryDelay: 200, redeliveryDelay: 200});
}

@test:Config {
    groups: ["transaction", "consumer-transaction"]
}
function testConsumerAcknowledgeInBlockCommits() returns error? {
    string queueName = "tx.consumer.commit.queue";
    check drainQueue(queueName);
    check sendToQueue(BROKER_URL, queueName, "commit-me");
    MessageConsumer consumer = check newTransactedConsumer(queueName);

    TextMessage? received = ();
    transaction {
        received = check consumer->receive(5000);
        if received is TextMessage {
            check consumer->acknowledge(received);
        }
        check commit;
    }
    check consumer->close();

    if received is () {
        test:assertFail("the message should be received");
    }
    test:assertEquals(received.payload, "commit-me");
    test:assertEquals(check receiveTexts(queueName), [], "the block's commit should acknowledge the message");
}

@test:Config {
    groups: ["transaction", "consumer-transaction"]
}
function testConsumerRollbackInBlockRedelivers() returns error? {
    string queueName = "tx.consumer.rollback.queue";
    check drainQueue(queueName);
    check sendToQueue(BROKER_URL, queueName, "roll-me-back");
    MessageConsumer consumer = check newTransactedConsumer(queueName);
    boolean shouldRollback = true;

    transaction {
        TextMessage? first = check consumer->receive(5000);
        if first is TextMessage {
            check consumer->acknowledge(first);
        }
        if shouldRollback {
            rollback;
        } else {
            check commit;
        }
    }
    // The block has ended, so the explicit commit is allowed again.
    TextMessage? redelivered = check consumer->receive(5000);
    check consumer->'commit();
    check consumer->close();

    if redelivered is () {
        test:assertFail("the rolled-back message should be redelivered");
    }
    test:assertEquals(redelivered.payload, "roll-me-back");
    test:assertEquals(redelivered.redelivered, true, "the redelivered message should be flagged as redelivered");
    test:assertEquals(check receiveTexts(queueName), [], "the explicit commit should acknowledge it");
}

@test:Config {
    groups: ["transaction", "consumer-transaction"]
}
function testConsumerFailedBlockRedelivers() returns error? {
    string queueName = "tx.consumer.failedblock.queue";
    check drainQueue(queueName);
    check sendToQueue(BROKER_URL, queueName, "fail-once");
    MessageConsumer consumer = check newTransactedConsumer(queueName);

    boolean received = false;
    do {
        transaction {
            TextMessage? first = check consumer->receive(5000);
            if first is TextMessage {
                received = true;
                check consumer->acknowledge(first);
            }
            check failStep();
            check commit;
        }
    } on fail {
    }
    TextMessage? redelivered = ();
    transaction {
        redelivered = check consumer->receive(5000);
        if redelivered is TextMessage {
            check consumer->acknowledge(redelivered);
        }
        check commit;
    }
    check consumer->close();

    test:assertTrue(received, "the message should be received before the block fails");
    if redelivered is () {
        test:assertFail("a failed block should have the message redelivered");
    }
    test:assertEquals(redelivered.payload, "fail-once");
    test:assertEquals(redelivered.redelivered, true, "the message should be flagged as redelivered");
    test:assertEquals(check receiveTexts(queueName), [], "the second block's commit should acknowledge it");
}

@test:Config {
    groups: ["transaction", "consumer-transaction"]
}
function testConsumerRollbackRedeliversMessagesReceivedBeforeBlock() returns error? {
    string queueName = "tx.consumer.earlier.queue";
    check drainQueue(queueName);
    check sendToQueue(BROKER_URL, queueName, "first");
    check sendToQueue(BROKER_URL, queueName, "second");
    MessageConsumer consumer = check newTransactedConsumer(queueName);
    boolean shouldRollback = true;

    // Received outside the block, so it stays in the session's open JMS transaction.
    TextMessage? first = check consumer->receive(5000);
    transaction {
        TextMessage? second = check consumer->receive(5000);
        if second is TextMessage {
            check consumer->acknowledge(second);
        }
        if shouldRollback {
            rollback;
        } else {
            check commit;
        }
    }
    string[] redelivered = [];
    transaction {
        foreach int i in 0 ..< 2 {
            TextMessage? msg = check consumer->receive(5000);
            if msg is TextMessage {
                redelivered.push(msg.payload);
                check consumer->acknowledge(msg);
            }
        }
        check commit;
    }
    check consumer->close();

    test:assertTrue(first is TextMessage, "the first message should be received before the block");
    test:assertEquals(redelivered, ["first", "second"],
            "rolling back the block should also redeliver the message received before it");
    test:assertEquals(check receiveTexts(queueName), [], "the second block's commit should acknowledge both");
}

@test:Config {
    groups: ["transaction", "consumer-transaction"]
}
function testConsumerAcknowledgeAndProducerSendCommitTogether() returns error? {
    string inQueue = "tx.consumer.withproducer.in.queue";
    string outQueue = "tx.consumer.withproducer.out.queue";
    check drainQueue(inQueue);
    check drainQueue(outQueue);
    check sendToQueue(BROKER_URL, inQueue, "process-me");
    MessageConsumer consumer = check newTransactedConsumer(inQueue);
    MessageProducer producer = check new (BROKER_URL, transacted = true, destination = {queueName: outQueue});

    boolean?[] redelivered = [];
    foreach int attempt in 1 ... 2 {
        transaction {
            TextMessage? msg = check consumer->receive(5000);
            if msg is TextMessage {
                redelivered.push(msg.redelivered);
                check producer->send({payload: "processed-" + attempt.toString()});
                check consumer->acknowledge(msg);
            }
            if attempt == 1 {
                rollback;
            } else {
                check commit;
            }
        }
    }
    check consumer->close();
    check producer->close();

    test:assertEquals(redelivered, [false, true], "the second attempt should receive the redelivered message");
    test:assertEquals(check receiveTexts(outQueue), ["processed-2"],
            "only the committed attempt's send should be delivered");
    test:assertEquals(check receiveTexts(inQueue), [], "the committed attempt should acknowledge the input");
}

@test:Config {
    groups: ["transaction", "consumer-transaction"]
}
function testConsumerNonTransactedAcknowledgeInBlockReturnsError() returns error? {
    string queueName = "tx.consumer.nontransacted.queue";
    check drainQueue(queueName);
    check sendToQueue(BROKER_URL, queueName, "client-ack");
    MessageConsumer consumer = check new (BROKER_URL, ackMode = CLIENT_ACKNOWLEDGE, destination = {queueName});

    TextMessage? received = check consumer->receive(5000);
    if received is () {
        test:assertFail("the message should be received");
    }
    Error? inBlock = ();
    transaction {
        inBlock = consumer->acknowledge(received);
        check commit;
    }
    // Outside the block, acknowledging works as before.
    check consumer->acknowledge(received);
    check consumer->close();

    if inBlock is () {
        test:assertFail("a consumer that isn't transacted must not acknowledge inside a transaction block");
    }
    test:assertTrue(inBlock.message().includes("isn't transacted"), "unexpected error message: " + inBlock.message());
    test:assertEquals(check receiveTexts(queueName), [], "acknowledging outside the block should settle it");
}

@test:Config {
    groups: ["transaction", "consumer-transaction"]
}
function testConsumerExplicitCommitInsideBlockReturnsError() returns error? {
    string queueName = "tx.consumer.explicit.queue";
    check drainQueue(queueName);
    check sendToQueue(BROKER_URL, queueName, "explicit");
    MessageConsumer consumer = check newTransactedConsumer(queueName);

    Error? explicitCommit = ();
    Error? explicitRollback = ();
    transaction {
        TextMessage? msg = check consumer->receive(5000);
        if msg is TextMessage {
            check consumer->acknowledge(msg);
        }
        explicitCommit = consumer->'commit();
        explicitRollback = consumer->'rollback();
        check commit;
    }
    check consumer->close();

    if explicitCommit is () || explicitRollback is () {
        test:assertFail("explicit commit and rollback must be refused while the consumer is part of a block");
    }
    test:assertTrue(explicitCommit.message().includes("Cannot commit explicitly"),
            "unexpected error message: " + explicitCommit.message());
    test:assertTrue(explicitRollback.message().includes("Cannot roll back explicitly"),
            "unexpected error message: " + explicitRollback.message());
    test:assertEquals(check receiveTexts(queueName), [], "the block's commit should still acknowledge it");
}

// Receives and acknowledges one message in its own transaction. Sets `<name>.joined`, then holds the
// transaction open until `<name>.release` is set.
function acknowledgeInHeldTransaction(MessageConsumer consumer, string name) returns error? {
    transaction {
        TextMessage? msg = check consumer->receive(5000);
        if msg is TextMessage {
            check consumer->acknowledge(msg);
        }
        setFlag(name + ".joined");
        check waitForFlag(name + ".release");
        check commit;
    }
}

@test:Config {
    groups: ["transaction", "consumer-transaction"]
}
function testConsumerSharedByConcurrentTransactionsReturnsError() returns error? {
    string queueName = "tx.consumer.shared.queue";
    check drainQueue(queueName);
    check sendToQueue(BROKER_URL, queueName, "first");
    MessageConsumer consumer = check newTransactedConsumer(queueName);

    future<error?> held = start acknowledgeInHeldTransaction(consumer, "consumer.shared");
    check waitForFlag("consumer.shared.joined");
    Error? secondResult = ();
    transaction {
        // Receiving joins the consumer's session, so the second transaction is refused at the receive.
        TextMessage|Error? msg = consumer->receive(1000);
        if msg is Error {
            secondResult = msg;
        }
        check commit;
    }
    setFlag("consumer.shared.release");
    check wait held;

    // Once the first transaction has ended, the same consumer must join a later one normally.
    check sendToQueue(BROKER_URL, queueName, "third");
    TextMessage? later = ();
    transaction {
        later = check consumer->receive(5000);
        if later is TextMessage {
            check consumer->acknowledge(later);
        }
        check commit;
    }
    check consumer->close();

    if secondResult is () {
        test:assertFail("a consumer already in one transaction must not join another one at the same time");
    }
    test:assertTrue(secondResult.message().includes("already part of another transaction"),
            "unexpected error message: " + secondResult.message());
    if later is () {
        test:assertFail("the consumer should receive in a later transaction");
    }
    test:assertEquals(later.payload, "third");
    test:assertEquals(check receiveTexts(queueName), [], "the later transaction should acknowledge its message");
}

@test:Config {
    groups: ["transaction", "consumer-transaction"]
}
function testConsumerFailureBeforeAcknowledgeRedelivers() returns error? {
    string queueName = "tx.consumer.failbeforeack.queue";
    check drainQueue(queueName);
    check sendToQueue(BROKER_URL, queueName, "A");
    MessageConsumer consumer = check newTransactedConsumer(queueName);

    do {
        transaction {
            TextMessage? first = check consumer->receive(5000);
            // Processing fails before the message is acknowledged. The receive has already joined the
            // consumer's session to the block, so the block's rollback still covers the message.
            check failStep();
            if first is TextMessage {
                check consumer->acknowledge(first);
            }
            check commit;
        }
    } on fail {
    }
    TextMessage? redelivered = ();
    transaction {
        redelivered = check consumer->receive(5000);
        if redelivered is TextMessage {
            check consumer->acknowledge(redelivered);
        }
        check commit;
    }
    check consumer->close();

    if redelivered is () {
        test:assertFail("a message whose block failed before acknowledge should be redelivered");
    }
    test:assertEquals(redelivered.payload, "A");
    test:assertEquals(redelivered.redelivered, true, "the message should be flagged as redelivered");
    test:assertEquals(check receiveTexts(queueName), [], "the second block's commit should acknowledge it");
}

@test:Config {
    groups: ["transaction", "consumer-transaction"]
}
function testConsumerExplicitCommitBeforeJoiningReturnsError() returns error? {
    string queueName = "tx.consumer.explicitbeforejoin.queue";
    check drainQueue(queueName);
    check sendToQueue(BROKER_URL, queueName, "before-join");
    MessageConsumer consumer = check newTransactedConsumer(queueName);

    // Received outside any block, so the consumer's session is open but hasn't joined a block.
    TextMessage? received = check consumer->receive(5000);
    Error? explicitCommit = ();
    Error? explicitRollback = ();
    transaction {
        explicitCommit = consumer->'commit();
        explicitRollback = consumer->'rollback();
        check commit;
    }
    // Outside the block, the explicit commit is allowed and acknowledges the message.
    check consumer->'commit();
    check consumer->close();

    test:assertTrue(received is TextMessage, "the message should be received");
    if explicitCommit is () || explicitRollback is () {
        test:assertFail("explicit commit and rollback must be refused inside a transaction block");
    }
    test:assertTrue(explicitCommit.message().includes("Cannot commit explicitly inside a transaction block"),
            "unexpected error message: " + explicitCommit.message());
    test:assertTrue(explicitRollback.message().includes("Cannot roll back explicitly inside a transaction block"),
            "unexpected error message: " + explicitRollback.message());
    test:assertEquals(check receiveTexts(queueName), [], "the explicit commit outside the block should settle it");
}

// Receives and forwards one message, then closes the producer so that its session can't commit.
function forwardThenCloseProducer(MessageConsumer consumer, MessageProducer producer) returns error? {
    transaction {
        TextMessage? msg = check consumer->receive(5000);
        if msg is TextMessage {
            check producer->send({payload: msg.payload});
        }
        check producer->close();
        check commit;
    }
}

@test:Config {
    groups: ["transaction", "consumer-transaction"]
}
function testConsumerMessageRedeliveredWhenProducerCommitFails() returns error? {
    string inQueue = "tx.consumer.producerfailure.in.queue";
    string outQueue = "tx.consumer.producerfailure.out.queue";
    check drainQueue(inQueue);
    check drainQueue(outQueue);
    check sendToQueue(BROKER_URL, inQueue, "keep-me");
    MessageConsumer consumer = check newTransactedConsumer(inQueue);
    MessageProducer producer = check new (BROKER_URL, transacted = true, destination = {queueName: outQueue});

    // The consumer joins the block first, but must not be committed before the producer.
    future<error?> block = start forwardThenCloseProducer(consumer, producer);
    error? blockError = trap wait block;
    TextMessage? redelivered = ();
    transaction {
        redelivered = check consumer->receive(5000);
        check commit;
    }
    check consumer->close();

    if blockError is () {
        test:assertFail("the block should fail when the producer can't commit");
    }
    // Newer runtimes report a failed commit as "hazard".
    test:assertTrue(blockError.message().includes("Failed to commit the transaction")
            || blockError.message() == "hazard", "unexpected error message: " + blockError.message());
    if redelivered is () {
        test:assertFail("the message must not be lost when its output can't be committed");
    }
    test:assertEquals(redelivered.payload, "keep-me");
    test:assertEquals(check receiveTexts(outQueue), [], "the output must not be delivered");
}

@test:Config {
    groups: ["transaction", "consumer-transaction"]
}
function testConsumerAcknowledgeOfAnotherConsumersMessageReturnsError() returns error? {
    string queueName = "tx.consumer.othermessage.queue";
    check drainQueue(queueName);
    check sendToQueue(BROKER_URL, queueName, "not-yours");
    MessageConsumer receiver = check newTransactedConsumer(queueName);
    MessageConsumer other = check newTransactedConsumer("tx.consumer.othermessage.other.queue");

    TextMessage? received = check receiver->receive(5000);
    if received is () {
        test:assertFail("the message should be received");
    }
    Error? result = ();
    transaction {
        result = other->acknowledge(received);
        check commit;
    }
    check receiver->close();
    check other->close();

    if result is () {
        test:assertFail("a message must be acknowledged by the consumer that received it");
    }
    test:assertTrue(result.message().includes("received by another consumer"),
            "unexpected error message: " + result.message());
    test:assertEquals(check receiveTexts(queueName), ["not-yours"], "the message should stay unacknowledged");
}

@test:Config {
    groups: ["transaction", "consumer-transaction"]
}
function testConsumerReceiveOutsideBlockWhileInBlockReturnsError() returns error? {
    string queueName = "tx.consumer.outsidereceive.queue";
    check drainQueue(queueName);
    check sendToQueue(BROKER_URL, queueName, "first");
    check sendToQueue(BROKER_URL, queueName, "second");
    MessageConsumer consumer = check newTransactedConsumer(queueName);

    future<error?> held = start acknowledgeInHeldTransaction(consumer, "consumer.outside");
    check waitForFlag("consumer.outside.joined");
    TextMessage?|Error outside = consumer->receive(1000);
    setFlag("consumer.outside.release");
    check wait held;
    check consumer->close();

    if outside !is Error {
        test:assertFail("another strand's block must not cover a message received outside it");
    }
    test:assertTrue(outside.message().includes("Cannot receive outside a transaction"),
            "unexpected error message: " + outside.message());
    test:assertEquals(check receiveTexts(queueName), ["second"], "the block should acknowledge only its message");
}

// Receives outside a transaction. Retries if the receive is refused because a block is using the consumer.
isolated function receiveOutsideBlock(MessageConsumer consumer, int timeoutMs) {
    _ = waitUntil(isolated function () returns boolean {
        TextMessage?|Error received = consumer->receive(timeoutMs);
        return received !is Error;
    });
}

isolated function isJoinRefused(MessageConsumer consumer) returns boolean {
    string message = "";
    do {
        transaction {
            TextMessage?|Error inBlock = consumer->receiveNoWait();
            if inBlock is Error {
                message = inBlock.message();
            }
            check commit;
        }
    } on fail {
        return false;
    }
    return message.includes("being used outside the transaction");
}

@test:Config {
    groups: ["transaction", "consumer-transaction"]
}
function testConsumerJoinWhileReceivingOutsideReturnsError() returns error? {
    string queueName = "tx.consumer.joinwhilereceiving.queue";
    check drainQueue(queueName);
    final MessageConsumer consumer = check newTransactedConsumer(queueName);

    future<()> outside = start receiveOutsideBlock(consumer, 3000);
    boolean joinRefused = waitUntil(isolated function () returns boolean {
        return isJoinRefused(consumer);
    });
    check wait outside;
    // Once the outside receive has ended, the consumer can join a block.
    check sendToQueue(BROKER_URL, queueName, "later");
    TextMessage? later = ();
    transaction {
        later = check consumer->receive(5000);
        check commit;
    }
    check consumer->close();

    test:assertTrue(joinRefused, "a consumer must not join a block while another strand is receiving with it");
    test:assertTrue(later is TextMessage, "the consumer should join a later block");
}

@test:Config {
    groups: ["transaction", "consumer-transaction"]
}
function testConsumerReceiveNoWaitJoinsBlock() returns error? {
    string queueName = "tx.consumer.nowait.queue";
    check drainQueue(queueName);
    check sendToQueue(BROKER_URL, queueName, "no-wait");
    final MessageConsumer consumer = check newTransactedConsumer(queueName);

    boolean received = false;
    do {
        transaction {
            // The broker may not have pushed the message to the consumer yet.
            received = waitUntil(isolated function () returns boolean {
                TextMessage?|Error first = consumer->receiveNoWait();
                return first is TextMessage;
            });
            check failStep();
            check commit;
        }
    } on fail {
    }
    TextMessage? redelivered = check consumer->receive(5000);
    check consumer->'commit();
    check consumer->close();

    test:assertTrue(received, "the message should be received");
    if redelivered is () {
        test:assertFail("a message received with receiveNoWait() should be rolled back with the block");
    }
    test:assertEquals(redelivered.redelivered, true, "the message should be flagged as redelivered");
}
