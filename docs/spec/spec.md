# Specification: Ballerina ActiveMQ Classic Connector

_Authors_: Kavisha Trishan \
_Reviewers_: To be assigned \
_Created_: 2026/11/09 \
_Updated_: 2026/11/09 \
_Edition_: Swan Lake

## Introduction

This is the specification for the `ballerinax/activemq.classic` connector of the [Ballerina language](https://ballerina.io/), which sends and receives messages by connecting to an [Apache ActiveMQ Classic](https://activemq.apache.org/) broker.

The specification may evolve in the future. Released versions can be found under the relevant GitHub tag.

If you have feedback or suggestions, start a discussion through a [GitHub issue](https://github.com/ballerina-platform/ballerina-library/issues) or on the [Discord server](https://discord.gg/ballerinalang). The conforming implementation is released to Ballerina Central. Any deviation from this specification is considered a bug.

## Contents

1. [Overview](#1-overview)
2. [Connection](#2-connection)
   * 2.1. [Broker URL](#21-broker-url)
   * 2.2. [Connection Configuration](#22-connection-configuration)
   * 2.3. [Secure Communication](#23-secure-communication)
   * 2.4. [Prefetch Policy](#24-prefetch-policy)
   * 2.5. [Redelivery Policy](#25-redelivery-policy)
3. [Destinations](#3-destinations)
4. [Messages](#4-messages)
   * 4.1. [The Message Record](#41-the-message-record)
   * 4.2. [Sending Payloads](#42-sending-payloads)
   * 4.3. [Receiving Payloads](#43-receiving-payloads)
   * 4.4. [Message Properties](#44-message-properties)
   * 4.5. [Scheduled Delivery](#45-scheduled-delivery)
5. [Message Producer](#5-message-producer)
6. [Message Consumer](#6-message-consumer)
7. [Listener and Service](#7-listener-and-service)
   * 7.1. [Listener](#71-listener)
   * 7.2. [Service Configuration](#72-service-configuration)
   * 7.3. [Service Methods](#73-service-methods)
   * 7.4. [Caller](#74-caller)
   * 7.5. [Message Dispatch and Failure Handling](#75-message-dispatch-and-failure-handling)
8. [Acknowledgement Modes](#8-acknowledgement-modes)
9. [Errors](#9-errors)
10. [Samples](#10-samples)

## 1. Overview

Apache ActiveMQ Classic is an open-source message broker that implements the Jakarta Messaging (JMS) API. The connector communicates with the broker over the OpenWire protocol and supports queues (point-to-point) and topics (publish/subscribe).

The connector provides three core APIs:

| API | Purpose |
|---|---|
| `classic:MessageProducer` | Sends messages to queues and topics. |
| `classic:MessageConsumer` | Pulls messages from one fixed queue or topic. |
| `classic:Listener` | Pushes messages from queues or topics to an attached `classic:Service`. |

This version is built on:

| Component | Version |
|---|---|
| Ballerina distribution | 2201.12.10 |
| Java | 21 |
| ActiveMQ client (`activemq-client`) | 6.1.8 |
| Jakarta Messaging API (`jakarta.jms-api`) | 3.1.0 |

## 2. Connection

Each `MessageProducer`, `MessageConsumer` and `Listener` opens its own JMS connection.

### 2.1. Broker URL

Every client takes the broker URL as its first argument.

| Format | Example |
|---|---|
| TCP | `tcp://localhost:61616` |
| SSL | `ssl://localhost:61617` |
| Failover | `failover:(tcp://host1:61616,tcp://host2:61616)` |

By default, the failover transport keeps retrying, so a client using it can block while no broker is reachable. Use the ActiveMQ `startupMaxReconnectAttempts` URL option to limit startup retries.

### 2.2. Connection Configuration

`classic:ConnectionConfiguration` is accepted as included parameters by all three clients.

```ballerina
public type ConnectionConfiguration record {
    string username?;
    string password?;
    string clientID?;
    SecureSocket secureSocket?;
    boolean optimizeAcknowledgements = false;
    boolean setAlwaysSessionAsync = true;
    PrefetchPolicy prefetchPolicy?;
    RedeliveryPolicy redeliveryPolicy?;
    map<string> properties = {};
};
```

| Field | Description |
|---|---|
| `username`, `password` | Broker credentials. Provide both, or neither for anonymous access. Providing only one returns an error. |
| `clientID` | JMS client ID. If it is not set here, in `properties`, or through a `jms.clientID` option in the broker URL, a random UUID is used. |
| `secureSocket` | TLS configuration. See [Secure Communication](#23-secure-communication). |
| `optimizeAcknowledgements` | Optimizes acknowledgement processing at the cost of possible duplicates. |
| `setAlwaysSessionAsync` | Uses asynchronous dispatch for all sessions. |
| `prefetchPolicy` | See [Prefetch Policy](#24-prefetch-policy). |
| `redeliveryPolicy` | See [Redelivery Policy](#25-redelivery-policy). |
| `properties` | Additional ActiveMQ connection factory properties. |

A dedicated field takes precedence over the same entry in `properties`. Because `optimizeAcknowledgements` and `setAlwaysSessionAsync` have default values, the `optimizeAcknowledge` and `alwaysSessionAsync` entries in `properties` are always ignored.

### 2.3. Secure Communication

Use an `ssl://` URL together with `secureSocket`.

```ballerina
public type SecureSocket record {|
    TrustStore|string cert;
    KeyStore|CertKey key?;
|};
```

| Field | Description |
|---|---|
| `cert` | A trust store, or the path to a certificate file that the client trusts. Required. |
| `key` | The client's key store, or a certificate and private key pair. Required only when the broker authenticates clients. |

`classic:TrustStore` and `classic:KeyStore` extend the corresponding `ballerina/crypto` records with a `format` field of type `classic:SslStoreFormat`, which is `classic:JKS` (default) or `classic:PKCS12`. `classic:CertKey` has `certFile`, `keyFile` (PKCS8) and an optional `keyPassword`.

Certificate and private key files, given as a path in `cert` or through `classic:CertKey`, must use RSA keys.

### 2.4. Prefetch Policy

The prefetch policy sets how many messages the broker pushes to a client ahead of demand.

| Field of `classic:PrefetchPolicy` | Default | Applies to |
|---|---|---|
| `queuePrefetchSize` | `1000` | Queues |
| `topicPrefetchSize` | `32767` | Non-durable topic subscriptions |
| `durableTopicPrefetchSize` | `100` | Durable topic subscriptions |
| `optimizeDurableTopicPrefetchSize` | `1000` | Durable topics when fast consumption is expected |

Negative values return an error at initialization.

### 2.5. Redelivery Policy

The redelivery policy controls how a rolled-back or recovered message is retried. When its retries are exhausted, the message is treated as a poison message and sent to the broker's dead letter queue.

| Field of `classic:RedeliveryPolicy` | Default | Description | Valid values |
|---|---|---|---|
| `maximumRedeliveries` | `6` | Retries before the message is dead-lettered. `-1` means unlimited. | `-1` or greater |
| `initialRedeliveryDelay` | `1000` | Delay in milliseconds before the first retry. | `0` or greater |
| `redeliveryDelay` | `1000` | Base delay in milliseconds between retries. | `0` or greater |
| `maximumRedeliveryDelay` | `-1` | Upper limit on the delay in milliseconds. `-1` means no limit. | `-1` or greater |
| `useExponentialBackOff` | `false` | Multiplies the delay on each retry. | |
| `backOffMultiplier` | `5.0` | Multiplier used with exponential backoff. | Greater than `0` |
| `useCollisionAvoidance` | `false` | Adds randomness to delays. | |
| `collisionAvoidancePercent` | `15` | Percentage of randomness to apply. | `0` to `100` |
| `preDispatchCheck` | `true` | Checks the dispatch policy before redelivering. | |

A policy can be set on the connection, or per service through `classic:ServiceConfig`. The per-service policy overrides the connection policy. Values outside the valid range return an error at initialization for a connection-level policy. A per-service policy isn't validated.

## 3. Destinations

```ballerina
public type Queue record {|
    string queueName;
    boolean temporary = false;
|};

public type Topic record {|
    string topicName;
    boolean temporary = false;
|};

public type Destination Queue|Topic;
```

When `temporary` is `true`, the broker assigns the name and the given name is ignored. Each `send` to a temporary destination record creates a new temporary destination, unless the record came from a received message. A `replyTo` or `destination` value taken from a received message refers to the original destination, so it can be used to reply.

ActiveMQ creates regular queues and topics on first use. Destination names may use ActiveMQ wildcards when subscribing, for example `prices.>`.

ActiveMQ virtual topics use ordinary destination names. Publish to the topic `VirtualTopic.<name>`, and consume from the queue `Consumer.<group>.VirtualTopic.<name>`.

## 4. Messages

### 4.1. The Message Record

```ballerina
public type Message record {|
    string messageId?;
    int timestamp?;
    string correlationId?;
    Destination replyTo?;
    Destination destination?;
    boolean persistent?;
    boolean redelivered?;
    string 'type?;
    int expiry?;
    int deliveryTime?;
    int priority?;
    string userId?;
    string format?;
    map<Property> properties?;
    anydata payload;
    int scheduledDelay?;
    int scheduledPeriod?;
    int scheduledRepeat?;
    string scheduledCron?;
|};
```

| Field | On send | On receive |
|---|---|---|
| `messageId` | Ignored. The broker client assigns the ID. | Always set. |
| `timestamp` | Ignored. | Set when greater than `0`. |
| `correlationId` | Used. | Set when present. |
| `replyTo` | Used. | Set when present. |
| `destination` | Ignored. Use the `send` argument or the producer's default. | Set when present. |
| `persistent` | Used. Defaults to the JMS default, persistent. | Always set. |
| `redelivered` | Ignored. | Always set. |
| `'type` | Used. | Set when present. |
| `expiry` | Used as an absolute time in milliseconds since the epoch. `0` means no expiry. A time in the past results in a time-to-live of 1 ms. | Set when greater than `0`. |
| `deliveryTime` | Ignored. | Set when greater than `0`. |
| `priority` | Used. Must be `0` to `9`, otherwise `send` returns an error. Defaults to the JMS default, `4`. | Always set. |
| `userId` | Ignored. | Set when the message has `JMSXUserID`. |
| `format` | Ignored. | Always set: `"text"` for a `TextMessage`, `"binary"` for a `BytesMessage`, otherwise `"unknown"`. |
| `properties` | Used. See [Message Properties](#44-message-properties). | Always set. |
| `payload` | Required. See [Sending Payloads](#42-sending-payloads). | See [Receiving Payloads](#43-receiving-payloads). |
| `scheduledDelay`, `scheduledPeriod`, `scheduledRepeat`, `scheduledCron` | Used. See [Scheduled Delivery](#45-scheduled-delivery). | Not set. |

### 4.2. Sending Payloads

The runtime type of `payload` determines the JMS message that is created.

| Runtime type | JMS message |
|---|---|
| `string` | `TextMessage` |
| `xml` | `TextMessage` containing the XML text |
| `byte[]` | `BytesMessage` |
| `map` or record | `MapMessage` |
| Any other `anydata`, including tuples | `BytesMessage` containing the value as JSON |

A `MapMessage` entry can be a `string`, `int`, `float`, `boolean`, `byte` or `byte[]`. Entries of any other type, including `decimal` values, nested maps and other arrays, are dropped with a logged warning, so the delivered payload may be incomplete.

To exchange JSON, send `value.toJsonString()` and receive it as a `string`. A `json` object sent directly becomes a `MapMessage`, which drops nested values and can't be received as `json`.

### 4.3. Receiving Payloads

The declared type of the `payload` field selects the conversion. Narrow it using a record that includes `classic:Message`:

```ballerina
record {|*classic:Message; string payload;|}? message = check consumer->receive(5000);
```

| Declared `payload` type | `TextMessage` | `MapMessage` | `BytesMessage` |
|---|---|---|---|
| `anydata` (plain `classic:Message`) | `byte[]` (UTF-8) | `map<classic:Property>` | `byte[]` |
| `string` | Text | Error | Error |
| `xml` or an XML subtype | Parsed XML | Error | Error |
| `map` | Error | `map<classic:Property>` | Error |
| Record | Error | Converted to the record | Error |
| `byte[]` | Error | Error | `byte[]` |
| Any other type | Error | Error | Parsed as JSON and converted to the type |

Additional rules:

- A sequence of XML items can be bound to `xml`. Binding a sequence to a singleton XML subtype, such as `xml:Element`, is an error.
- Union types other than `anydata` are not bound from a `TextMessage` or `MapMessage`.
- A `MapMessage` entry that can't be represented as a `classic:Property` is dropped with a logged warning.
- Other JMS message types can only be received as `anydata`. A plain `Message` has no body and arrives as an empty `byte[]`. An `ObjectMessage` holding a string arrives as UTF-8 `byte[]`. A `StreamMessage`, or an `ObjectMessage` holding any other non-null value, returns an error. Any other declared type is an error.
- `receive` and `receiveNoWait` accept a read-only target type, such as `classic:Message & readonly`, but the returned value isn't actually read-only. A service's `onMessage` parameter can't be a read-only type, and attaching such a service returns an error.

A failed conversion is returned as a `classic:Error` by `receive` and `receiveNoWait`. For a service, see [Message Dispatch and Failure Handling](#75-message-dispatch-and-failure-handling).

### 4.4. Message Properties

```ballerina
public type Property boolean|int|byte|float|string|byte[];
```

On send, `boolean`, `int`, `byte`, `float` and `string` properties are applied to the message. A `byte[]` value is dropped with a logged warning, because JMS message properties don't support byte arrays.

On receive, all JMS property values are mapped to the matching Ballerina type. Values of any other type are converted to `string` with a logged warning.

Properties can be used in [message selectors](#6-message-consumer).

### 4.5. Scheduled Delivery

These fields map to the ActiveMQ scheduler properties. The broker must be configured with `schedulerSupport="true"`. Without it, these fields have no effect and the message is delivered immediately.

| Field | ActiveMQ property | Description |
|---|---|---|
| `scheduledDelay` | `AMQ_SCHEDULED_DELAY` | Delay in milliseconds before first delivery. |
| `scheduledPeriod` | `AMQ_SCHEDULED_PERIOD` | Period in milliseconds between repeated deliveries. |
| `scheduledRepeat` | `AMQ_SCHEDULED_REPEAT` | Additional deliveries after the first. `-1` repeats indefinitely. Requires `scheduledPeriod`. |
| `scheduledCron` | `AMQ_SCHEDULED_CRON` | Cron expression for the schedule. Overrides the other three fields. |

## 5. Message Producer

`classic:MessageProducer` sends messages to queues and topics.

```ballerina
public type ProducerConfiguration record {|
    *ConnectionConfiguration;
    boolean transacted = false;
    Destination destination?;
|};
```

| Field | Description |
|---|---|
| `transacted` | Sends within a local transaction that requires `commit` or `rollback`. |
| `destination` | Default destination, used when `send` is called without one. |

| Function | Description |
|---|---|
| `init(string url, *ProducerConfiguration configurations) returns Error?` | Connects to the broker. |
| `send(Message message, Destination? destination = ()) returns Error?` | Sends a message. The `destination` argument overrides the default. An error is returned if neither is available. |
| `'commit() returns Error?` | Delivers all messages sent since the last commit or rollback. Valid only with `transacted: true`. |
| `'rollback() returns Error?` | Discards all messages sent since the last commit. Valid only with `transacted: true`. |
| `close() returns Error?` | Closes the producer and its connection. Uncommitted messages are discarded. |

`close` can be called more than once. Any other call after `close` returns an error. Session operations on one producer are serialized, so it can be shared between strands.

## 6. Message Consumer

`classic:MessageConsumer` pulls messages from a queue or topic that is fixed for its lifetime. Durable subscriptions are available only through a [Listener](#72-service-configuration).

```ballerina
public type ConsumerConfiguration record {|
    *ConnectionConfiguration;
    AcknowledgementMode ackMode = AUTO_ACKNOWLEDGE;
    Destination destination;
    string messageSelector?;
|};
```

| Field | Description |
|---|---|
| `ackMode` | See [Acknowledgement Modes](#8-acknowledgement-modes). |
| `destination` | The queue or topic to consume from. |
| `messageSelector` | JMS selector expression over message properties, for example `"region = 'APAC'"`. |

| Function | Description |
|---|---|
| `init(string url, *ConsumerConfiguration configurations) returns Error?` | Connects to the broker and creates the consumer. |
| `receive(int timeoutMs = 5000, typedesc<Message> T = <>) returns T\|Error?` | Waits for a message for up to `timeoutMs` milliseconds, and returns `()` on timeout. A negative timeout returns an error. A timeout of `0` waits indefinitely. |
| `receiveNoWait(typedesc<Message> T = <>) returns T\|Error?` | Returns a message if one is immediately available, otherwise `()`. |
| `acknowledge(Message message) returns Error?` | Acknowledges the message and every earlier message received by the session. Meaningful only with `CLIENT_ACKNOWLEDGE`. It has no effect on a message that wasn't received from the connector. |
| `'commit() returns Error?` | Commits all messages received since the last commit or rollback. Valid only with `SESSION_TRANSACTED`. |
| `'rollback() returns Error?` | Rolls back those messages, making them available for redelivery. Valid only with `SESSION_TRANSACTED`. |
| `close() returns Error?` | Closes the consumer and its connection. |

`close` can be called more than once. After `close`, `receive`, `receiveNoWait`, `'commit` and `'rollback` return an error, but `acknowledge` doesn't check whether the consumer is closed. A `receive` that is already waiting when `close` is called can return `()`. Session operations are serialized, so a `commit` or `rollback` from another strand waits for a `receive` in progress to return.

## 7. Listener and Service

### 7.1. Listener

`classic:Listener` delivers messages to attached services. Its `init` takes the broker URL and `*ConnectionConfiguration`.

| Function | Description |
|---|---|
| `attach(Service s, string[]\|string? name = ()) returns Error?` | Validates the service and creates its session and consumer. If the listener has already started, the service starts consuming immediately. `name` is reserved and ignored. |
| `detach(Service s) returns Error?` | Stops consumption for the service. |
| `'start() returns Error?` | Starts the connection and consumption for all attached services. |
| `gracefulStop() returns Error?` | Closes each service's consumer and session, then closes the connection. |
| `immediateStop() returns Error?` | In this version, behaves the same as `gracefulStop`. |

Closing a service's consumer and session is limited to 30 seconds. If that limit is exceeded, the operation returns an error.

### 7.2. Service Configuration

A service must have the `@classic:ServiceConfig` annotation. Its type is `classic:ServiceConfiguration`, which is either `QueueConfig` or `TopicConfig`. An empty `queueName` or `topicName` returns an error when the service is attached.

Fields common to both:

| Field | Default | Description |
|---|---|---|
| `sessionAckMode` | `AUTO_ACKNOWLEDGE` | See [Acknowledgement Modes](#8-acknowledgement-modes). |
| `messageSelector` | None | JMS selector expression over message properties. |
| `exclusive` | `false` | Requests ActiveMQ's exclusive consumer behaviour for the destination. |
| `redeliveryPolicy` | None | Overrides the connection's redelivery policy for this service. |

`QueueConfig` adds a required `queueName`. `TopicConfig` adds:

| Field | Default | Description |
|---|---|---|
| `topicName` | Required | The topic to subscribe to. |
| `noLocal` | `false` | Doesn't deliver messages published on the listener's own connection. Each connector client opens its own connection, so messages sent through a `classic:MessageProducer` are still delivered, even with the same client ID. |
| `consumerType` | `DEFAULT` | A `classic:ConsumerType`, which is `classic:DEFAULT` or `classic:DURABLE`. |
| `subscriberName` | None | Identifies a durable subscription. Set it for `DURABLE`. The connector doesn't check that it's present. |

A durable subscription is identified by the client ID together with `subscriberName`. Set a stable `clientID` on the listener, because the generated default changes on every start.

### 7.3. Service Methods

There is no compiler plugin, so the following rules are checked when the service is attached. A violation returns a `classic:Error`.

- A service has no resource methods.
- It has `onMessage` and may have `onError`. No other remote methods are allowed.

Accepted signatures:

```ballerina
remote function onMessage(classic:Message message) returns error?;
remote function onMessage(classic:Message message, classic:Caller caller) returns error?;
remote function onMessage(record {|*classic:Message; T payload;|} message) returns error?;
remote function onMessage(record {|*classic:Message; T payload;|} message, classic:Caller caller) returns error?;
remote function onError(classic:Error err) returns error?;
```

The message parameter may only narrow `payload`, which follows the rules in [Receiving Payloads](#43-receiving-payloads). All other `classic:Message` fields must keep their types and optionality. Any additional fields must be optional.

### 7.4. Caller

`classic:Caller` operates on the session of the service that received the message.

| Function | Description |
|---|---|
| `acknowledge(Message message) returns Error?` | Acknowledges the message and every earlier message received by the session. Meaningful only with `CLIENT_ACKNOWLEDGE`. |
| `'commit() returns Error?` | Commits the session's transaction. Valid only with `SESSION_TRANSACTED`. |
| `'rollback() returns Error?` | Rolls back the session's transaction. Rolled-back messages are redelivered according to the redelivery policy. Valid only with `SESSION_TRANSACTED`. |

### 7.5. Message Dispatch and Failure Handling

Each service has its own session and receives messages one at a time, in order. `onError` runs asynchronously.

| Situation | Effect on the message | Notification |
|---|---|---|
| `onMessage` returns normally | Handled according to the acknowledgement mode. | None. |
| `onMessage` returns an error | Not rolled back or redelivered by the connector. In `AUTO_ACKNOWLEDGE` mode the message is acknowledged. | `onError` is called. |
| `onMessage` panics | Not rolled back or redelivered by the connector. | Printed to standard error. |
| The payload can't be bound to the declared type | Handed back for redelivery. It is sent to the dead letter queue once the redelivery policy is exhausted. | `onError` is called on every attempt. |

To retry after a business failure, use `SESSION_TRANSACTED` and call `caller->'rollback()`.

Handing back a message after a binding failure works as follows: `SESSION_TRANSACTED` rolls back the transaction and `CLIENT_ACKNOWLEDGE` recovers the session, so other uncommitted or unacknowledged messages in the session are also redelivered. In the remaining modes, the ActiveMQ client performs the redelivery.

If the service has no `onError` method, the error is printed to standard error. If `onError` returns an error, it is printed. If `onError` panics, consumption for that service stops.

## 8. Acknowledgement Modes

| `classic:AcknowledgementMode` | Behaviour |
|---|---|
| `AUTO_ACKNOWLEDGE` | The session acknowledges each message automatically when `receive` returns or `onMessage` completes. |
| `CLIENT_ACKNOWLEDGE` | The application calls `acknowledge`. Acknowledging a message also acknowledges every earlier message received by the session. |
| `DUPS_OK_ACKNOWLEDGE` | The session acknowledges lazily, which can deliver duplicates after a failure. |
| `SESSION_TRANSACTED` | Messages are grouped into a local transaction that the application ends with `commit` or `rollback`. |

## 9. Errors

The connector has one error type:

```ballerina
public type Error distinct error;
```

Connection, send, receive, acknowledgement, transaction, validation and data binding failures are all returned as `classic:Error`. The JMS exception, when there is one, is available as the error's cause.

## 10. Samples

### 10.1. Send Messages in a Transaction

```ballerina
import ballerinax/activemq.classic;

public function main() returns error? {
    classic:MessageProducer producer = check new ("tcp://localhost:61616",
        username = "admin",
        password = "admin",
        transacted = true,
        destination = {queueName: "orders"}
    );

    check producer->send({payload: "order-1", persistent: true, priority: 7});
    check producer->send({payload: "order-2", properties: {"region": "APAC"}});
    check producer->'commit();
    check producer->close();
}
```

### 10.2. Pull Messages with Client Acknowledgement

```ballerina
import ballerina/io;
import ballerinax/activemq.classic;

public function main() returns error? {
    classic:MessageConsumer consumer = check new ("tcp://localhost:61616",
        username = "admin",
        password = "admin",
        destination = {queueName: "orders"},
        ackMode = classic:CLIENT_ACKNOWLEDGE
    );

    record {|*classic:Message; string payload;|}? message = check consumer->receive(5000);
    if message !is () {
        io:println("Received: ", message.payload);
        check consumer->acknowledge(message);
    }
    check consumer->close();
}
```

### 10.3. Subscribe to a Durable Topic with a Listener

```ballerina
import ballerina/log;
import ballerinax/activemq.classic;

listener classic:Listener eventListener = check new ("tcp://localhost:61616",
    username = "admin",
    password = "admin",
    clientID = "audit-service"
);

@classic:ServiceConfig {
    topicName: "order.events",
    consumerType: classic:DURABLE,
    subscriberName: "audit",
    sessionAckMode: classic:CLIENT_ACKNOWLEDGE
}
service classic:Service on eventListener {
    remote function onMessage(record {|*classic:Message; string payload;|} message,
            classic:Caller caller) returns error? {
        log:printInfo("Event received", payload = message.payload);
        check caller->acknowledge(message);
    }

    remote function onError(classic:Error err) returns error? {
        log:printError("Failed to process an event", err);
    }
}
```
