// Copyright (c) 2025, WSO2 LLC. (http://www.wso2.com)
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

import ballerina/crypto;

# The ActiveMQ service type attached to a `classic:Listener` for asynchronous (push-based)
# consumption.
#
# An attached service must declare a remote `onMessage` method and may optionally declare an
# `onError` method. The accepted signatures are:
# ```ballerina
# remote function onMessage(record {|*classic:Message; T payload;|} message) returns classic:Error?;
# remote function onMessage(record {|*classic:Message; T payload;|} message, classic:Caller caller) returns classic:Error?;
# remote function onMessage(classic:Message message) returns classic:Error?;
# remote function onError(classic:Error err) returns classic:Error?;
# ```
# Declaring a narrowed `payload` type (`T`) causes the message payload to be data-bound into that
# type; declaring the base `classic:Message` type yields the raw payload as `anydata`.
# The subscription (queue or topic) and consumer options are supplied via the
# `@classic:ServiceConfig` annotation on the service.
public type Service distinct service object {};

# Represents a queue destination for point-to-point messaging.
public type Queue record {|
    # The name of the queue. Ignored when `temporary` is `true` — the broker assigns the name.
    string queueName;
    # Whether this is a connection-scoped temporary queue rather than a regular, named one.
    boolean temporary = false;
|};

# Represents a topic destination for publish/subscribe messaging.
public type Topic record {|
    # The name of the topic. Ignored when `temporary` is `true` — the broker assigns the name.
    string topicName;
    # Whether this is a connection-scoped temporary topic rather than a regular, named one.
    boolean temporary = false;
|};

# Represents a JMS destination.
public type Destination Queue|Topic;

# ActiveMQ broker connection configuration.
#
# + username - Username for broker authentication (must be provided with password)
# + password - Password for broker authentication (must be provided with username)
# + clientID - The JMS client ID to use for identifying this client connection. If not specified,
#              a random UUID will be generated
# + secureSocket - SSL/TLS encryption configurations for secure broker communication
# + optimizeAcknowledgements - When enabled, optimizes acknowledgement processing for better
#                              performance at the cost of potential duplicate messages
# + setAlwaysSessionAsync - When enabled, all sessions will use asynchronous message delivery
# + redeliveryPolicy - Configurations that control how failed messages are redelivered
# + prefetchPolicy - Configurations that control how many messages are prefetched from the broker
# + properties - Additional broker-specific connection properties as key-value pairs. Note: If the
#                same property is configured in both this map and dedicated fields, the dedicated
#                field value takes precedence
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

# Configuration for an ActiveMQ message producer.
#
# + transacted - Enables transacted messaging. Messages are sent within a transaction context,
#                requiring an explicit commit or rollback
# + destination - The default destination (queue or topic) to publish to. Can be omitted and/or
#                 overridden per call via the `destination` parameter of `MessageProducer.send()`
public type ProducerConfiguration record {|
    *ConnectionConfiguration;
    boolean transacted = false;
    Destination destination?;
|};

# Configuration for an ActiveMQ message consumer.
#
# + ackMode - Specifies how messages received by the session will be acknowledged
# + destination - The queue or topic to consume messages from, fixed for this consumer's lifetime
# + messageSelector - JMS message selector expression to filter messages. Only messages with
#                      properties matching the selector are delivered. For example, to receive only
#                      messages with property `priority` set to `'high'`, use: `"priority = 'high'"`.
#                      If not set, all messages from the destination will be delivered.
public type ConsumerConfiguration record {|
    *ConnectionConfiguration;
    AcknowledgementMode ackMode = AUTO_ACKNOWLEDGE;
    Destination destination;
    string messageSelector?;
|};

# Configurations for message prefetching behavior. Prefetching allows the broker to send messages
# to the consumer before they are explicitly requested, improving throughput by reducing network
# round trips.
#
# + queuePrefetchSize - Number of messages to prefetch for queue subscriptions (default: 1000)
# + topicPrefetchSize - Number of messages to prefetch for non-durable topic subscriptions
#                       (default: 32767)
# + durableTopicPrefetchSize - Number of messages to prefetch for durable topic subscriptions
#                              (default: 100)
# + optimizeDurableTopicPrefetchSize - Optimized prefetch size for durable topics when fast
#                                      consumption is expected (default: 1000)
public type PrefetchPolicy record {|
    int queuePrefetchSize = 1000;
    int topicPrefetchSize = 32767;
    int durableTopicPrefetchSize = 100;
    int optimizeDurableTopicPrefetchSize = 1000;
|};

# Configurations for secure communication with the ActiveMQ broker.
#
# + cert - Configurations associated with `TrustStore` or single certificate file that the client trusts
# + key - Configurations associated with `KeyStore` or combination of certificate and private key of the client
public type SecureSocket record {|
    TrustStore|string cert;
    KeyStore|CertKey key?;
|};

# Java KeyStore format.
public const JKS = "jks";
# PKCS12 format.
public const PKCS12 = "pkcs12";

# Represents the supported SSL store formats.
public type SslStoreFormat JKS|PKCS12;

# Represents a trust store containing trusted CA certificates.
#
# + format - The format of the truststore file
public type TrustStore record {|
    *crypto:TrustStore;
    SslStoreFormat format = JKS;
|};

# Represents a key store containing the client's private key and certificate.
#
# + format - The format of the keystore file
public type KeyStore record {|
    *crypto:KeyStore;
    SslStoreFormat format = JKS;
|};

# Represents a combination of certificate, private key, and private key password if encrypted.
#
# + certFile - A file containing the certificate
# + keyFile - A file containing the private key in PKCS8 format
# + keyPassword - Password of the private key if it is encrypted
public type CertKey record {|
    string certFile;
    string keyFile;
    string keyPassword?;
|};

# Represents a message received from an ActiveMQ broker.
#
# + messageId - Unique identifier assigned to the message by the JMS provider
# + timestamp - The time when the message was handed to the provider to be sent (in milliseconds
#               since epoch)
# + correlationId - Optional identifier used to correlate this message with another (typically used
#                   in request-reply patterns)
# + replyTo - Queue or topic where replies should be sent
# + destination - Queue or topic where the message was sent
# + persistent - Message delivery mode: true for persistent (survives broker restarts), false for
#                non-persistent (faster but may be lost on broker failure)
# + redelivered - Indicates whether this message is being redelivered after a previous delivery
#                 attempt failed
# + 'type - Message type identifier string (provider-specific or application-defined) used for
#           message classification and filtering
# + expiry - Timestamp (in milliseconds since epoch) when the message expires, or 0 if it doesn't
#            expire
# + deliveryTime - The earliest time when the message can be delivered to a consumer (in
#                  milliseconds since epoch), or 0 if no delivery delay
# + priority - Message priority level (0-9, where 0-4 is normal priority and 5-9 is expedited
#              priority)
# + userId - Identifier of the user who sent the message (if available from the broker)
# + format - Format identifier of the message payload (e.g., "text", "binary", "json")
# + properties - Application-specific custom properties attached to the message. Values are
#                restricted to the types the JMS specification allows as message properties
# + payload - The message content. On send, the runtime type determines the JMS message produced
#             (`string` → `TextMessage`, `byte[]` → `BytesMessage`, `map`/record → `MapMessage`).
#             On receive, defaults to the JMS-message-appropriate representation unless narrowed
#             via `MessageConsumer.receive`'s `typedesc<Message> T` parameter
# + scheduledDelay - Delay in milliseconds before the broker first delivers the message.
#                   Requires `schedulerSupport="true"` in the ActiveMQ broker configuration.
# + scheduledPeriod - Period in milliseconds between successive redeliveries. Use together
#                    with `scheduledRepeat` for periodic delivery.
# + scheduledRepeat - Number of additional deliveries after the first. Use `-1` for
#                    infinite repetition. Requires `scheduledPeriod` to be set.
# + scheduledCron - Cron expression controlling the delivery schedule
#                  (for example, `"0 0 12 * * ?"`). Overrides delay/period/repeat when set.
#                  Requires the broker scheduler to be enabled.
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

# Represents the allowed value types for a JMS message property, and for a typed `MapMessage` entry.
public type Property boolean|int|byte|float|string|byte[];

# Defines the supported JMS message consumer types for ActiveMQ topic subscriptions.
public enum ConsumerType {
    # Durable subscriber - Subscription persists even when the client disconnects. Messages
    # published while the subscriber is offline will be delivered when it reconnects.
    DURABLE,
    # Default (non-durable) consumer - Subscription exists only while the client is connected.
    # Messages published while the consumer is disconnected are not delivered.
    DEFAULT
}

# Defines the JMS session acknowledgement modes.
public enum AcknowledgementMode {
    # Indicates that the session will use a local transaction which may subsequently be
    # committed or rolled back by calling the session's `commit` or `rollback` methods.
    SESSION_TRANSACTED,
    # Indicates that the session automatically acknowledges a client's receipt of a message
    # either when the session has successfully returned from a call to `receive` or when the
    # message listener the session has called to process the message successfully returns.
    AUTO_ACKNOWLEDGE,
    # Indicates that the client acknowledges a consumed message by calling the MessageConsumer's
    # or Caller's `acknowledge` method. Acknowledging a consumed message acknowledges all
    # messages that the session has consumed.
    CLIENT_ACKNOWLEDGE,
    # Indicates that the session lazily acknowledges the delivery of messages. This is likely to
    # result in the delivery of some duplicate messages if the JMS provider fails, so it should
    # only be used by consumers that can tolerate duplicate messages. Use of this mode can reduce
    # session overhead by minimizing the work the session does to prevent duplicates.
    DUPS_OK_ACKNOWLEDGE
}

# Configurations for message redelivery behavior. RedeliveryPolicy defines how messages are
# redelivered when they are rolled back. This can be set at the connection level or per consumer.
# The consumer level redelivery policy overrides the connection level redelivery policy.
#
# + collisionAvoidancePercent - Percentage of randomness (0-100) to apply to redelivery delays to
#                               avoid collision when multiple consumers retry simultaneously
# + maximumRedeliveries - The maximum number of times to redeliver a message before it is
#                         considered a poison message and moved to a dead letter queue
# + maximumRedeliveryDelay - The maximum delay (in milliseconds) between redelivery attempts. This
#                            helps to cap the delay when using exponential backoff
# + initialRedeliveryDelay - The initial delay (in milliseconds) before the first redelivery
#                            attempt
# + useCollisionAvoidance - When enabled, adds randomness to redelivery delays to prevent multiple
#                           consumers from retrying simultaneously
# + useExponentialBackOff - When enabled, increases the delay between redelivery attempts
#                           exponentially using the backOffMultiplier
# + backOffMultiplier - Multiplier applied to the delay on each redelivery when exponential backoff
#                       is enabled (e.g., 5.0 means each retry delay is 5x the previous)
# + redeliveryDelay - The base delay (in milliseconds) between redelivery attempts
# + preDispatchCheck - When enabled, checks the dispatch policy before redelivering a message
public type RedeliveryPolicy record {|
    int collisionAvoidancePercent = 15;
    int maximumRedeliveries = 6;
    int maximumRedeliveryDelay = -1;
    int initialRedeliveryDelay = 1000;
    boolean useCollisionAvoidance = false;
    boolean useExponentialBackOff = false;
    float backOffMultiplier = 5.0;
    int redeliveryDelay = 1000;
    boolean preDispatchCheck = true;
|};

# Common configurations related to ActiveMQ queue or topic subscriptions.
#
# + sessionAckMode - Specifies how messages received by the session will be acknowledged
# + messageSelector - JMS message selector expression to filter messages. Only messages with
#                     properties matching the selector are delivered. For example, to receive only
#                     messages with property `priority` set to `'high'`, use: `"priority = 'high'"`.
#                     If not set, all messages from the destination will be delivered.
# + exclusive - When enabled, only this consumer can consume messages from the destination,
#               preventing other consumers from receiving messages from the same destination
# + redeliveryPolicy - Configurations for message redelivery behavior when messages fail processing
type CommonSubscriptionConfig record {|
    AcknowledgementMode sessionAckMode = AUTO_ACKNOWLEDGE;
    string messageSelector?;
    boolean exclusive = false;
    RedeliveryPolicy redeliveryPolicy?;
|};

# Configuration for an ActiveMQ queue.
#
# + queueName - The name of the queue to consume messages from
public type QueueConfig record {|
    *CommonSubscriptionConfig;
    string queueName;
|};

# Configuration for an ActiveMQ topic subscription.
#
# + topicName - The name of the topic to subscribe to
# + noLocal - When enabled, messages published to the topic using this session's connection, or any
#             other connection with the same client identifier, will not be delivered to this
#             subscriber (useful to avoid receiving your own messages)
# + consumerType - The message consumer type (DURABLE or DEFAULT). Use DURABLE for subscriptions
#                  that should persist when the client disconnects.
# + subscriberName - The name used to identify a durable subscription (required for DURABLE
#                    consumer type)
public type TopicConfig record {|
    *CommonSubscriptionConfig;
    string topicName;
    boolean noLocal = false;
    ConsumerType consumerType = DEFAULT;
    string subscriberName?;
|};

# The service configuration type for the `classic:Service`.
public type ServiceConfiguration QueueConfig|TopicConfig;

# Annotation to configure the `classic:Service`.
public annotation ServiceConfiguration ServiceConfig on service;
