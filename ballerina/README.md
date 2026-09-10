## Overview

[Apache ActiveMQ Classic](https://activemq.apache.org/) is a widely used, open-source message
broker that implements the Java Message Service (JMS) API, enabling reliable, asynchronous
communication between distributed applications through queues (point-to-point) and topics
(publish/subscribe).

The `ballerinax/activemq.classic` package provides APIs to interact with ActiveMQ Classic brokers over the
OpenWire protocol. It allows developers to programmatically produce and consume messages, subscribe
to queues and topics both synchronously and asynchronously, and build reliable, event-driven
integrations that leverage ActiveMQ's messaging capabilities within Ballerina applications.

### Key Features

- Point-to-point (queue) and publish/subscribe (topic) messaging, including durable topic subscriptions
- Both synchronous (pull-based `MessageConsumer`) and asynchronous (push-based `Listener`) consumption
- Transacted sessions with `commit`/`rollback`, on both the producer and consumer side
- Typed payload data binding on `receive`/`receiveNoWait` and on a `Listener` service's `onMessage`
- Secure communication over TLS/SSL
- Message selectors, redelivery and prefetch policy tuning, and scheduled message delivery

## Setup guide

To try out the `ballerinax/activemq.classic` connector, you need a running ActiveMQ Classic broker. The
quickest way to get one locally is with Docker.

### Start an ActiveMQ broker with Docker

```bash
docker run -d \
  --name activemq-local \
  -p 61616:61616 \
  -p 8161:8161 \
  -e ACTIVEMQ_ADMIN_LOGIN=admin \
  -e ACTIVEMQ_ADMIN_PASSWORD=admin \
  apache/activemq-classic:6.2.0
```

Once the container is up and running, the broker is reachable at `tcp://localhost:61616` with the
credentials `admin`/`admin` - the same defaults used throughout this guide. The admin console is
available at [http://localhost:8161](http://localhost:8161). Unlike some other JMS brokers, ActiveMQ
creates queues and topics on first use, so no separate provisioning step is needed.

## Quickstart

### Step 1: Import the module

Import the `ballerinax/activemq.classic` module into your Ballerina project.

```ballerina
import ballerinax/activemq.classic;
```

### Step 2: Instantiate a new connector

#### Initialize a `classic:MessageProducer`

```ballerina
configurable string brokerUrl = "tcp://localhost:61616";
configurable string username = "admin";
configurable string password = "admin";

classic:MessageProducer producer = check new (brokerUrl, username = username, password = password);
```

#### Initialize a `classic:MessageConsumer`

A `MessageConsumer` is bound to a single queue or topic for its whole lifetime:

```ballerina
configurable string brokerUrl = "tcp://localhost:61616";
configurable string username = "admin";
configurable string password = "admin";
configurable string queueName = "orders.queue";

classic:MessageConsumer consumer = check new (brokerUrl,
    username = username,
    password = password,
    destination = {queueName}
);
```

#### Initialize a `classic:Listener`

```ballerina
configurable string brokerUrl = "tcp://localhost:61616";
configurable string username = "admin";
configurable string password = "admin";
configurable string queueName = "orders.queue";

listener classic:Listener mqListener = check new (brokerUrl, username = username, password = password);

@classic:ServiceConfig {
    queueName
}
service classic:Service on mqListener {
    remote function onMessage(classic:Message message) returns error? {
        // Process the received message
    }
}
```

### Step 3: Invoke the connector operations

Now you can use the available connector operations to interact with the ActiveMQ broker.

#### Send a message to a queue

```ballerina
check producer->send({
    payload: "This is a sample message"
}, {queueName});
```

#### Receive a message from a queue

```ballerina
record {|*classic:Message; string payload;|}? message = check consumer->receive(5000);
```

The `payload` field's declared type in the target record determines how the message body is
converted. A `Listener` service's `onMessage` accepts the same narrowed-payload pattern - it does
not need to be explicitly invoked, since it's called automatically for every message on the queue
or topic once the listener starts.

### Step 4: Run the Ballerina application

```bash
bal run
```

## Examples

The `ballerinax/activemq.classic` connector provides practical examples illustrating usage in various
scenarios. Explore these [examples](https://github.com/ballerina-platform/module-ballerinax-activemq.classic/tree/main/examples):

1. [Send To Queue](https://github.com/ballerina-platform/module-ballerinax-activemq.classic/tree/main/examples/send-to-queue) - Send a small batch of orders to a queue with `classic:MessageProducer`. The most basic point-to-point send flow.

2. [Receive From Queue](https://github.com/ballerina-platform/module-ballerinax-activemq.classic/tree/main/examples/receive-from-queue) - Pull-receive messages from a queue with `classic:MessageConsumer`, narrowing the payload to a `string` at the call site.

3. [Listener Service](https://github.com/ballerina-platform/module-ballerinax-activemq.classic/tree/main/examples/listener-service) - Subscribe to a queue declaratively with `classic:Listener`/`classic:Service`, processing each message as it's pushed to the service instead of pulling for it.
