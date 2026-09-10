# module-ballerinax-activemq.classic

[![Build](https://github.com/ballerina-platform/module-ballerinax-activemq.classic/actions/workflows/ci.yml/badge.svg)](https://github.com/ballerina-platform/module-ballerinax-activemq.classic/actions/workflows/ci.yml)
[![Trivy](https://github.com/ballerina-platform/module-ballerinax-activemq.classic/actions/workflows/trivy-scan.yml/badge.svg)](https://github.com/ballerina-platform/module-ballerinax-activemq.classic/actions/workflows/trivy-scan.yml)
[![GraalVM Check](https://github.com/ballerina-platform/module-ballerinax-activemq.classic/actions/workflows/build-with-bal-test-graalvm.yml/badge.svg)](https://github.com/ballerina-platform/module-ballerinax-activemq.classic/actions/workflows/build-with-bal-test-graalvm.yml)
[![GitHub Last Commit](https://img.shields.io/github/last-commit/ballerina-platform/module-ballerinax-activemq.classic.svg)](https://github.com/ballerina-platform/module-ballerinax-activemq.classic/commits/main)
[![GitHub Issues](https://img.shields.io/github/issues/ballerina-platform/ballerina-library/module/activemq.svg?label=Open%20Issues)](https://github.com/ballerina-platform/ballerina-library/labels/module%2Factivemq)

Ballerina connector for the Apache ActiveMQ (Classic) message broker.

## Setup guide

This guide covers everything needed to get the connector running locally — from starting
a broker to sending your first message.

### Prerequisites

| Requirement | Version | Notes |
|---|---|---|
| [Ballerina Swan Lake](https://ballerina.io/downloads/) | 2201.x or later | `bal` CLI must be on `PATH` |
| Java (JDK) | 17 or 21 | Set `JAVA_HOME` |
| [Docker](https://docs.docker.com/get-docker/) | any recent version | Only needed for the local broker |

### 1. Start an ActiveMQ broker

The fastest way to get a broker running is Docker:

```bash
docker run -d \
  --name activemq-local \
  -p 61616:61616 \
  -p 8161:8161 \
  -e ACTIVEMQ_ADMIN_LOGIN=admin \
  -e ACTIVEMQ_ADMIN_PASSWORD=admin \
  apache/activemq-classic:6.2.0
```

Wait until the broker is ready (the web console at <http://localhost:8161> becomes
accessible, admin / admin).  You can also run the compose file that ships with
the repository — it starts both a plain and an SSL-enabled broker:

```bash
docker compose -f ballerina/tests/resources/docker-compose.yaml up -d
```

### 2. Add the dependency

In your `Ballerina.toml`:

```toml
[[dependency]]
org = "ballerinax"
name = "activemq.classic"
version = "0.1.0"
```

### 3. Send a message to a queue

```ballerina
import ballerinax/activemq.classic;

public function main() returns error? {
    classic:MessageProducer producer = check new ("tcp://localhost:61616",
        username = "admin",
        password = "admin"
    );

    check producer->send({
        payload: "{'item':'book','qty':2}".toBytes(),
        properties: {"region": "APAC"}
    }, {queueName: "orders.queue"});

    check producer->close();
}
```

`messageId` is optional — the JMS provider assigns the real one on send, and it's populated on
any message you receive back. Set it yourself only if you have a specific reason to; use
`correlationId` instead for application-level correlation.

A producer can also be configured with a default `destination`, letting you omit it on every
`send()` call unless you need to override it for a specific message:

```ballerina
classic:MessageProducer producer = check new ("tcp://localhost:61616",
    destination = {queueName: "orders.queue"}
);
check producer->send({payload: "{'item':'book','qty':2}".toBytes()});
```

### 4. Receive a message from a queue

A `MessageConsumer` is bound to a single queue or topic for its whole lifetime:

```ballerina
import ballerinax/activemq.classic;
import ballerina/io;

public function main() returns error? {
    classic:MessageConsumer consumer = check new ("tcp://localhost:61616",
        username = "admin",
        password = "admin",
        destination = {queueName: "orders.queue"}
    );

    record {|*classic:Message; string payload;|}? msg = check consumer->receive(5000);
    if msg is classic:Message {
        io:println("Received: ", msg.payload);
    }

    check consumer->close();
}
```

The `payload` field's declared type in the target record determines how the message body is
converted — `string`/`xml` for a `TextMessage`, `byte[]` for a `BytesMessage`, or a `map`/record
shape for a `MapMessage`. Omit the explicit type (`classic:Message? msg = check consumer->receive(5000);`)
to get the JMS-message-appropriate default representation instead.

### 5. Subscribe with a Listener service

The `Listener` delivers each message pushed to a queue (or topic) to the `onMessage`
remote method:

```ballerina
import ballerinax/activemq.classic;
import ballerina/log;

listener classic:Listener mqListener = check new ("tcp://localhost:61616",
    username = "admin",
    password = "admin"
);

@classic:ServiceConfig {
    queueName: "orders.queue"
}
service classic:Service on mqListener {
    remote function onMessage(classic:Message message) returns error? {
        string text = check string:fromBytes(check message.payload.ensureType());
        log:printInfo("Processing order: " + text);
    }
}
```

Use `topicName` instead of `queueName` to subscribe to a JMS topic.

`onMessage`'s parameter can also narrow `Message`'s `payload` field to a specific type, the same
way `MessageConsumer.receive()` does:

```ballerina
service classic:Service on mqListener {
    remote function onMessage(record {|*classic:Message; string payload;|} message) returns error? {
        log:printInfo("Processing order: " + message.payload);
    }
}
```

The `payload` field's declared type determines the conversion — see [section 4](#4-receive-a-message-from-a-queue)
for the full set of supported combinations. A payload that can't be converted to the requested
type is routed to `onError` (if defined) instead of being silently dropped.

### 6. Publish to a topic

Use a `Topic` destination when calling `send`:

```ballerina
check producer->send({
    correlationId: "evt-001",
    payload: "order placed".toBytes()
}, {topicName: "order.events"});
```

### 7. Transactional sends

Configure the producer with `transacted: true`, then group multiple sends into a single
atomic operation:

```ballerina
classic:MessageProducer producer = check new ("tcp://localhost:61616", transacted = true);
check producer->send({correlationId: "tx-1", payload: "order A".toBytes()}, {queueName: "orders.queue"});
check producer->send({correlationId: "tx-2", payload: "audit A".toBytes()}, {queueName: "audit.queue"});
check producer->'commit();   // both messages are delivered together
check producer->close();
```

Call `'rollback()` (or simply `close()` without committing) to discard all buffered
messages. `'commit()`/`'rollback()` return a `classic:Error` if the producer wasn't
configured with `transacted: true`.

### 8. SSL / TLS connection

Pass a `secureSocket` block to connect over `ssl://`:

```ballerina
classic:MessageProducer producer = check new ("ssl://localhost:61617",
    secureSocket = {
        cert: "/path/to/broker.pem",                 // broker's CA certificate
        key: {
            certFile: "/path/to/client-cert.pem",
            keyFile:  "/path/to/client.key"
        }
    }
);
```

Alternatively, use `crypto:TrustStore` / `crypto:KeyStore` (`.jks` / `.p12` files)
instead of PEM paths.

### 9. Message selector

Configure a consumer to receive only messages whose properties match a JMS selector
expression:

```ballerina
// Only receive messages where the "region" property equals "APAC"
classic:MessageConsumer consumer = check new ("tcp://localhost:61616",
    destination = {queueName: "orders.queue"},
    messageSelector = "region = 'APAC'"
);
classic:Message? msg = check consumer->receive(5000);
```

The same `messageSelector` field is available in `@ServiceConfig` for listener services.

### 10. Tuning prefetch and redelivery

`PrefetchPolicy` controls how many messages the broker pushes to the client ahead of demand
(higher values trade memory for throughput); `RedeliveryPolicy` controls retry behavior for a
message that's rolled back or left unacknowledged:

```ballerina
classic:MessageConsumer consumer = check new ("tcp://localhost:61616",
    destination = {queueName: "orders.queue"},
    prefetchPolicy = {queuePrefetchSize: 100},
    redeliveryPolicy = {maximumRedeliveries: 3, initialRedeliveryDelay: 2000}
);
```

Here, the broker prefetches up to 100 messages for this consumer, and a message that's rolled
back is redelivered up to 3 times, waiting 2 seconds before the first retry. Both configurations
are also accepted by `MessageProducer` and `Listener`.

### Broker containers (for local development)

| Container | Port | Purpose |
|---|---|---|
| `activemq-local` | `61616` (OpenWire) | Plain TCP — used by all non-SSL examples |
| `activemq-ssl-test-server` | `61617` (OpenWire/SSL) | SSL — mounts a pre-generated keystore; started by the compose file |
| Web console | `8161` | Admin UI at <http://localhost:8161> (admin / admin) |

### Stop the broker

```bash
docker stop activemq-local && docker rm activemq-local
# or, if started via compose:
docker compose -f ballerina/tests/resources/docker-compose.yaml down
```

---

## Running integration tests

### Prerequisites

- Docker must be running.
- Java 17+.

### Start the broker and run tests

```bash
./gradlew build
```

The Gradle build automatically starts an ActiveMQ Classic 6.2.0 container, runs the
integration tests, and stops the container.  If Docker is not available, the integration
tests are skipped automatically with the message:
`ActiveMQ integration tests skipped: Docker is not available.`

### Run only the integration tests (broker already running)

```bash
cd ballerina && bal test
```

To run only a specific group:

```bash
bal test --groups integration   # All new integration tests
bal test --groups client        # Client send/receive tests
bal test --groups ssl           # SSL connection tests
```

### Start the broker manually for local development

```bash
./gradlew startActiveMQBroker
```

The task waits until the broker's web console (port 8161) responds before returning.

### Stop the broker manually

```bash
./gradlew stopActiveMQBroker
```

### Test configuration

Integration tests read the broker URL and credentials from
`ballerina/tests/Config.toml` (gitignored).  Copy the committed example and adjust:

```bash
cp ballerina/tests/Config.toml.example ballerina/tests/Config.toml
```

The default values target the Docker broker started by Gradle and require no changes
for local development.

### Broker containers

| Container | Port | Purpose |
|---|---|---|
| `activemq-test-server` | `61616` | Plain OpenWire — used by all non-SSL tests |
| `activemq-ssl-test-server` | `61617` | SSL OpenWire — used by SSL tests; mounts a pre-generated keystore/truststore and a custom `activemq.xml` |

## Build from the source

### Setting up the prerequisites

1. Download and install Java SE Development Kit (JDK) version 21. You can download it from either of the following sources:

    * [Oracle JDK](https://www.oracle.com/java/technologies/downloads/)
    * [OpenJDK](https://adoptium.net/)

   > **Note:** After installation, remember to set the `JAVA_HOME` environment variable to the directory where JDK was installed.

2. Download and install [Ballerina Swan Lake](https://ballerina.io/).

3. Export Github Personal access token with read package permissions as follows,

    ```bash
    export packageUser=<Username>
    export packagePAT=<Personal access token>
    ```

### Build options

Execute the commands below to build from the source.

1. To build the package:

   ```bash
   ./gradlew clean build
   ```

2. To run the tests:

   ```bash
   ./gradlew clean test
   ```

3. To build without the tests:

   ```bash
   ./gradlew clean build -x test
   ```

4. To run tests against different environments:

   ```bash
   ./gradlew clean test -Pgroups=<Comma separated groups/test cases>
   ```

5. To debug the package with a remote debugger:

   ```bash
   ./gradlew clean build -Pdebug=<port>
   ```

6. To debug with the Ballerina language:

   ```bash
   ./gradlew clean build -PbalJavaDebug=<port>
   ```

7. Publish the generated artifacts to the local Ballerina Central repository:

    ```bash
    ./gradlew clean build -PpublishToLocalCentral=true
    ```

8. Publish the generated artifacts to the Ballerina Central repository:

   ```bash
   ./gradlew clean build -PpublishToCentral=true
   ```

## Contribute to Ballerina

As an open-source project, Ballerina welcomes contributions from the community.

For more information, go to the [contribution guidelines](https://github.com/ballerina-platform/ballerina-lang/blob/master/CONTRIBUTING.md).

## Code of conduct

All the contributors are encouraged to read the [Ballerina Code of Conduct](https://ballerina.io/code-of-conduct).

## Useful links

* For example demonstrations of the usage, go to [Ballerina By Examples](https://ballerina.io/learn/by-example/).
* Chat live with us via our [Discord server](https://discord.gg/ballerinalang).
* Post all technical questions on Stack Overflow with the [#ballerina](https://stackoverflow.com/questions/tagged/ballerina) tag.
