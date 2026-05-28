# module-ballerinax-activemq

[![Build](https://github.com/ballerina-platform/module-ballerinax-activemq/actions/workflows/ci.yml/badge.svg)](https://github.com/ballerina-platform/module-ballerinax-activemq/actions/workflows/ci.yml)
[![Trivy](https://github.com/ballerina-platform/module-ballerinax-activemq/actions/workflows/trivy-scan.yml/badge.svg)](https://github.com/ballerina-platform/module-ballerinax-activemq/actions/workflows/trivy-scan.yml)
[![GraalVM Check](https://github.com/ballerina-platform/module-ballerinax-activemq/actions/workflows/build-with-bal-test-graalvm.yml/badge.svg)](https://github.com/ballerina-platform/module-ballerinax-activemq/actions/workflows/build-with-bal-test-graalvm.yml)
[![GitHub Last Commit](https://img.shields.io/github/last-commit/ballerina-platform/module-ballerinax-activemq.svg)](https://github.com/ballerina-platform/module-ballerinax-activemq/commits/main)
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
name = "activemq"
version = "0.1.0"
```

### 3. Send a message to a queue

```ballerina
import ballerinax/activemq;

public function main() returns error? {
    activemq:Client mqClient = check new ("tcp://localhost:61616",
        username = "admin",
        password = "admin"
    );

    check mqClient->sendMessage("orders.queue", {
        messageId: "order-001",
        payload: "{'item':'book','qty':2}".toBytes(),
        properties: {"region": "APAC"}
    });

    check mqClient->close();
}
```

### 4. Receive a message from a queue

```ballerina
import ballerinax/activemq;
import ballerina/io;

public function main() returns error? {
    activemq:Client mqClient = check new ("tcp://localhost:61616",
        username = "admin",
        password = "admin"
    );

    activemq:Message? msg = check mqClient->receiveMessage("orders.queue", 5000);
    if msg is activemq:Message {
        string text = check string:fromBytes(msg.payload);
        io:println("Received: ", text);
    }

    check mqClient->close();
}
```

### 5. Subscribe with a Listener service

The `Listener` polls a queue (or topic) and delivers each message to the `onMessage`
remote method:

```ballerina
import ballerinax/activemq;
import ballerina/log;

listener activemq:Listener mqListener = check new ("tcp://localhost:61616",
    username = "admin",
    password = "admin"
);

@activemq:ServiceConfig {
    queueName: "orders.queue",
    pollingInterval: 2,     // seconds between polls
    receiveTimeout: 5       // seconds to wait for a message per poll
}
service activemq:Service on mqListener {
    remote function onMessage(activemq:Message message) returns error? {
        string text = check string:fromBytes(message.payload);
        log:printInfo("Processing order: " + text);
    }
}
```

Use `topicName` instead of `queueName` to subscribe to a JMS topic.

### 6. Publish to a topic

Prefix the destination with `topic://` when calling `sendMessage`:

```ballerina
check mqClient->sendMessage("topic://order.events", {
    messageId: "evt-001",
    payload: "order placed".toBytes()
});
```

### 7. Transactional sends

Group multiple sends into a single atomic operation:

```ballerina
activemq:Transaction tx = check mqClient->'transaction();
check tx->sendMessage("orders.queue",  {messageId: "tx-1", payload: "order A".toBytes()});
check tx->sendMessage("audit.queue",   {messageId: "tx-2", payload: "audit A".toBytes()});
check tx->'commit();   // both messages are delivered together
check tx->close();
```

Call `'rollback()` (or simply `close()` without committing) to discard all buffered
messages.

### 8. Request-reply pattern

`sendRequest` creates a temporary reply queue, attaches it to the message as `replyTo`,
and blocks until the responder sends a reply:

```ballerina
activemq:Message? reply = check mqClient->sendRequest("pricing.service.queue", {
    messageId:     "req-001",
    correlationId: "corr-abc",
    payload:       "{'sku':'B007'}".toBytes()
}, 8000);

if reply is activemq:Message {
    io:println("Price: ", check string:fromBytes(reply.payload));
}
```

### 9. SSL / TLS connection

Pass a `secureSocket` block to connect over `ssl://`:

```ballerina
activemq:Client mqClient = check new ("ssl://localhost:61617",
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

### 10. Message selector

Receive only messages whose properties match a JMS selector expression:

```ballerina
// Only receive messages where the "region" property equals "APAC"
activemq:Message? msg = check mqClient->receiveMessage(
    "orders.queue", 5000, "region = 'APAC'"
);
```

The same `messageSelector` field is available in `@ServiceConfig` for listener services.

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

3. To build the without the tests:

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
