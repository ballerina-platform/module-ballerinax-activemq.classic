# Examples

The `ballerinax/activemq.classic` connector provides practical examples illustrating usage in various
scenarios. Explore these [examples](https://github.com/ballerina-platform/module-ballerinax-activemq.classic/tree/main/examples)
to understand how to produce, consume, and reliably process messages with an ActiveMQ broker.

1. [Send To Queue](send-to-queue/Send%20To%20Queue.md) - Send a small batch of orders to a queue
   with `classic:MessageProducer`. The most basic point-to-point send flow.

2. [Receive From Queue](receive-from-queue/Receive%20From%20Queue.md) - Pull-receive messages from
   a queue with `classic:MessageConsumer`, narrowing the payload to a `string` at the call site.

3. [Listener Service](listener-service/Listener%20Service.md) - Subscribe to a queue declaratively
   with `classic:Listener`/`classic:Service`, processing each message as it's pushed to the
   service instead of pulling for it.

## Prerequisites

All examples connect to a local ActiveMQ broker. Start one with Docker:

```bash
docker compose -f examples/docker-compose.yaml up -d
```

This starts a broker reachable at `tcp://localhost:61616` with credentials `admin`/`admin`, bound
to `127.0.0.1` only. ActiveMQ creates queues on first use, so no provisioning step is needed beyond
starting the broker.

## Running an Example

Each example is an independent Ballerina project. `brokerUrl` and `username` default to the values
above, but `password` doesn't ship with a default - copy `Config.toml.example` to `Config.toml` in
the example's directory first:

```bash
cp Config.toml.example Config.toml
bal run
```

[Send To Queue](send-to-queue/Send%20To%20Queue.md) and
[Receive From Queue](receive-from-queue/Receive%20From%20Queue.md) can be run in either order —
messages sent to a queue stay there until received. [Listener Service](listener-service/Listener%20Service.md)
should be started before [Send To Queue](send-to-queue/Send%20To%20Queue.md) so it's already
subscribed when the messages arrive.

## Running against local, unpublished changes

Each example's `Ballerina.toml` depends on the published `ballerinax/activemq.classic` package. To run an
example against local changes instead:

1. Build and push the module to your local Ballerina repository (see the main
   [README](../README.md#build-from-the-source)):

   ```bash
   cd ballerina && bal pack && bal push --repository=local
   ```

2. Add `repository = "local"` to the `[[dependency]]` entry in the example's `Ballerina.toml` -
   Ballerina only checks the local repository when a dependency explicitly opts into it:

   ```toml
   [[dependency]]
   org = "ballerinax"
   name = "activemq.classic"
   version = "0.1.0"
   repository = "local"
   ```

3. Run `bal run` from the example's directory as usual, then revert the `Ballerina.toml` change
   before committing.
