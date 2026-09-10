# Send To Queue

Sends a small batch of orders to an ActiveMQ queue using `classic:MessageProducer`. Shows the
most basic point-to-point send flow: connect, send a few messages, close.

## Prerequisites

Start a local ActiveMQ broker (see [examples/README.md](../README.md) for the shared
`docker-compose.yaml`), then run:

```bash
bal run
```

## What it does

* Connects to `tcp://localhost:61616` with `admin`/`admin` credentials.
* Sends three `Order` records, JSON-encoded, to the `examples.orders.queue` queue.
* Closes the producer once all messages are sent.

Expected output:

```text
time=... level=INFO ... msg="Order placed" orderId=ORD-1001 item="Wireless Mouse"
time=... level=INFO ... msg="Order placed" orderId=ORD-1002 item="Mechanical Keyboard"
time=... level=INFO ... msg="Order placed" orderId=ORD-1003 item="USB-C Hub"
```

Run [Receive From Queue](../receive-from-queue/Receive%20From%20Queue.md) afterwards to pull these
messages back off the same queue, or [Listener Service](../listener-service/Listener%20Service.md)
beforehand to have them processed as they arrive instead.
