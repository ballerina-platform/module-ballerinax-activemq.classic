# Listener Service

Subscribes to an ActiveMQ queue with a declarative `classic:Listener`/`classic:Service` pair,
processing each message as it's pushed to the service instead of pulling for it.

## Prerequisites

Start a local ActiveMQ broker (see [examples/README.md](../README.md)), then run this example
first (it keeps running, waiting for messages) and, in a second terminal, run
[Send To Queue](../send-to-queue/Send%20To%20Queue.md):

```bash
bal run
```

## What it does

* Declares a `listener classic:Listener` bound to the broker.
* Attaches a `classic:Service` configured for the `examples.orders.queue` queue via
  `@classic:ServiceConfig`.
* Its `onMessage` remote method narrows the payload to `string` and logs each order as it arrives.

Expected output (once [Send To Queue](../send-to-queue/Send%20To%20Queue.md) runs in another
terminal):

```text
time=... level=INFO ... msg="Processing order" payload="{\"orderId\":\"ORD-1001\",...}"
time=... level=INFO ... msg="Processing order" payload="{\"orderId\":\"ORD-1002\",...}"
time=... level=INFO ... msg="Processing order" payload="{\"orderId\":\"ORD-1003\",...}"
```

Stop the example with `Ctrl+C` once done — it runs until interrupted, since a listener service
keeps the program alive to keep receiving messages.
