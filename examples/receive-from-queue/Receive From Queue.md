# Receive From Queue

Pull-receives messages from an ActiveMQ queue using `classic:MessageConsumer`, narrowing the
payload to a `string` on the way out.

## Prerequisites

Start a local ActiveMQ broker (see [examples/README.md](../README.md)), and run
[Send To Queue](../send-to-queue/Send%20To%20Queue.md) first so there's something to receive:

```bash
bal run
```

## What it does

* Connects a `MessageConsumer` bound to the `examples.orders.queue` queue.
* Calls `receive(5000)` in a loop, narrowing each message's `payload` to `string` at the call site.
* Stops after 3 messages, or as soon as a `receive()` call times out with no message.

Expected output (after running [Send To Queue](../send-to-queue/Send%20To%20Queue.md)):

```text
Received order: {"orderId":"ORD-1001", "item":"Wireless Mouse", "quantity":2}
Received order: {"orderId":"ORD-1002", "item":"Mechanical Keyboard", "quantity":1}
Received order: {"orderId":"ORD-1003", "item":"USB-C Hub", "quantity":3}
```
