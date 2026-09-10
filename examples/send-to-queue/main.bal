import ballerina/log;
import ballerinax/activemq.classic;

configurable string brokerUrl = "tcp://localhost:61616";
configurable string username = "admin";
configurable string password = ?;

const string ORDERS_QUEUE = "examples.orders.queue";

type Order record {|
    string orderId;
    string item;
    int quantity;
|};

public function main() returns error? {
    classic:MessageProducer producer = check new (brokerUrl, username = username, password = password);

    Order[] orders = [
        {orderId: "ORD-1001", item: "Wireless Mouse", quantity: 2},
        {orderId: "ORD-1002", item: "Mechanical Keyboard", quantity: 1},
        {orderId: "ORD-1003", item: "USB-C Hub", quantity: 3}
    ];

    do {
        foreach Order 'order in orders {
            check producer->send({
                payload: 'order.toJsonString()
            }, {queueName: ORDERS_QUEUE});
            log:printInfo("Order placed", orderId = 'order.orderId, item = 'order.item);
        }
    } on fail error sendErr {
        check producer->close();
        return sendErr;
    }

    check producer->close();
}
