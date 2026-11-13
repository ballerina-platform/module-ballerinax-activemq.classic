import ballerina/io;
import ballerinax/activemq.classic;

configurable string brokerUrl = "tcp://localhost:61616";
configurable string username = "admin";
configurable string password = ?;

const string ORDERS_QUEUE = "examples.orders.queue";

public function main() returns error? {
    classic:MessageConsumer consumer = check new (brokerUrl,
        username = username,
        password = password,
        destination = {queueName: ORDERS_QUEUE}
    );

    do {
        int received = 0;
        while received < 3 {
            record {|*classic:Message; string payload;|}? msg = check consumer->receive(5000);
            if msg is () {
                io:println("No more messages within the timeout, stopping.");
                break;
            }
            io:println("Received order: ", msg.payload);
            received += 1;
        }
    } on fail error receiveErr {
        check consumer->close();
        return receiveErr;
    }

    check consumer->close();
}
