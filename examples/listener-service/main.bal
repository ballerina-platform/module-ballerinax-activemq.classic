import ballerina/log;
import ballerinax/activemq.classic;

configurable string brokerUrl = "tcp://localhost:61616";
configurable string username = "admin";
configurable string password = ?;

const string ORDERS_QUEUE = "examples.orders.queue";

listener classic:Listener mqListener = check new (brokerUrl, username = username, password = password);

@classic:ServiceConfig {
    queueName: ORDERS_QUEUE
}
service classic:Service on mqListener {
    remote function onMessage(record {|*classic:Message; string payload;|} message) returns error? {
        log:printInfo("Processing order", payload = message.payload);
    }
}
