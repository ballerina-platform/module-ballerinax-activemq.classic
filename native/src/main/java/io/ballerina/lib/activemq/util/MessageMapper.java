/*
 * Copyright (c) 2025, WSO2 LLC. (http://www.wso2.com)
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.ballerina.lib.activemq.util;

import io.ballerina.runtime.api.creators.TypeCreator;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.types.ArrayType;
import io.ballerina.runtime.api.types.IntersectionType;
import io.ballerina.runtime.api.types.MapType;
import io.ballerina.runtime.api.types.PredefinedTypes;
import io.ballerina.runtime.api.types.RecordType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.types.TypeTags;
import io.ballerina.runtime.api.types.UnionType;
import io.ballerina.runtime.api.utils.JsonUtils;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.utils.TypeUtils;
import io.ballerina.runtime.api.utils.ValueUtils;
import io.ballerina.runtime.api.utils.XmlUtils;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.api.values.BTypedesc;
import jakarta.jms.BytesMessage;
import jakarta.jms.DeliveryMode;
import jakarta.jms.Destination;
import jakarta.jms.JMSException;
import jakarta.jms.MapMessage;
import jakarta.jms.Message;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.jms.TemporaryQueue;
import jakarta.jms.TemporaryTopic;
import jakarta.jms.TextMessage;
import jakarta.jms.Topic;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Enumeration;
import java.util.logging.Logger;

import static io.ballerina.lib.activemq.util.ActiveMQConstants.AMQ_SCHEDULED_CRON;
import static io.ballerina.lib.activemq.util.ActiveMQConstants.AMQ_SCHEDULED_DELAY;
import static io.ballerina.lib.activemq.util.ActiveMQConstants.AMQ_SCHEDULED_PERIOD;
import static io.ballerina.lib.activemq.util.ActiveMQConstants.AMQ_SCHEDULED_REPEAT;
import static io.ballerina.lib.activemq.util.ActiveMQConstants.CORRELATION_ID;
import static io.ballerina.lib.activemq.util.ActiveMQConstants.DELIVERY_TIME_FIELD;
import static io.ballerina.lib.activemq.util.ActiveMQConstants.DESTINATION_FIELD;
import static io.ballerina.lib.activemq.util.ActiveMQConstants.EXPIRY_FIELD;
import static io.ballerina.lib.activemq.util.ActiveMQConstants.FORMAT_FIELD;
import static io.ballerina.lib.activemq.util.ActiveMQConstants.MESSAGE_ID;
import static io.ballerina.lib.activemq.util.ActiveMQConstants.MESSAGE_PAYLOAD;
import static io.ballerina.lib.activemq.util.ActiveMQConstants.MESSAGE_PROPERTIES;
import static io.ballerina.lib.activemq.util.ActiveMQConstants.MESSAGE_USERID;
import static io.ballerina.lib.activemq.util.ActiveMQConstants.PERSISTENT_FIELD;
import static io.ballerina.lib.activemq.util.ActiveMQConstants.PRIORITY_FIELD;
import static io.ballerina.lib.activemq.util.ActiveMQConstants.QUEUE_NAME;
import static io.ballerina.lib.activemq.util.ActiveMQConstants.REDELIVERED_FIELD;
import static io.ballerina.lib.activemq.util.ActiveMQConstants.REPLY_TO;
import static io.ballerina.lib.activemq.util.ActiveMQConstants.SCHEDULED_CRON;
import static io.ballerina.lib.activemq.util.ActiveMQConstants.SCHEDULED_DELAY;
import static io.ballerina.lib.activemq.util.ActiveMQConstants.SCHEDULED_PERIOD;
import static io.ballerina.lib.activemq.util.ActiveMQConstants.SCHEDULED_REPEAT;
import static io.ballerina.lib.activemq.util.ActiveMQConstants.TEMPORARY;
import static io.ballerina.lib.activemq.util.ActiveMQConstants.TIMESTAMP_FIELD;
import static io.ballerina.lib.activemq.util.ActiveMQConstants.TOPIC_NAME;
import static io.ballerina.lib.activemq.util.ActiveMQConstants.TYPE_FIELD;
import static io.ballerina.lib.activemq.util.ModuleUtils.getModule;

/**
 * MessageMapper converts between JMS messages and Ballerina message records, in both directions.
 *
 * @since 0.1.0
 */
public class MessageMapper {
    private static final Logger LOGGER = Logger.getLogger(MessageMapper.class.getName());

    static final BString TEXT = StringUtils.fromString("text");
    static final BString BINARY = StringUtils.fromString("binary");
    static final BString UNKNOWN = StringUtils.fromString("unknown");
    public static final String NATIVE_MESSAGE = "native.message";
    public static final String NATIVE_DESTINATION = "native.destination";

    private static final UnionType PROPERTY_TYPE = TypeCreator.createUnionType(
            PredefinedTypes.TYPE_BOOLEAN, PredefinedTypes.TYPE_INT, PredefinedTypes.TYPE_BYTE,
            PredefinedTypes.TYPE_FLOAT, PredefinedTypes.TYPE_STRING,
            TypeCreator.createArrayType(PredefinedTypes.TYPE_BYTE));
    private static final MapType BALLERINA_PROPERTY_TYPE = TypeCreator.createMapType(
            "Property", PROPERTY_TYPE, getModule());

    /** Converts a JMS message to a Ballerina message record shaped by bTypedesc's payload field. */
    public static BMap<BString, Object> toBallerinaMessage(Message message, BTypedesc bTypedesc)
            throws JMSException {
        return toBallerinaMessage(message, resolveRecordType(bTypedesc.getDescribingType()));
    }

    /** Converts a JMS message to a message record of the given shape; shared by receive() and the listener. */
    public static BMap<BString, Object> toBallerinaMessage(Message message, RecordType recordType)
            throws JMSException {
        BMap<BString, Object> result = ValueCreator.createRecordValue(recordType);
        populateHeaders(result, message);

        Type payloadType = TypeUtils.getReferredType(recordType.getFields().get(MESSAGE_PAYLOAD.getValue())
                .getFieldType());
        result.put(MESSAGE_PAYLOAD, getPayloadWithIntendedType(message, payloadType));
        result.put(FORMAT_FIELD, resolveFormat(message));
        result.addNativeData(NATIVE_MESSAGE, message);
        return result;
    }

    /** Populates every Message field except {@code payload}/{@code format}, shared by both receive paths. */
    private static void populateHeaders(BMap<BString, Object> result, Message message) throws JMSException {
        result.put(MESSAGE_ID, StringUtils.fromString(message.getJMSMessageID()));

        long timestamp = message.getJMSTimestamp();
        if (timestamp > 0) {
            result.put(TIMESTAMP_FIELD, timestamp);
        }

        String correlationId = message.getJMSCorrelationID();
        if (correlationId != null) {
            result.put(CORRELATION_ID, StringUtils.fromString(correlationId));
        }

        if (message.getJMSReplyTo() != null) {
            result.put(REPLY_TO, toBallerinaDestination(message.getJMSReplyTo()));
        }

        if (message.getJMSDestination() != null) {
            result.put(DESTINATION_FIELD, toBallerinaDestination(message.getJMSDestination()));
        }

        result.put(PERSISTENT_FIELD, message.getJMSDeliveryMode() == 2);

        result.put(REDELIVERED_FIELD, message.getJMSRedelivered());

        String type = message.getJMSType();
        if (type != null) {
            result.put(TYPE_FIELD, StringUtils.fromString(type));
        }

        long expiry = message.getJMSExpiration();
        if (expiry > 0) {
            result.put(EXPIRY_FIELD, expiry);
        }

        long deliveryTime = message.getJMSDeliveryTime();
        if (deliveryTime > 0) {
            result.put(DELIVERY_TIME_FIELD, deliveryTime);
        }

        int priority = message.getJMSPriority();
        result.put(PRIORITY_FIELD, (long) priority);

        String userID = message.getStringProperty("JMSXUserID");
        if (userID != null) {
            result.put(MESSAGE_USERID, StringUtils.fromString(userID));
        }

        BMap<BString, Object> props = ValueCreator.createMapValue(BALLERINA_PROPERTY_TYPE);
        Enumeration<?> propNames = message.getPropertyNames();
        while (propNames.hasMoreElements()) {
            String name = (String) propNames.nextElement();
            Object value = message.getObjectProperty(name);
            BString bName = StringUtils.fromString(name);
            if (value instanceof String s) {
                props.put(bName, StringUtils.fromString(s));
            } else if (value instanceof Integer i) {
                props.put(bName, (long) i);
            } else if (value instanceof Long l) {
                props.put(bName, l);
            } else if (value instanceof Short sh) {
                props.put(bName, (long) sh);
            } else if (value instanceof Byte b) {
                // JMS byte is signed; Ballerina byte is unsigned.
                props.put(bName, b & 0xFF);
            } else if (value instanceof Float f) {
                props.put(bName, (double) f);
            } else if (value instanceof Double d) {
                props.put(bName, d);
            } else if (value instanceof Boolean b) {
                props.put(bName, b);
            } else if (value != null) {
                LOGGER.warning(() -> String.format(
                        "Property '%s' of unsupported type '%s' cannot be represented as a classic:Property "
                                + "(boolean, int, byte, float, string, or byte[]) - falling back to toString()",
                        name, value.getClass().getSimpleName()));
                props.put(bName, StringUtils.fromString(value.toString()));
            }
        }
        result.put(MESSAGE_PROPERTIES, props);
    }

    private static BString resolveFormat(Message message) {
        if (message instanceof TextMessage) {
            return TEXT;
        }
        if (message instanceof BytesMessage) {
            return BINARY;
        }
        return UNKNOWN;
    }

    private static Object getPayloadWithIntendedType(Message message, Type payloadType) throws JMSException {
        int typeTag = payloadType.getTag();
        try {
            if (message instanceof TextMessage textMessage) {
                return getPayloadFromTextMessage(textMessage, payloadType, typeTag);
            }
            if (message instanceof MapMessage mapMessage) {
                return getPayloadFromMapMessage(mapMessage, payloadType, typeTag);
            }
            if (message instanceof BytesMessage bytesMessage) {
                return getPayloadFromBytesMessage(bytesMessage, payloadType, typeTag);
            }
        } catch (BError bError) {
            throw new ActiveMQDatabindingException("Data binding failed: " + bError.getDetails());
        }
        if (typeTag == TypeTags.ANYDATA_TAG) {
            String body = message.getBody(String.class);
            byte[] fallback = (body != null ? body : "").getBytes(StandardCharsets.UTF_8);
            return ValueCreator.createArrayValue(fallback);
        }
        throw new ActiveMQDatabindingException(String.format(
                "Data binding failed: Unsupported message type '%s' for typed payload binding",
                message.getClass().getSimpleName()));
    }

    private static Object getPayloadFromTextMessage(TextMessage message, Type payloadType, int typeTag)
            throws JMSException {
        String rawText = message.getText();
        String text = rawText != null ? rawText : "";
        if (typeTag == TypeTags.ANYDATA_TAG) {
            return ValueCreator.createArrayValue(text.getBytes(StandardCharsets.UTF_8));
        }
        if (typeTag == TypeTags.STRING_TAG) {
            return StringUtils.fromString(text);
        }
        if (typeTag == TypeTags.XML_TAG) {
            return XmlUtils.parse(text);
        }
        throw new ActiveMQDatabindingException(
                String.format("Data binding failed: Cannot bind TextMessage to type '%s'. "
                        + "Expected 'string' or 'xml'", payloadType));
    }

    private static Object getPayloadFromMapMessage(MapMessage message, Type payloadType, int typeTag)
            throws JMSException {
        if (typeTag != TypeTags.ANYDATA_TAG && typeTag != TypeTags.MAP_TAG && typeTag != TypeTags.RECORD_TYPE_TAG) {
            throw new ActiveMQDatabindingException(
                    String.format("Data binding failed: Cannot bind MapMessage to type '%s'. "
                            + "Expected 'map<classic:Property>'", payloadType));
        }
        BMap<BString, Object> payload = ValueCreator.createMapValue(BALLERINA_PROPERTY_TYPE);
        Enumeration<?> mapNames = message.getMapNames();
        while (mapNames.hasMoreElements()) {
            String key = (String) mapNames.nextElement();
            Object value = message.getObject(key);
            BString bKey = StringUtils.fromString(key);
            if (value instanceof String s) {
                payload.put(bKey, StringUtils.fromString(s));
            } else if (value instanceof Integer i) {
                payload.put(bKey, (long) i);
            } else if (value instanceof Long l) {
                payload.put(bKey, l);
            } else if (value instanceof Short sh) {
                payload.put(bKey, (long) sh);
            } else if (value instanceof Byte b) {
                // JMS byte is signed; Ballerina byte is unsigned.
                payload.put(bKey, b & 0xFF);
            } else if (value instanceof Float f) {
                payload.put(bKey, (double) f);
            } else if (value instanceof Double d) {
                payload.put(bKey, d);
            } else if (value instanceof Boolean b) {
                payload.put(bKey, b);
            } else if (value instanceof byte[] bytes) {
                payload.put(bKey, ValueCreator.createArrayValue(bytes));
            } else if (value != null) {
                LOGGER.warning(() -> String.format(
                        "Dropped MapMessage entry '%s' of unsupported type '%s' - value cannot be represented as "
                                + "a classic:Property (boolean, int, byte, float, string, or byte[])",
                        key, value.getClass().getSimpleName()));
            }
        }
        if (typeTag == TypeTags.RECORD_TYPE_TAG) {
            return ValueUtils.convert(payload, payloadType);
        }
        return payload;
    }

    private static Object getPayloadFromBytesMessage(BytesMessage message, Type payloadType, int typeTag)
            throws JMSException {
        if (typeTag == TypeTags.STRING_TAG || typeTag == TypeTags.XML_TAG) {
            throw new ActiveMQDatabindingException(
                    String.format("Data binding failed: Cannot bind BytesMessage to type '%s'. "
                            + "Use TextMessage for string/xml payloads", payloadType));
        }
        if (typeTag == TypeTags.MAP_TAG || typeTag == TypeTags.RECORD_TYPE_TAG) {
            throw new ActiveMQDatabindingException(
                    String.format("Data binding failed: Cannot bind BytesMessage to type '%s'. "
                            + "Use MapMessage for map/record payloads", payloadType));
        }

        byte[] bytes = new byte[(int) message.getBodyLength()];
        message.readBytes(bytes);

        if (typeTag == TypeTags.ANYDATA_TAG) {
            return ValueCreator.createArrayValue(bytes);
        }
        if (typeTag == TypeTags.ARRAY_TAG
                && TypeUtils.getReferredType(((ArrayType) payloadType).getElementType()).getTag()
                        == TypeTags.BYTE_TAG) {
            return ValueCreator.createArrayValue(bytes);
        }

        String jsonString = new String(bytes, StandardCharsets.UTF_8);
        return ValueUtils.convert(JsonUtils.parse(jsonString), payloadType);
    }

    private static RecordType resolveRecordType(Type describingType) {
        if (describingType.isReadOnly()) {
            return (RecordType) TypeUtils.getReferredType(
                    ((IntersectionType) describingType).getConstituentTypes().get(0));
        }
        return (RecordType) describingType;
    }

    /** Converts a JMS destination to a Destination record, stashing the native object for reuse by toJmsDestination. */
    static BMap<BString, Object> toBallerinaDestination(Destination destination) throws JMSException {
        BMap<BString, Object> result;
        if (destination instanceof TemporaryQueue queue) {
            result = createQueue(queue.getQueueName(), true);
        } else if (destination instanceof Queue queue) {
            result = createQueue(queue.getQueueName(), false);
        } else if (destination instanceof TemporaryTopic topic) {
            result = createTopic(topic.getTopicName(), true);
        } else if (destination instanceof Topic topic) {
            result = createTopic(topic.getTopicName(), false);
        } else {
            throw new JMSException("Unsupported JMS destination type: " + destination.getClass().getName());
        }
        result.addNativeData(NATIVE_DESTINATION, destination);
        return result;
    }

    private static BMap<BString, Object> createQueue(String name, boolean temporary) {
        BMap<BString, Object> queue = ValueCreator.createRecordValue(getModule(), "Queue");
        queue.put(QUEUE_NAME, StringUtils.fromString(name));
        queue.put(TEMPORARY, temporary);
        return queue;
    }

    private static BMap<BString, Object> createTopic(String name, boolean temporary) {
        BMap<BString, Object> topic = ValueCreator.createRecordValue(getModule(), "Topic");
        topic.put(TOPIC_NAME, StringUtils.fromString(name));
        topic.put(TEMPORARY, temporary);
        return topic;
    }

    /** Creates a JMS Destination, reusing a stashed native destination if present, else building one by name. */
    public static Destination toJmsDestination(Session session, BMap<BString, Object> destination)
            throws JMSException {
        Object nativeDestination = destination.getNativeData(NATIVE_DESTINATION);
        if (nativeDestination instanceof Destination jmsDestination) {
            return jmsDestination;
        }
        boolean temporary = destination.get(TEMPORARY) instanceof Boolean b && b;
        Object topicName = destination.get(TOPIC_NAME);
        if (topicName instanceof BString topic) {
            return temporary ? session.createTemporaryTopic() : session.createTopic(topic.getValue());
        }
        Object queueName = destination.get(QUEUE_NAME);
        if (queueName instanceof BString queue) {
            return temporary ? session.createTemporaryQueue() : session.createQueue(queue.getValue());
        }
        throw new JMSException("Invalid destination: expected queueName or topicName");
    }

    /** Converts a Message record to a JMS message, mapping headers, properties, and scheduler properties. */
    @SuppressWarnings("unchecked")
    public static Message toJmsMessage(Session session, BMap<BString, Object> bMsg) throws JMSException {
        Message jmsMsg = createOutgoingMessage(session, bMsg.get(MESSAGE_PAYLOAD));

        Object corrId = bMsg.get(CORRELATION_ID);
        if (corrId instanceof BString bCorrId) {
            jmsMsg.setJMSCorrelationID(bCorrId.getValue());
        }

        Object replyTo = bMsg.get(REPLY_TO);
        if (replyTo instanceof BMap<?, ?> rawReplyTo) {
            BMap<BString, Object> bReplyTo = (BMap<BString, Object>) rawReplyTo;
            jmsMsg.setJMSReplyTo(toJmsDestination(session, bReplyTo));
        }

        Object type = bMsg.get(TYPE_FIELD);
        if (type instanceof BString bType) {
            jmsMsg.setJMSType(bType.getValue());
        }

        Object propsObj = bMsg.get(MESSAGE_PROPERTIES);
        if (propsObj instanceof BMap<?, ?> rawProps) {
            BMap<BString, Object> props = (BMap<BString, Object>) rawProps;
            for (BString key : props.getKeys()) {
                Object val = props.get(key);
                String propName = key.getValue();
                if (val instanceof BString bStr) {
                    jmsMsg.setStringProperty(propName, bStr.getValue());
                } else if (val instanceof Long l) {
                    jmsMsg.setLongProperty(propName, l);
                } else if (val instanceof Double d) {
                    jmsMsg.setDoubleProperty(propName, d);
                } else if (val instanceof Boolean b) {
                    jmsMsg.setBooleanProperty(propName, b);
                } else if (val instanceof Integer i) {
                    // Ballerina byte arrives boxed as Integer at this native boundary.
                    jmsMsg.setByteProperty(propName, i.byteValue());
                } else if (val != null) {
                    LOGGER.warning(() -> String.format(
                            "Dropped message property '%s' of unsupported type '%s' - JMS message properties "
                                    + "support only boolean, int, byte, float, and string values",
                            propName, val.getClass().getSimpleName()));
                }
            }
        }

        Object scheduledDelay = bMsg.get(SCHEDULED_DELAY);
        if (scheduledDelay instanceof Long l) {
            jmsMsg.setLongProperty(AMQ_SCHEDULED_DELAY, l);
        }
        Object scheduledPeriod = bMsg.get(SCHEDULED_PERIOD);
        if (scheduledPeriod instanceof Long l) {
            jmsMsg.setLongProperty(AMQ_SCHEDULED_PERIOD, l);
        }
        Object scheduledRepeat = bMsg.get(SCHEDULED_REPEAT);
        if (scheduledRepeat instanceof Long l) {
            jmsMsg.setIntProperty(AMQ_SCHEDULED_REPEAT, l.intValue());
        }
        Object scheduledCron = bMsg.get(SCHEDULED_CRON);
        if (scheduledCron instanceof BString bs) {
            jmsMsg.setStringProperty(AMQ_SCHEDULED_CRON, bs.getValue());
        }

        return jmsMsg;
    }

    /** Maps payload's runtime type: string/byte[]/map -> Text/Bytes/Map, other anydata -> JSON bytes. */
    @SuppressWarnings("unchecked")
    private static Message createOutgoingMessage(Session session, Object payload) throws JMSException {
        if (payload instanceof BString bString) {
            TextMessage textMessage = session.createTextMessage();
            textMessage.setText(bString.getValue());
            return textMessage;
        }
        if (payload instanceof BArray bArray
                && TypeUtils.getReferredType(bArray.getElementType()).getTag() == TypeTags.BYTE_TAG) {
            BytesMessage bytesMessage = session.createBytesMessage();
            bytesMessage.writeBytes(bArray.getBytes());
            return bytesMessage;
        }
        if (payload instanceof BMap<?, ?> rawMap) {
            MapMessage mapMessage = session.createMapMessage();
            BMap<BString, Object> bMap = (BMap<BString, Object>) rawMap;
            for (BString key : bMap.getKeys()) {
                setMapEntry(mapMessage, key.getValue(), bMap.get(key));
            }
            return mapMessage;
        }
        return createJsonBytesMessage(session, payload);
    }

    private static Message createJsonBytesMessage(Session session, Object payload) throws JMSException {
        ByteArrayOutputStream jsonOut = new ByteArrayOutputStream();
        try {
            JsonUtils.serialize(JsonUtils.convertToJson(payload), jsonOut);
        } catch (BError e) {
            throw new JMSException("Unsupported payload type for sending: "
                    + (payload == null ? "null" : payload.getClass().getSimpleName()));
        }
        BytesMessage bytesMessage = session.createBytesMessage();
        bytesMessage.writeBytes(jsonOut.toByteArray());
        return bytesMessage;
    }

    private static void setMapEntry(MapMessage message, String name, Object value) throws JMSException {
        if (value instanceof BString bStr) {
            message.setString(name, bStr.getValue());
        } else if (value instanceof Long l) {
            message.setLong(name, l);
        } else if (value instanceof Double d) {
            message.setDouble(name, d);
        } else if (value instanceof Boolean b) {
            message.setBoolean(name, b);
        } else if (value instanceof Integer i) {
            // Ballerina byte arrives boxed as Integer at this native boundary.
            message.setByte(name, i.byteValue());
        } else if (value instanceof BArray bArray) {
            message.setBytes(name, bArray.getBytes());
        } else if (value != null) {
            LOGGER.warning(() -> String.format(
                    "Dropped map payload entry '%s' of unsupported type '%s' when sending a MapMessage",
                    name, value.getClass().getSimpleName()));
        }
    }

    public static int getDeliveryMode(BMap<BString, Object> bMsg) {
        Object persistent = bMsg.get(PERSISTENT_FIELD);
        if (persistent instanceof Boolean b) {
            return b ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT;
        }
        return Message.DEFAULT_DELIVERY_MODE;
    }

    public static int getPriority(BMap<BString, Object> bMsg) {
        if (bMsg.containsKey(PRIORITY_FIELD)) {
            Object priority = bMsg.get(PRIORITY_FIELD);
            if (priority instanceof Long l) {
                return l.intValue();
            }
        }
        return Message.DEFAULT_PRIORITY;
    }

    public static long getTTL(BMap<BString, Object> bMsg) {
        if (bMsg.containsKey(EXPIRY_FIELD)) {
            Object expiry = bMsg.get(EXPIRY_FIELD);
            if (expiry instanceof Long l) {
                if (l == 0L) {
                    // JMS defines a TTL of zero as no expiry.
                    return Message.DEFAULT_TIME_TO_LIVE;
                }
                long ttl = l - System.currentTimeMillis();
                // Using zero for a past expiry would disable expiry.
                return ttl <= 0 ? 1L : ttl;
            }
        }
        return Message.DEFAULT_TIME_TO_LIVE;
    }
}
