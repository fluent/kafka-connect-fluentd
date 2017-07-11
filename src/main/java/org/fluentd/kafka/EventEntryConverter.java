package org.fluentd.kafka;

import influent.EventEntry;
import org.apache.kafka.connect.data.*;
import org.msgpack.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EventEntryConverter {
    static final Logger log = LoggerFactory.getLogger(EventEntryConverter.class);
    public EventEntryConverter() {

    }

    Struct toStruct(EventEntry entry) {
        SchemaBuilder builder = SchemaBuilder.struct();
        Map<Value, Value> map = entry.getRecord().map();
        Map<String, Object> record = new HashMap<>();
        map.forEach((key, value) -> {
            if (!key.isStringValue()) {
                log.error("type error");
                return;
            }
            String k = key.asStringValue().asString();
            builder.field(k, toType(value));
            record.put(k, toValue(value));
        });
        Schema schema = builder.build();
        Struct struct = new Struct(schema);
        record.forEach((key, value) -> {
            System.out.println(value.getClass());
            struct.put(key, value);
        });
        return struct;
    }

    private Schema toType(Value value) {
        switch (value.getValueType()) {
            case NIL:
            case STRING:
                return Schema.OPTIONAL_STRING_SCHEMA;
            case FLOAT:
                return Schema.OPTIONAL_FLOAT32_SCHEMA;
            case INTEGER:
                return Schema.OPTIONAL_INT32_SCHEMA;
            case BOOLEAN:
                return Schema.OPTIONAL_BOOLEAN_SCHEMA;
            case ARRAY:
                return buildSchema(value);
            case MAP:
                return buildSchema(value);
            default:
                throw new UnsupportedOperationException();
        }
    }

    private Schema buildSchema(Value value) {
        SchemaBuilder builder;
        switch (value.getValueType()) {
            case MAP:
                builder = SchemaBuilder.struct();
                value.asMapValue().map().forEach((key, val) -> builder.field(key.asStringValue().asString(), toType(val)));
                break;
            case ARRAY:
                // Array cannot include mixed types such as [String, Integer, ...]
                Value firstValue = value.asArrayValue().get(0);
                builder = SchemaBuilder.array(toType(firstValue));
                break;
            default:
                throw new UnsupportedOperationException();
        }
        return builder.build();
    }

    private Object toValue(Value value) {
        switch (value.getValueType()) {
            case NIL:
            case STRING:
                return value.asStringValue().asString();
            case FLOAT:
                return value.asFloatValue().toFloat();
            case INTEGER:
                return value.asIntegerValue().toInt();
            case BOOLEAN:
                return value.asBooleanValue().asBooleanValue();
            case ARRAY:
                return buildValue(value);
            case MAP:
                return buildValue(value);
            default:
                throw new UnsupportedOperationException();
        }
    }

    private Object buildValue(Value value) {
        switch (value.getValueType()) {
            case ARRAY:
                // array cannot include mixed types such as [String, Integer, ...]
                List<Object> array = new ArrayList<>();
                value.asArrayValue().forEach(val -> array.add(toValue(val)));
                return array;
            case MAP:
                Schema schema = buildSchema(value);
                Struct struct = new Struct(schema);
                value.asMapValue().map().forEach((key, val) -> struct.put(key.asStringValue().asString(), toValue(val)));
                return struct;
            default:
                throw new UnsupportedOperationException();
        }
    }
}
