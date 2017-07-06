package org.fluentd.kafka;

import influent.EventEntry;
import org.apache.kafka.connect.data.*;
import org.msgpack.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import javax.xml.validation.SchemaFactory;
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
            record.put(k, toValue((value)));
        });
        Schema schema = builder.build();
        Struct struct = new Struct(schema);
        record.forEach((key, value) -> {
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
                throw new NotImplementedException();
            case MAP:
                throw new NotImplementedException();
            default:
                throw new NotImplementedException();
        }
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
                throw new NotImplementedException();
            case MAP:
                throw new NotImplementedException();
            default:
                throw new NotImplementedException();
        }
    }
}
