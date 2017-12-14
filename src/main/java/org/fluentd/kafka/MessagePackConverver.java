package org.fluentd.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import influent.EventEntry;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.msgpack.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class MessagePackConverver {
    static final Logger log = LoggerFactory.getLogger(MessagePackConverver.class);
    private final FluentdSourceConnectorConfig config;

    public MessagePackConverver(final FluentdSourceConnectorConfig config) {
        this.config = config;
    }

    public SourceRecord convert(String topic, String tag, Long timestamp, EventEntry entry) {
        if (config.isFluentdSchemasEnable()) {
            SchemaAndValue schemaAndValue = convert(topic, entry);
            return new SourceRecord(
                    null,
                    null,
                    topic,
                    null,
                    Schema.STRING_SCHEMA,
                    tag,
                    schemaAndValue.schema(),
                    schemaAndValue.value(),
                    timestamp
            );
        } else {
            Object record;
            try {
                record = new ObjectMapper().readValue(entry.getRecord().toJson(), LinkedHashMap.class);
            } catch (IOException e) {
                record = entry.getRecord().toJson();
            }
            return new SourceRecord(
                    null,
                    null,
                    topic,
                    null,
                    null,
                    null,
                    null,
                    record,
                    timestamp
            );
        }
    }

    public SourceRecord convert(String topic, String tag, Instant timestamp, EventEntry entry) {
        return convert(topic, tag, timestamp.toEpochMilli(), entry);
    }

    private SchemaAndValue convert(String topic, EventEntry entry) {
        return convert(topic, entry.getRecord());
    }

    private SchemaAndValue convert(String name, Value value) {
        switch (value.getValueType()) {
            case STRING:
                return new SchemaAndValue(Schema.STRING_SCHEMA, value.asStringValue().asString());
            case NIL:
                return new SchemaAndValue(Schema.OPTIONAL_STRING_SCHEMA, null);
            case BOOLEAN:
                return new SchemaAndValue(Schema.BOOLEAN_SCHEMA, value.asBooleanValue().getBoolean());
            case INTEGER:
                return new SchemaAndValue(Schema.INT64_SCHEMA, value.asIntegerValue().toLong());
            case FLOAT:
                // We cannot identify float32 and float64. We must treat float64 as double.
                return new SchemaAndValue(Schema.FLOAT64_SCHEMA, value.asFloatValue().toDouble());
            case BINARY:
                return new SchemaAndValue(Schema.BYTES_SCHEMA, value.asBinaryValue().asByteArray());
            case MAP: {
                SchemaBuilder builder = SchemaBuilder.struct().name(name);
                Map<Value, Value> map = value.asMapValue().map();
                Map<String, SchemaAndValue> fields = new HashMap<>();
                map.forEach((k, v) -> {
                    String n = k.asStringValue().asString();
                    fields.put(n, convert(n, v));
                });
                fields.forEach((k, v) -> {
                    builder.field(k, v.schema());
                });
                Schema schema = builder.build();
                Struct struct = new Struct(schema);
                fields.forEach((k, v) -> {
                    struct.put(k, v.value());
                });
                return new SchemaAndValue(schema, struct);
            }
            case ARRAY: {
                List<Value> array = value.asArrayValue().list();
                SchemaAndValue sv = convert(name, array.get(0));
                ArrayList<Object> values = new ArrayList<>();
                SchemaBuilder.type(sv.schema().type());
                array.forEach(val -> values.add(convert(null, val).value()));
                Schema schema = SchemaBuilder.array(sv.schema()).optional().build();
                return new SchemaAndValue(schema, values);
            }
            default:
                return null;
        }
    }
}
