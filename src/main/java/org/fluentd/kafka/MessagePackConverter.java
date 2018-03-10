/**
 * Copyright 2017 ClearCode Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
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
import java.util.*;

public class MessagePackConverter {
    static final Logger log = LoggerFactory.getLogger(MessagePackConverter.class);
    private final FluentdSourceConnectorConfig config;

    public MessagePackConverter(final FluentdSourceConnectorConfig config) {
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
                if (config.getFluentdSchemasMapField() != null && config.getFluentdSchemasMapField().contains(name)) {
                    Map<Value, Value> map = value.asMapValue().map();
                    Map<String, Object> converted = new TreeMap<>();
                    Schema valueSchema = null;
                    for (Map.Entry<Value, Value> entry : map.entrySet()) {
                        Value k = entry.getKey();
                        Value v = entry.getValue();
                        String keyString = k.asStringValue().asString();
                        SchemaAndValue schemaAndValue = convert(keyString, v);
                        if (valueSchema == null) {
                            valueSchema = schemaAndValue.schema();
                        }
                        converted.put(keyString, schemaAndValue.value());
                    };
                    Schema schema = SchemaBuilder.map(Schema.STRING_SCHEMA, valueSchema).name(name).build();
                    return new SchemaAndValue(schema, converted);
                } else {
                    SchemaBuilder builder = SchemaBuilder.struct().name(name);
                    Map<Value, Value> map = value.asMapValue().map();
                    Map<String, SchemaAndValue> fields = new TreeMap<>();
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
                return SchemaAndValue.NULL;
        }
    }
}
