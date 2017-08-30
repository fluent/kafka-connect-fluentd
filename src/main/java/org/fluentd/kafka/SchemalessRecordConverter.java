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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.util.LinkedHashMap;
import java.util.Map;

public class SchemalessRecordConverter implements RecordConverter {
    @Override
    public FluentdEventRecord convert(Schema schema, Object value) {
        if (value instanceof Map) {
            return convertMap((Map<?, ?>)value);
        }
        // TODO support other types

        return null;
    }

    private FluentdEventRecord convertMap(Map<?, ?>map) {
        Map<String, Object> record = new LinkedHashMap<>();
        map.forEach((key, val) -> {
            record.put(key.toString(), val);
        });
        return new FluentdEventRecord(null, record);
    }
}
