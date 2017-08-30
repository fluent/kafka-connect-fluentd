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

import org.komamitsu.fluency.EventTime;

import java.util.Map;

public class FluentdEventRecord {
    private String tag;
    private EventTime eventTime;
    private Long timestamp;
    private Map<String, Object> data;

    public FluentdEventRecord(String tag, EventTime eventTime, Map<String, Object> data) {
        this.tag = tag;
        this.eventTime = eventTime;
        this.data = data;
    }

    public FluentdEventRecord(String tag, long timestamp, Map<String, Object> data) {
        this.tag = tag;
        this.timestamp = timestamp;
        this.data = data;
    }

    public FluentdEventRecord(String tag, Map<String, Object> data) {
        this.tag = tag;
        this.timestamp = System.currentTimeMillis() / 1000;
        this.data = data;
    }

    public String getTag() {
        return tag;
    }

    public EventTime getEventTime() {
        return eventTime;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public Map<String, Object> getData() {
        return data;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public String toString() {
        return "FluentdEventRecord{tag=" + getTag() + " eventTime=" + getEventTime() + " data=" + getData() + "}";
    }
}
