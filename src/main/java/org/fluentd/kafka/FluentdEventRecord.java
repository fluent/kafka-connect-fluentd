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
