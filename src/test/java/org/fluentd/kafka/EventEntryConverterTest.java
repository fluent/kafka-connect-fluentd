package org.fluentd.kafka;

import influent.EventEntry;
import org.apache.kafka.connect.data.Struct;
import org.junit.Before;
import org.junit.Test;
import org.msgpack.value.ImmutableMapValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

public class EventEntryConverterTest {
    private static EventEntryConverter converter;

    @Before
    public void setUp() {
        converter = new EventEntryConverter();
    }

    @Test
    public void simple() {
        Map<Value, Value> map = new HashMap<>();
        map.put(ValueFactory.newString("key"), ValueFactory.newString("value"));
        ImmutableMapValue record = ValueFactory.newMap(map);

        EventEntry entry = EventEntry.of(Instant.now(), record);
        Struct struct = converter.toStruct(entry);
        System.out.println(struct);
        assertEquals("value", struct.getString("key"));
    }

    @Test
    public void mixed() {
        Map<Value, Value> map = new HashMap<>();
        map.put(ValueFactory.newString("key"), ValueFactory.newString("value"));
        map.put(ValueFactory.newString("number"), ValueFactory.newInteger(123));
        ImmutableMapValue record = ValueFactory.newMap(map);

        EventEntry entry = EventEntry.of(Instant.now(), record);
        Struct struct = converter.toStruct(entry);
        System.out.println(struct);
        assertEquals("value", struct.getString("key"));
        assertEquals(java.util.Optional.of(123), java.util.Optional.of(struct.getInt32("number")));
    }

    @Test
    public void nestedMap() {
        Map<Value, Value> map = new HashMap<>();
        Map<Value, Value> childMap = new HashMap<>();
        childMap.put(ValueFactory.newString("childKey"), ValueFactory.newString("childValue"));
        map.put(ValueFactory.newString("key"), ValueFactory.newMap(childMap));
        ImmutableMapValue record = ValueFactory.newMap(map);

        EventEntry entry = EventEntry.of(Instant.now(), record);
        Struct struct = converter.toStruct(entry);
        Struct childStruct = struct.getStruct("key");
        assertEquals("childValue", childStruct.getString("childKey"));
    }

    @Test
    public void nestedArray() {
        Map<Value, Value> map = new HashMap<>();
        List<Value> childArray = new ArrayList<>();
        childArray.add(ValueFactory.newString("childValue1"));
        childArray.add(ValueFactory.newString("childValue2"));
        map.put(ValueFactory.newString("key"), ValueFactory.newArray(childArray));
        ImmutableMapValue record = ValueFactory.newMap(map);

        EventEntry entry = EventEntry.of(Instant.now(), record);
        Struct struct = converter.toStruct(entry);
        List<String> array = struct.getArray("key");
        assertThat(array, contains("childValue1", "childValue2"));
    }

}