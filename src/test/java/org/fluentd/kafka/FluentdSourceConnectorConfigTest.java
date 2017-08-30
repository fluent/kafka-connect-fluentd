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

import org.junit.Test;
import static org.junit.Assert.*;

import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;

public class FluentdSourceConnectorConfigTest {
    @Test
    public void doc() {
        System.out.println(FluentdSourceConnectorConfig.conf().toRst());
    }

    @Test
    public void defaultValues() {
        Map<String, String> parsedConfig = new HashMap<>();
        FluentdSourceConnectorConfig conf = new FluentdSourceConnectorConfig(parsedConfig);
        assertEquals(24224, conf.getFluentdPort());
        assertEquals("0.0.0.0", conf.getFluentdBind());
    }

    @Test
    public void configValues() {
        Map<String, String> parsedConfig = new HashMap<>();
        parsedConfig.put(FluentdSourceConnectorConfig.FLUENTD_PORT, "24225");
        parsedConfig.put(FluentdSourceConnectorConfig.FLUENTD_BIND, "127.0.0.1");
        FluentdSourceConnectorConfig conf = new FluentdSourceConnectorConfig(parsedConfig);
        assertEquals(24225, conf.getFluentdPort());
        assertEquals("127.0.0.1", conf.getFluentdBind());
    }
}