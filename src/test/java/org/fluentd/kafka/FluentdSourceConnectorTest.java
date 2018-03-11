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

import org.apache.kafka.connect.connector.ConnectorContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.easymock.PowerMock;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FluentdSourceConnectorTest {
    private FluentdSourceConnector connector;
    private ConnectorContext context;

    @Before
    public void setup() {
        connector = new FluentdSourceConnector();
        context = PowerMock.createMock(ConnectorContext.class);
        connector.initialize(context);
    }

    @Test
    public void testSingleTask() {
        PowerMock.replayAll();
        connector.start(buildSourceProperties());
        List<Map<String, String>> taskConfigs = connector.taskConfigs(1);
        Assert.assertEquals(1, taskConfigs.size());
        Assert.assertEquals("24225", taskConfigs.get(0).get(FluentdSourceConnectorConfig.FLUENTD_PORT));
        Assert.assertEquals("127.0.0.1", taskConfigs.get(0).get(FluentdSourceConnectorConfig.FLUENTD_BIND));
        Assert.assertEquals("100", taskConfigs.get(0).get(FluentdSourceConnectorConfig.FLUENTD_CHUNK_SIZE_LIMIT));
        Assert.assertEquals("200", taskConfigs.get(0).get(FluentdSourceConnectorConfig.FLUENTD_BACKLOG));
        Assert.assertEquals("300", taskConfigs.get(0).get(FluentdSourceConnectorConfig.FLUENTD_SEND_BUFFER_BYTES));
        Assert.assertEquals("400", taskConfigs.get(0).get(FluentdSourceConnectorConfig.FLUENTD_RECEVE_BUFFER_BYTES));
        Assert.assertEquals("false", taskConfigs.get(0).get(FluentdSourceConnectorConfig.FLUENTD_KEEP_ALIVE_ENABLED));
        Assert.assertEquals("false", taskConfigs.get(0).get(FluentdSourceConnectorConfig.FLUENTD_TCP_NO_DELAY_ENABLED));
        Assert.assertEquals("2", taskConfigs.get(0).get(FluentdSourceConnectorConfig.FLUENTD_WORKER_POOL_SIZE));
        Assert.assertEquals("tls", taskConfigs.get(0).get(FluentdSourceConnectorConfig.FLUENTD_TRANSPORT));
        Assert.assertEquals("TLSv1.1,TLSv1.2", taskConfigs.get(0).get(FluentdSourceConnectorConfig.FLUENTD_TLS_VERSIONS));
        Assert.assertEquals("AES,DES,RSA", taskConfigs.get(0).get(FluentdSourceConnectorConfig.FLUENTD_TLS_CIPHERS));
        Assert.assertEquals("/tmp/keystore.jks", taskConfigs.get(0).get(FluentdSourceConnectorConfig.FLUENTD_KEYSTORE_PATH));
        Assert.assertEquals("password1", taskConfigs.get(0).get(FluentdSourceConnectorConfig.FLUENTD_KEYSTORE_PASSWORD));
        Assert.assertEquals("password2", taskConfigs.get(0).get(FluentdSourceConnectorConfig.FLUENTD_KEY_PASSWORD));
        Assert.assertEquals("topic", taskConfigs.get(0).get(FluentdSourceConnectorConfig.KAFKA_TOPIC));
        PowerMock.verifyAll();
    }

    @Test
    public void testTaskConfigs() {
        PowerMock.replayAll();
        connector.start(buildSourceProperties());
        List<Map<String, String>> taskConfigs = connector.taskConfigs(10);
        Assert.assertEquals(10, taskConfigs.size());
        Assert.assertEquals("24225", taskConfigs.get(0).get(FluentdSourceConnectorConfig.FLUENTD_PORT));
        Assert.assertEquals("127.0.0.1", taskConfigs.get(0).get(FluentdSourceConnectorConfig.FLUENTD_BIND));
        Assert.assertEquals("100", taskConfigs.get(0).get(FluentdSourceConnectorConfig.FLUENTD_CHUNK_SIZE_LIMIT));
        Assert.assertEquals("200", taskConfigs.get(0).get(FluentdSourceConnectorConfig.FLUENTD_BACKLOG));
        Assert.assertEquals("300", taskConfigs.get(0).get(FluentdSourceConnectorConfig.FLUENTD_SEND_BUFFER_BYTES));
        Assert.assertEquals("400", taskConfigs.get(0).get(FluentdSourceConnectorConfig.FLUENTD_RECEVE_BUFFER_BYTES));
        Assert.assertEquals("false", taskConfigs.get(0).get(FluentdSourceConnectorConfig.FLUENTD_KEEP_ALIVE_ENABLED));
        Assert.assertEquals("false", taskConfigs.get(0).get(FluentdSourceConnectorConfig.FLUENTD_TCP_NO_DELAY_ENABLED));
        Assert.assertEquals("2", taskConfigs.get(0).get(FluentdSourceConnectorConfig.FLUENTD_WORKER_POOL_SIZE));
        Assert.assertEquals("tls", taskConfigs.get(0).get(FluentdSourceConnectorConfig.FLUENTD_TRANSPORT));
        Assert.assertEquals("TLSv1.1,TLSv1.2", taskConfigs.get(0).get(FluentdSourceConnectorConfig.FLUENTD_TLS_VERSIONS));
        Assert.assertEquals("AES,DES,RSA", taskConfigs.get(0).get(FluentdSourceConnectorConfig.FLUENTD_TLS_CIPHERS));
        Assert.assertEquals("/tmp/keystore.jks", taskConfigs.get(0).get(FluentdSourceConnectorConfig.FLUENTD_KEYSTORE_PATH));
        Assert.assertEquals("password1", taskConfigs.get(0).get(FluentdSourceConnectorConfig.FLUENTD_KEYSTORE_PASSWORD));
        Assert.assertEquals("password2", taskConfigs.get(0).get(FluentdSourceConnectorConfig.FLUENTD_KEY_PASSWORD));
        Assert.assertEquals("topic", taskConfigs.get(0).get(FluentdSourceConnectorConfig.KAFKA_TOPIC));
        PowerMock.verifyAll();
    }

    private Map<String, String> buildSourceProperties() {
        final Map<String, String> sourceProperties = new HashMap<>();
        sourceProperties.put(FluentdSourceConnectorConfig.FLUENTD_PORT, "24225");
        sourceProperties.put(FluentdSourceConnectorConfig.FLUENTD_BIND, "127.0.0.1");
        sourceProperties.put(FluentdSourceConnectorConfig.FLUENTD_CHUNK_SIZE_LIMIT, "100");
        sourceProperties.put(FluentdSourceConnectorConfig.FLUENTD_BACKLOG, "200");
        sourceProperties.put(FluentdSourceConnectorConfig.FLUENTD_SEND_BUFFER_BYTES, "300");
        sourceProperties.put(FluentdSourceConnectorConfig.FLUENTD_RECEVE_BUFFER_BYTES, "400");
        sourceProperties.put(FluentdSourceConnectorConfig.FLUENTD_KEEP_ALIVE_ENABLED, "false");
        sourceProperties.put(FluentdSourceConnectorConfig.FLUENTD_TCP_NO_DELAY_ENABLED, "false");
        sourceProperties.put(FluentdSourceConnectorConfig.FLUENTD_WORKER_POOL_SIZE, "2");
        sourceProperties.put(FluentdSourceConnectorConfig.FLUENTD_TRANSPORT, "tls");
        sourceProperties.put(FluentdSourceConnectorConfig.FLUENTD_TLS_VERSIONS, "TLSv1.1,TLSv1.2");
        sourceProperties.put(FluentdSourceConnectorConfig.FLUENTD_TLS_CIPHERS, "AES,DES,RSA");
        sourceProperties.put(FluentdSourceConnectorConfig.FLUENTD_KEYSTORE_PATH, "/tmp/keystore.jks");
        sourceProperties.put(FluentdSourceConnectorConfig.FLUENTD_KEYSTORE_PASSWORD, "password1");
        sourceProperties.put(FluentdSourceConnectorConfig.FLUENTD_KEY_PASSWORD, "password2");
        sourceProperties.put(FluentdSourceConnectorConfig.KAFKA_TOPIC, "topic");
        return sourceProperties;
    }
}
