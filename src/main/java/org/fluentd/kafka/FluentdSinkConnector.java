package org.fluentd.kafka;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FluentdSinkConnector extends SinkConnector {
    private static Logger log = LoggerFactory.getLogger(FluentdSinkConnector.class);
    private FluentdSinkConnectorConfig config;

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> map) {
        config = new FluentdSinkConnectorConfig(map);

        //TODO: Add things you need to do to setup your connector.

        /**
         * This will be executed once per connector. This can be used to handle connector level setup.
         */

    }

    @Override
    public Class<? extends Task> taskClass() {
        return FluentdSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        //TODO: Define the individual task configurations that will be executed.
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        Map<String, String> taskConfig = new HashMap<>();
        taskConfig.put(FluentdSinkConnectorConfig.FLUENTD_CONNECT, config.getFluentdConnect());
        taskConfigs.add(taskConfig);
        return taskConfigs;
    }

    @Override
    public void stop() {
        //TODO: Do things that are necessary to stop your connector.
    }

    @Override
    public ConfigDef config() {
        return FluentdSinkConnectorConfig.conf();
    }
}
