package org.fluentd.kafka;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import influent.forward.ForwardCallback;
import influent.forward.ForwardServer;
import influent.EventEntry;

public class FluentdSourceConnector extends SourceConnector {
    private static Logger log = LoggerFactory.getLogger(FluentdSourceConnector.class);
    private Map<String, String> properties;

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> properties) {
        this.properties = properties;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return FluentdSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int taskMax) {
        //TODO: Define the individual task configurations that will be executed.
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        taskConfigs.add(this.properties);
        return taskConfigs;
    }

    @Override
    public void stop() {
        //TODO: Do things that are necessary to stop your connector.
    }

    @Override
    public ConfigDef config() {
        return FluentdSourceConnectorConfig.conf();
    }
}
