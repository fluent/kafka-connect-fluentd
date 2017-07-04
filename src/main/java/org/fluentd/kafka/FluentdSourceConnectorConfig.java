package org.fluentd.kafka;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;

import java.util.Map;


public class FluentdSourceConnectorConfig extends AbstractConfig {

    public static final String FLUENTD_PORT = "fluentd.port";
    public static final String FLUENTD_BIND = "fluentd.bind";
    // in_forward configs
    //   linger_timeout
    //   check_size_limit
    //   chunk_size_warn_limit
    //   skip_invalid_event
    //   source_hostname_key
    //   log_level

    public FluentdSourceConnectorConfig(ConfigDef config, Map<String, String> parsedConfig) {
        super(config, parsedConfig);
    }

    public FluentdSourceConnectorConfig(Map<String, String> parsedConfig) {
        this(conf(), parsedConfig);
    }

    public static ConfigDef conf() {
        return new ConfigDef()
            .define(FLUENTD_PORT, Type.STRING, Importance.HIGH, "")
            .define(FLUENTD_BIND, Type.STRING, Importance.HIGH, "");
    }

    public String getFluentdPort() {
        return this.getString(FLUENTD_PORT);
    }

    public String getFluentdBind() {
        return this.getString(FLUENTD_BIND);
    }
}
