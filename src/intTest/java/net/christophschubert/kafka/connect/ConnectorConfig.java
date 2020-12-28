package net.christophschubert.kafka.connect;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.*;

public class ConnectorConfig {

    public static final String CONNECTOR_CLASS_CONFIG = "connector.class";
    public static final String TASKS_MAX_CONFIG = "tasks.max";
    public static final String TOPICS_CONFIG = "topics"; // sink connectors only
    public static final String TOPIC_REGEX_CONFIG= "topic.regex"; // sink connectors only
    public static final String KEY_CONVERTER_CONFIG = "key.converter";
    public static final String VALUE_CONVERTER_CONFIG = "value.converter";
    public static final String HEADER_CONVERTER_CONFIG = "header.converter";
    public static final String CONFIG_ACTION_RELOAD_CONFIG = "config.action.reload";
    public static final String TRANSFORMS_CONFIG = "transforms";
    public static final String PREDICATES_CONFIG = "predicates";
    public static final String ERRORS_RETRY_TIMEOUT_CONFIG = "errors.retry.timeout";
    public static final String ERRORS_RETRY_DELAY_MAX_MS_CONFIG = "errors.retry.delay.max.ms";
    public static final String ERRORS_TOLERANCE_CONFIG = "errors.tolerance";
    public static final String ERRORS_LOG_ENABLE_CONFIG = "errors.log.enable";
    public static final String ERRORS_LOG_INCLUDE_MESSAGES = "errors.log.include.messages";
    public static final String ERRORS_DEADLETTERQUEUE_TOPIC_NAME_CONFIG = "errors.deadletterqueue.topic.name"; //sink connectors only
    public static final String ERRORS_DEADLETTERQUEUE_TOPIC_REPLICATION_FACTOR_CONFIG = "errors.deadletterqueue.topic.replication.factor"; //sink connectors only
    public static final String ERRORS_DEADLETTERQUEUE_CONTEXT_HEADERS_ENABLE_CONFIG = "errors.deadletterqueue.context.headers.enable"; //sink connectors only
    public static final String TOPIC_CREATION_GROUPS_CONFIG = "topic.creation.groups"; //source connectors only

    //lightweight builder pattern
    public static ConnectorConfig sink(String name, String connectorClassName) {
        final Map<String, Object> config = new HashMap<>();
        config.put(CONNECTOR_CLASS_CONFIG, connectorClassName);
        return new ConnectorConfig(name, config);
    }

    //lightweight builder pattern
    public static ConnectorConfig source(String name, String connectorClassName) {
        return sink(name, connectorClassName); //stub implementation
    }

    public ConnectorConfig withTasksMax(int tasksMax) {
        config.put(TASKS_MAX_CONFIG, tasksMax);
        return this;
    }

    public ConnectorConfig withTopics(Collection<String> topicNames) {
        config.put(TOPICS_CONFIG, String.join(",", topicNames));
        return this;
    }

    public ConnectorConfig withTopicRegex(String topicRegex) {
        config.put(TOPIC_REGEX_CONFIG, topicRegex);
        return this;
    }

    public ConnectorConfig withKeyConverter(String keyConverter) {
        config.put(KEY_CONVERTER_CONFIG, keyConverter);
        return this;
    }

    public ConnectorConfig withValueConverter(String valueConverter) {
        config.put(VALUE_CONVERTER_CONFIG, valueConverter);
        return this;
    }

    public ConnectorConfig withHeaderConverter(String headerConverter) {
        config.put(HEADER_CONVERTER_CONFIG, headerConverter);
        return this;
    }

    public ConnectorConfig withConfigActionReload(String configActionReload) {
        config.put(CONFIG_ACTION_RELOAD_CONFIG, configActionReload);
        return this;
    }

    public ConnectorConfig withTransforms(String transforms) {
        config.put(TRANSFORMS_CONFIG, transforms);
        return this;
    }

    public ConnectorConfig withPredicates(String predicates) {
        config.put(PREDICATES_CONFIG, predicates);
        return this;
    }

    public ConnectorConfig with(String key, Object value) {
        config.put(key, value);
        return this;
    }

    @JsonProperty("name")
    private final String name;

    @JsonProperty("config")
    private final Map<String, Object> config;

    @JsonCreator
    public ConnectorConfig(@JsonProperty("name") String name, @JsonProperty("config") Map<String, Object> config) {
        this.name = name;
        this.config = config;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ConnectorConfig)) return false;
        ConnectorConfig that = (ConnectorConfig) o;
        return Objects.equals(name, that.name) && Objects.equals(config, that.config);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, config);
    }

    @Override
    public String toString() {
        return "ConnectorConfig{" +
                "name='" + name + '\'' +
                ", config=" + config +
                '}';
    }

}
