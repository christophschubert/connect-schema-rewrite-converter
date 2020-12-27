package net.christophschubert.kafka.connect;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import java.util.Objects;

public class ConnectorConfig {
    @JsonProperty("name")
    private final String name;

    @JsonProperty("config")
    private final Map<String, ?> config;

    @JsonCreator
    public ConnectorConfig(@JsonProperty("name") String name, @JsonProperty("config") Map<String, ?> config) {
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
