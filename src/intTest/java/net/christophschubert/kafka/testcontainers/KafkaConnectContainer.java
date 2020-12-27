package net.christophschubert.kafka.testcontainers;

import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

public class KafkaConnectContainer extends CPTestContainer<KafkaConnectContainer> {

    final int defaultPort = 8083;

    KafkaConnectContainer(DockerImageName dockerImageName, KafkaContainer bootstrap, Network network) {
        super(dockerImageName, bootstrap, network);

        withStartupTimeout(Duration.ofMinutes(5));
        waitingFor(Wait.forHttp("/"));
        withEnv("CONNECT_BOOTSTRAP_SERVERS", getInternalBootstrap(bootstrap));
        withEnv("CONNECT_REST_PORT", "" + defaultPort);
        withEnv("CONNECT_GROUP_ID", "connect");
        withEnv("CONNECT_REPLICATION_FACTOR", "1");
        withEnv("CONNECT_REST_ADVERTISED_HOST_NAME", "localhost"); //changed this from example
        withEnv("CONNECT_CONNECTOR_CLIENT_CONFIG_OVERRIDE_POLICY", "All");
        withEnv("CONNECT_CONFLUENT_TOPIC_REPLICATION_FACTOR", "1");
        withEnv("CONNECT_LISTENERS", "http://0.0.0.0:" + defaultPort);
        withEnv("CONNECT_LOG4J_ROOT_LOGLEVEL", "INFO");
        withEnv("CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR", "1");
        withEnv("CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR", "1");
        withEnv("CONNECT_STATUS_STORAGE_REPLICATION_FACTOR", "1");
        withEnv("CONNECT_CONFIG_STORAGE_TOPIC", "default.config");
        withEnv("CONNECT_OFFSET_STORAGE_TOPIC", "default.offsets");
        withEnv("CONNECT_STATUS_STORAGE_TOPIC", "default.status");
        withEnv("CONNECT_PLUGIN_PATH", "/usr/share/java");
        withEnv("CONNECT_KEY_CONVERTER", "org.apache.kafka.connect.json.JsonConverter");
        withEnv("CONNECT_VALUE_CONVERTER", "org.apache.kafka.connect.json.JsonConverter");
        withExposedPorts(defaultPort);
    }

    public String getBaseUrl() {
       return String.format("http://%s:%d", getContainerIpAddress(), getMappedPort(defaultPort)); //TODO: finish!

    }
}
