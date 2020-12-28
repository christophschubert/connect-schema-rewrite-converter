package net.christophschubert.kafka.testcontainers;

import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Set;
import java.util.stream.Collectors;

public class KafkaConnectContainer extends CPTestContainer<KafkaConnectContainer> {

    final int defaultPort = 8083;

    void _configure(KafkaContainer bootstrap) {
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

    KafkaConnectContainer(DockerImageName dockerImageName, KafkaContainer bootstrap, Network network) {
        super(dockerImageName, bootstrap, network);
        _configure(bootstrap);

    }

    protected KafkaConnectContainer(ImageFromDockerfile image, KafkaContainer bootstrap, Network network){
        super(image, bootstrap, network);
        _configure(bootstrap);
    }

    public static ImageFromDockerfile customImage(Set<String> connectorNames, String baseImageName) {
        final var commandPrefix = "confluent-hub install --no-prompt ";
        final String command = connectorNames.stream().map(s -> commandPrefix + s).collect(Collectors.joining(" && "));
        return new ImageFromDockerfile().withDockerfileFromBuilder(builder ->
            builder
                    .from(baseImageName)
                    .run(command)
                    .build()
        );
    }

    public String getBaseUrl() {
       return String.format("http://%s:%d", getContainerIpAddress(), getMappedPort(defaultPort));

    }
}
