package net.christophschubert.kafka.testcontainers;

import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

import java.util.Objects;

public class CPTestContainerFactory {

    String repository = "confluentinc";
    String tag = "6.0.1";

    Network network;

    public CPTestContainerFactory(Network network) {
        Objects.requireNonNull(network);
        this.network = network;
    }

    DockerImageName imageName(String componentName) {
        return DockerImageName.parse(String.format("%s/%s:%s", repository, componentName, tag));
    }

    public KafkaContainer createKafka() {
        return new KafkaContainer(imageName("cp-kafka")).withNetwork(network);
    }

    public SchemaRegistryContainer createSchemaRegistry(KafkaContainer bootstrap) {
        final var sr = new SchemaRegistryContainer(imageName("cp-schema-registry"), bootstrap, network);
        return sr;
    }
}
