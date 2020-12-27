package net.christophschubert.kafka.testcontainers;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

import java.net.URI;

public class SchemaRegistryContainer extends CPTestContainer<SchemaRegistryContainer> {



    SchemaRegistryContainer(DockerImageName imageName, KafkaContainer bootstrap, Network network) {
        super(imageName, bootstrap, network);

        withEnv("SCHEMA_REGISTRY_HOST_NAME", "localhost");
        withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", bootstrap.getNetworkAliases().get(0) + ":9092");
        withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081").withExposedPorts(8081);
    }


}
