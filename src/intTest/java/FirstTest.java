import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.*;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.*;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class FirstTest {

    final String cpVersion = "6.0.1";

    String makeHttpUrl(DockerComposeContainer env, String serviceName, int servicePort) {
        return String.format("http://%s", makeEndPoint(env, serviceName, servicePort));
    }

    String makeEndPoint(DockerComposeContainer env, String serviceName, int servicePort) {
        return String.format("%s:%d", env.getServiceHost(serviceName, servicePort), env.getServicePort(serviceName, servicePort));
    }

    final String srcSchemaRegistryService = "schema-registryA_1";
    final String destSchemaRegistryService = "schema-registryB_1";
    final String srcKafkaService = "kafkaA_1";
    final String destKafkaService = "kafkaB_1";
    final String connectService = "connect_1";


    @Test
    // seems to have some problems as I'm not able to get any data from the consumer
    // should double-check with https://www.confluent.io/blog/kafka-listeners-explained/
    public void dockerComposeEnvironment() throws ExecutionException, InterruptedException {
        final DockerComposeContainer environment =
                new DockerComposeContainer<>(new File("src/intTest/resources/envs/basicReplication/docker-compose.yaml"))
                        .withExposedService(srcSchemaRegistryService, 8081, Wait.forListeningPort())
                        .withExposedService(destSchemaRegistryService, 8082, Wait.forListeningPort())
                        .withExposedService(srcKafkaService, 9091, Wait.forListeningPort())
                        .withExposedService(destKafkaService, 9092, Wait.forListeningPort())
                        .withExposedService(connectService, 8083, Wait.forListeningPort());
        environment.start();


        final Consumer consumer = new KafkaConsumer(Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, makeEndPoint(environment, srcKafkaService, 9091), ConsumerConfig.GROUP_ID_CONFIG, "test-group-3", ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest"), new StringDeserializer(), new StringDeserializer());
        final Producer producer = new KafkaProducer(Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, makeEndPoint(environment, srcKafkaService, 9091)), new StringSerializer(), new StringSerializer());

        AdminClient client = KafkaAdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, makeEndPoint(environment, srcKafkaService, 9091)));
        client.createTopics(List.of(new NewTopic("data.topic", Optional.empty(), Optional.empty()))).all().get();

        System.out.println(client.listTopics().names().get());
        System.out.println(client.listConsumerGroups().all().get());

        try {
            for (int i = 0; i < 30; i++) {
                final var o1 = producer.send(new ProducerRecord("data.topic", "hello", "world")).get();
                System.out.println(o1);
            }

            System.out.println("produced");
            consumer.subscribe(List.of("data.topic"), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    System.out.println("revoked" + partitions);
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    System.out.println("Got assigned" + partitions);
                }
            });
            Thread.sleep(1000);
//            consumer.assign(List.of(new TopicPartition("data.topic", 0)));
            System.out.println(consumer.subscription());

//            System.out.println(" pos: " + consumer.position(new TopicPartition("data.topic", 0)));
            for (int i = 0; i < 5; ++i) {
                final var records = consumer.poll(Duration.ofMillis(500));
                System.out.println(records.isEmpty());
                System.out.println("polled...");
                System.out.println(records);

            }

        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testUsingKafkaTestContainer() throws IOException, InterruptedException {
        final Network network = Network.newNetwork();
        final String cpVersion = "6.0.1";

        final KafkaContainer sourceKafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:" + cpVersion))
                .withNetwork(network);
        sourceKafka.start();

        // TODO: encapsulate this construction into a class
        final GenericContainer sourceSchemaRegistry = new GenericContainer<>(DockerImageName.parse("confluentinc/cp-schema-registry:" + cpVersion))
                .withNetwork(network)
                .dependsOn(sourceKafka)
                .withEnv("SCHEMA_REGISTRY_HOST_NAME", "localhost")
                // TODO: check if there is a better way to get the internal address, getBootstrapServers does not work
                .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", sourceKafka.getNetworkAliases().get(0) + ":9092")
                .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081").withExposedPorts(8081);

        System.out.println(sourceSchemaRegistry.getExposedPorts());

        sourceSchemaRegistry.start();

        HttpClient client = HttpClient.newBuilder().build();
        final var request = HttpRequest.newBuilder(URI.create("http://" + sourceSchemaRegistry.getContainerIpAddress() + ":" + sourceSchemaRegistry.getMappedPort(8081) + "/subjects")).build();
        final var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        System.out.println(response);
        System.out.println(response.body());

        System.out.println("way1 " + sourceKafka.getNetworkAliases().get(0) + ":9092");
        System.out.println("way2 " + sourceKafka.getBootstrapServers());

        final var schemaRegistryUrl = String.format("http://%s:%d", sourceSchemaRegistry.getContainerIpAddress(), sourceSchemaRegistry.getMappedPort(8081));


        final var producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, sourceKafka.getBootstrapServers());
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        producerProperties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

        final Producer producer = new KafkaProducer(producerProperties);


        final var consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, sourceKafka.getBootstrapServers());
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        consumerProperties.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final Consumer consumer = new KafkaConsumer(consumerProperties);

        Schema s = SchemaBuilder.builder().record("User").fields().requiredString("email").requiredInt("age").endRecord();

        for (int i = 0; i < 30; i++) {
            final var record = new GenericRecordBuilder(s).set("email", "peter@a.com").set("age", i + 18).build();
            final var o1 = producer.send(new ProducerRecord("data.topic", "user", record));
            System.out.println(o1);
        }
        System.out.println("produced");
        consumer.subscribe(List.of("data.topic"));
        for (int i = 0; i < 5; ++i) {
            final ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, GenericRecord> record : records) {
                System.out.println(record.value());
            }
        }
    }

    @Test
    public void setupConnect() throws InterruptedException, IOException {
        final Network network = Network.newNetwork();

        final KafkaContainer sourceKafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:" + cpVersion))
                .withNetwork(network);
        final KafkaContainer destinationKafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:" + cpVersion))
                .withNetwork(network);
        sourceKafka.start();
        destinationKafka.start();

        // TODO: encapsulate this construction into a class
        final GenericContainer replicatorContainer = new GenericContainer<>(DockerImageName.parse("confluentinc/cp-enterprise-replicator:" + cpVersion))
                .withNetwork(network)
                .dependsOn(destinationKafka)
                .withLogConsumer(outputFrame -> System.out.print(outputFrame.getUtf8String()))
                .withStartupTimeout(Duration.ofMinutes(5))
                .waitingFor(Wait.forHttp("/"))
                .withEnv("CONNECT_BOOTSTRAP_SERVERS", destinationKafka.getNetworkAliases().get(0) + ":9092")
                .withEnv("CONNECT_REST_PORT", "8083")
                .withEnv("CONNECT_GROUP_ID", "replicator")
                .withEnv("CONNECT_REPLICATION_FACTOR", "1")
                .withEnv("CONNECT_REST_ADVERTISED_HOST_NAME", "localhost")
                .withEnv("CONNECT_CONNECTOR_CLIENT_CONFIG_OVERRIDE_POLICY", "All")
                .withEnv("CONNECT_CONFLUENT_TOPIC_REPLICATION_FACTOR", "1")
                .withEnv("CONNECT_LISTENERS", "http://0.0.0.0:8083")
                .withEnv("CONNECT_LOG4J_ROOT_LOGLEVEL", "INFO")
                .withEnv("CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR", "1")
                .withEnv("CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR", "1")
                .withEnv("CONNECT_STATUS_STORAGE_REPLICATION_FACTOR", "1")
                .withEnv("CONNECT_CONFIG_STORAGE_TOPIC", "default.config")
                .withEnv("CONNECT_OFFSET_STORAGE_TOPIC", "default.offsets")
                .withEnv("CONNECT_STATUS_STORAGE_TOPIC", "default.status")
                .withEnv("CONNECT_PLUGIN_PATH", "/usr/share/java")
                .withEnv("CONNECT_KEY_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")
                .withEnv("CONNECT_VALUE_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")
                .withExposedPorts(8083);

        replicatorContainer.start();

        final var destBootstrapServer = destinationKafka.getNetworkAliases().get(0) + ":9092";
        final var sourceBootstrapServer = sourceKafka.getNetworkAliases().get(0) + ":9092";

        final var replicatorConfig = "{\n" +
                "  \"name\": \"replicator-data\",\n" +
                "  \"config\": {\n" +
                "    \"connector.class\": \"io.confluent.connect.replicator.ReplicatorSourceConnector\",\n" +
                "    \"topic.regex\": \"data\\\\..*\",\n" +
                "    \"topic.rename.format\": \"${topic}.replica\",\n" +
                "    \"key.converter\": \"io.confluent.connect.replicator.util.ByteArrayConverter\",\n" +
                "    \"value.converter\": \"io.confluent.connect.replicator.util.ByteArrayConverter\",\n" +
                "    \"src.kafka.bootstrap.servers\": \"" + sourceBootstrapServer + "\",\n" +
                "    \"src.consumer.group.id\": \"replicator\",\n" +
                "    \"dest.topic.replication.factor\": 1,\n" +
                "    \"dest.kafka.bootstrap.servers\": \"" + destBootstrapServer + "\",\n" +
                "    \"producer.override.bootstrap.servers\": \""+ destBootstrapServer +"\"\n" +
                "  }\n" +
                "}";

        HttpClient client = HttpClient.newBuilder().build();
        final var request = HttpRequest.newBuilder(URI.create("http://" + replicatorContainer.getContainerIpAddress() + ":" + replicatorContainer.getMappedPort(8083) + "/connectors"))
                .POST(HttpRequest.BodyPublishers.ofString(replicatorConfig))
                .header("Content-Type", "application/json")
                .build();
        final var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        System.out.println(response);
        System.out.println(response.body());

        final var producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, sourceKafka.getBootstrapServers());
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        final Producer producer = new KafkaProducer(producerProperties);

        final var consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, destinationKafka.getBootstrapServers());
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final Consumer consumer = new KafkaConsumer(consumerProperties);

        producer.send(new ProducerRecord("some.topic", "user", "value-whatever"));

//        Schema s = SchemaBuilder.builder().record("User").fields().requiredString("email").requiredInt("age").endRecord();

        for (int i = 0; i < 30; i++) {
//            final var record = new GenericRecordBuilder(s).set("email", "peter@a.com").set("age", i + 18).build();
            final var o1 = producer.send(new ProducerRecord("data.topic", "user", "value-" + i));
            System.out.println(o1);
        }
        System.out.println("produced");
        Thread.sleep(2000);
        consumer.subscribe(List.of("data.topic.replica"));
        for (int i = 0; i < 5; ++i) {
            final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.value());
            }
        }
    }


    @Test
    public void setupConnectWithSchemaRegAndCustomConverter() throws InterruptedException, IOException, ExecutionException {
        final Network network = Network.newNetwork();

        final KafkaContainer sourceKafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:" + cpVersion))
                .withNetwork(network);
        final KafkaContainer destinationKafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:" + cpVersion))
                .withNetwork(network);
        sourceKafka.start();
        destinationKafka.start();


        LogWaiter waiter = new LogWaiter("INFO Successfully started up Replicator source task");

        // TODO: encapsulate this construction into a class
        final GenericContainer sourceSchemaRegistry = new GenericContainer<>(DockerImageName.parse("confluentinc/cp-schema-registry:" + cpVersion))
                .withNetwork(network)
                .dependsOn(sourceKafka)
                .withEnv("SCHEMA_REGISTRY_HOST_NAME", "sourceSR")
                // TODO: check if there is a better way to get the internal address, getBootstrapServers does not work
                .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", sourceKafka.getNetworkAliases().get(0) + ":9092")
                .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081").withExposedPorts(8081);

        // TODO: encapsulate this construction into a class
        final GenericContainer destinationSchemaRegistry = new GenericContainer<>(DockerImageName.parse("confluentinc/cp-schema-registry:" + cpVersion))
                .withNetwork(network)
                .dependsOn(destinationKafka)
                .withLogConsumer(outputFrame -> System.out.println(outputFrame.getUtf8String()))
                .withEnv("SCHEMA_REGISTRY_HOST_NAME", "destinationSR")
                // TODO: check if there is a better way to get the internal address, getBootstrapServers does not work
                .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", destinationKafka.getNetworkAliases().get(0) + ":9092")
                .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081").withExposedPorts(8081);

        sourceSchemaRegistry.start();
        destinationSchemaRegistry.start();

        // TODO: encapsulate this construction into a class
        final GenericContainer replicatorContainer = new GenericContainer<>(DockerImageName.parse("confluentinc/cp-enterprise-replicator:" + cpVersion))
                .withNetwork(network)
                .dependsOn(destinationKafka)
                .withLogConsumer(outputFrame -> waiter.accept(outputFrame.getUtf8String()))
                .withStartupTimeout(Duration.ofMinutes(5))
                .waitingFor(Wait.forHttp("/"))

                .withEnv("CONNECT_BOOTSTRAP_SERVERS", destinationKafka.getNetworkAliases().get(0) + ":9092")
                .withEnv("CONNECT_REST_PORT", "8083")
                .withEnv("CONNECT_GROUP_ID", "replicator")
                .withEnv("CONNECT_REPLICATION_FACTOR", "1")
                .withEnv("CONNECT_REST_ADVERTISED_HOST_NAME", "localhost")
                .withEnv("CONNECT_CONNECTOR_CLIENT_CONFIG_OVERRIDE_POLICY", "All")
                .withEnv("CONNECT_CONFLUENT_TOPIC_REPLICATION_FACTOR", "1")
                .withEnv("CONNECT_LISTENERS", "http://0.0.0.0:8083")
                .withEnv("CONNECT_LOG4J_ROOT_LOGLEVEL", "INFO")
                .withEnv("CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR", "1")
                .withEnv("CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR", "1")
                .withEnv("CONNECT_STATUS_STORAGE_REPLICATION_FACTOR", "1")
                .withEnv("CONNECT_CONFIG_STORAGE_TOPIC", "default.config")
                .withEnv("CONNECT_OFFSET_STORAGE_TOPIC", "default.offsets")
                .withEnv("CONNECT_STATUS_STORAGE_TOPIC", "default.status")
                .withEnv("CONNECT_PLUGIN_PATH", "/usr/share/java,/extras")
                .withEnv("CONNECT_KEY_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")
                .withEnv("CONNECT_VALUE_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")
                .withFileSystemBind("./build/libs", "/extras", BindMode.READ_ONLY)
                .withExposedPorts(8083);

        replicatorContainer.start();

        final var destBootstrapServer = destinationKafka.getNetworkAliases().get(0) + ":9092";
        final var sourceBootstrapServer = sourceKafka.getNetworkAliases().get(0) + ":9092";

        //should pre-create all topics:
        final AdminClient adminClient = KafkaAdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, sourceKafka.getBootstrapServers()));
        adminClient.createTopics(List.of(new NewTopic("data.topic", Optional.empty(), Optional.empty()))).all().get();
        adminClient.createTopics(List.of(new NewTopic("some.topic", Optional.empty(), Optional.empty()))).all().get();

        final var srcSRUrl = sourceSchemaRegistry.getNetworkAliases().get(0) + ":8081";
        final var destSRUrl = destinationSchemaRegistry.getNetworkAliases().get(0) + ":8081";

        final var replicatorConfig = "{\n" +
                "  \"name\": \"replicator-data\",\n" +
                "  \"config\": {\n" +
                "    \"connector.class\": \"io.confluent.connect.replicator.ReplicatorSourceConnector\",\n" +
//                "    \"topic.whitelist\": \"data.topic,data.some.topic\",\n" +
                "    \"topic.regex\": \"data\\\\..*\",\n" +
                "    \"topic.rename.format\": \"${topic}.replica\",\n" +
                "    \"key.converter\": \"io.confluent.connect.replicator.util.ByteArrayConverter\",\n" +
                "    \"value.converter\": \"net.christophschubert.kafka.connect.converter.SchemaIdRewriteConverter\",\n" +
                "    \"value.converter.source.schema.registry.url\": \"http://"+srcSRUrl+"\",\n" +
                "    \"value.converter.destination.schema.registry.url\": \"http://"+destSRUrl+ "\",\n" +
                "    \"src.kafka.bootstrap.servers\": \"" + sourceBootstrapServer + "\",\n" +
                "    \"src.consumer.group.id\": \"replicator\",\n" +
                "    \"dest.topic.replication.factor\": 1,\n" +
                "    \"dest.kafka.bootstrap.servers\": \"" + destBootstrapServer + "\",\n" +
                "    \"producer.override.bootstrap.servers\": \""+ destBootstrapServer +"\"\n" +
                "  }\n" +
                "}";

        HttpClient client = HttpClient.newBuilder().build();
        final var request = HttpRequest.newBuilder(URI.create("http://" + replicatorContainer.getContainerIpAddress() + ":" + replicatorContainer.getMappedPort(8083) + "/connectors"))
                .POST(HttpRequest.BodyPublishers.ofString(replicatorConfig))
                .header("Content-Type", "application/json")
                .build();
        final var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        System.out.println(response);
        System.out.println(response.body());

        final var sourceSchemaRegistryUrl = String.format("http://%s:%d", sourceSchemaRegistry.getContainerIpAddress(), sourceSchemaRegistry.getMappedPort(8081));
        final var destinationSchemaRegistryUrl = String.format("http://%s:%d", destinationSchemaRegistry.getContainerIpAddress(), destinationSchemaRegistry.getMappedPort(8081));


        final var producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, sourceKafka.getBootstrapServers());
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        producerProperties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, sourceSchemaRegistryUrl);

        final Producer producer = new KafkaProducer(producerProperties);

        final var consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, destinationKafka.getBootstrapServers());
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        consumerProperties.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, destinationSchemaRegistryUrl);
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final Consumer consumer = new KafkaConsumer(consumerProperties);
        consumer.subscribe(List.of("data.topic.replica"));


        final Schema s = SchemaBuilder.builder().record("User").fields().requiredString("email").requiredInt("age").endRecord();
        final Schema t = SchemaBuilder.builder().record("Order").fields().requiredString("product").requiredInt("quantity").endRecord();
        final Schema t2 = SchemaBuilder.builder().record("SomeSchema").fields().requiredString("product").requiredInt("quantity").endRecord();

        SchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(sourceSchemaRegistryUrl, 10);
        try {
            schemaRegistryClient.register("somesubject", new AvroSchema(t2));
        } catch (RestClientException e) {
            e.printStackTrace();
        }

        //make sure we have a schema which will not be replicated:
        final var orderRecord = new GenericRecordBuilder(t).set("product", "container").set("quantity", 12).build();
        producer.send(new ProducerRecord("some.topic", "order", orderRecord)); // TODO: replicator task doesn't seem to start if this record is produced. Investigate!

        for (int i = 0; i < 30; i++) {
            final var record = new GenericRecordBuilder(s).set("email", "peter@a.com").set("age", i + 18).build();
            final var o1 = producer.send(new ProducerRecord("data.topic", "user", record));
            System.out.println(o1);
        }
        producer.flush(); //remember to flush, otherwise tests will get pretty flaky

        System.out.println("produced");


        while(!waiter.found) {
            for (int i = 0; i < 2; i++) { // jumping through some hoops to ensure that (async) replication has token place
                final ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofMillis(5000));
                for (ConsumerRecord<String, GenericRecord> record : records) {
                    System.out.println("FROM DESTINATION: " + record.value());
                }

            }
        }
    }


    static class LogWaiter {
        public boolean found = false;
        String part;

        public LogWaiter(String part) {
            this.part = part;
        }

        void accept (String s) {
            System.out.print(s);
            if (s.contains(part))
                found = true;
        }
    }

}
