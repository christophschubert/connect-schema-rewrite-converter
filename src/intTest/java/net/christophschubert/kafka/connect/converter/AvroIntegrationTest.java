package net.christophschubert.kafka.connect.converter;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import net.christophschubert.cp.testcontainers.util.LogWaiter;
import net.christophschubert.kafka.connect.client.ConnectClient;
import net.christophschubert.kafka.connect.client.ConnectorConfig;
import net.christophschubert.cp.testcontainers.CPTestContainer;
import net.christophschubert.cp.testcontainers.CPTestContainerFactory;
import net.christophschubert.cp.testcontainers.KafkaConnectContainer;
import net.christophschubert.cp.testcontainers.SchemaRegistryContainer;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
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
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Assert;
import org.junit.Test;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.KafkaContainer;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class AvroIntegrationTest {

    @Test
    public void setupConnectWithSchemaRegAndCustomConverter() throws InterruptedException, IOException, ExecutionException {

        final var testContainerFactory = new CPTestContainerFactory();

        final KafkaContainer sourceKafka = testContainerFactory.createKafka();
        final KafkaContainer destinationKafka = testContainerFactory.createKafka();
        sourceKafka.start();
        destinationKafka.start();

        final SchemaRegistryContainer sourceSchemaRegistry = testContainerFactory.createSchemaRegistry(sourceKafka);
        sourceSchemaRegistry.start();

        final SchemaRegistryContainer destinationSchemaRegistry = testContainerFactory.createSchemaRegistry(destinationKafka);
        destinationSchemaRegistry.start();

        final LogWaiter waiter = new LogWaiter("INFO Successfully started up Replicator source task");

        final KafkaConnectContainer replicatorContainer = testContainerFactory.createReplicator(destinationKafka)
                .withLogConsumer(outputFrame -> waiter.accept(outputFrame.getUtf8String()))
                .withEnv("CONNECT_PLUGIN_PATH", "/usr/share/java,/extras")
                .withFileSystemBind("./build/libs", "/extras", BindMode.READ_ONLY);
        replicatorContainer.start();

        //should pre-create all topics:
        final AdminClient adminClient = KafkaAdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, sourceKafka.getBootstrapServers()));
        adminClient.createTopics(List.of(
                new NewTopic("data.topic", Optional.empty(), Optional.empty()),
                new NewTopic("some.topic", Optional.empty(), Optional.empty())
        )).all().get();


        final var replicatorConfig = ConnectorConfig.source("replicator-data", "io.confluent.connect.replicator.ReplicatorSourceConnector")
                .withTopicRegex("data\\..*")
                .with("topic.rename.format", "${topic}.replica")
                .withKeyConverter("io.confluent.connect.replicator.util.ByteArrayConverter")
                .withValueConverter("net.christophschubert.kafka.connect.converter.SchemaIdRewriteConverter")
                .with("value.converter.source.schema.registry.url", sourceSchemaRegistry.getInternalBaseUrl())
                .with("value.converter.destination.schema.registry.url", destinationSchemaRegistry.getInternalBaseUrl())
                .with("src.kafka.bootstrap.servers", CPTestContainer.getInternalBootstrap(sourceKafka));

        final ConnectClient connectClient = new ConnectClient(replicatorContainer.getBaseUrl());
        connectClient.startConnector(replicatorConfig);


        final var producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, sourceKafka.getBootstrapServers());
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        producerProperties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, sourceSchemaRegistry.getBaseUrl());

        final Producer<String, GenericRecord> producer = new KafkaProducer<>(producerProperties);

        final var consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, destinationKafka.getBootstrapServers());
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        consumerProperties.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, destinationSchemaRegistry.getBaseUrl());
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final Consumer<String, GenericRecord> consumer = new KafkaConsumer<>(consumerProperties);
        consumer.subscribe(List.of("data.topic.replica"));


        final Schema s = SchemaBuilder.builder().record("User").fields().requiredString("email").requiredInt("age").endRecord();
        final Schema t = SchemaBuilder.builder().record("Order").fields().requiredString("product").requiredInt("quantity").endRecord();
        final Schema t2 = SchemaBuilder.builder().record("SomeSchema").fields().requiredString("product").requiredInt("quantity").endRecord();

        SchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(sourceSchemaRegistry.getBaseUrl(), 10);
        try {
            schemaRegistryClient.register("some-subject", new AvroSchema(t2));
        } catch (RestClientException e) {
            e.printStackTrace();
        }

        //make sure we have a schema which will not be replicated:
        final var orderRecord = new GenericRecordBuilder(t).set("product", "container").set("quantity", 12).build();
        producer.send(new ProducerRecord<>("some.topic", "order", orderRecord));

        final var totalMessageCount = 20;
        for (int i = 0; i < totalMessageCount; ++i) {
            final var record = new GenericRecordBuilder(s).set("email", "peter@a.com").set("age", i + 18).build();
            producer.send(new ProducerRecord<>("data.topic", "user", record));

        }
        producer.flush(); //remember to flush, otherwise tests will get pretty flaky

        var msgCount = 0;
        while (!waiter.found) {
            for (int i = 0; i < 2; ++i) { // jumping through some hoops to ensure that (async) replication has token place
                final ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofMillis(5000));
                for (ConsumerRecord<String, GenericRecord> record : records) {
                    Assert.assertEquals("peter@a.com", record.value().get("email").toString());
                    ++msgCount;
                }
            }
        }
        Assert.assertEquals(totalMessageCount, msgCount);
    }
}
