package net.christophschubert.kafka.connect.converter;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;
import io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class SchemaIdRewriter {
    static private final Logger logger = LoggerFactory.getLogger(SchemaIdRewriter.class);

    private final SchemaRegistryClient srcClient;
    private final SchemaRegistryClient destClient;

    private final SubjectNameStrategy nameStrategy = new TopicNameStrategy();

    public SchemaIdRewriter(SchemaRegistryClient srcClient, SchemaRegistryClient destClient, boolean isKey) {
        this.srcClient = srcClient;
        this.destClient = destClient;
        this.isKey = isKey;
    }

    private final boolean isKey;
    private final Map<Integer, Integer> idMapping = new HashMap<>();

    /**
     * Modifies the value parameter inplace by looking up a schema from the source schema registry and replacing the
     * it with corresponding ID in the destination schema registry.
     * @param topic
     * @param value
     * @return
     */
    public byte[] rewriteId(String topic, byte[] value) {
        final ByteBuffer buffer = ByteBuffer.wrap(value);

        //TODO: do we have to fix byte order? I remember it is platform dependent
        final int originalId = buffer.getInt(1);
        final var newId = idMapping.computeIfAbsent(originalId, oId -> reRegister(topic, oId));

        buffer.putInt(1, newId);
        return buffer.array();
    }

    int reRegister(String topic, int originalId) {
        try {
            final var schema = srcClient.getSchemaById(originalId);
            // TODO: how to handle null schema? can this happen or will an exception by thrown?
            final String subject = nameStrategy.subjectName(topic, isKey, schema);
            final var newId = destClient.register(subject, schema);
            logger.info("rewrote ID {} -> {} for subject '{}'", originalId, newId, subject);
            return newId;
        } catch (IOException | RestClientException e) {
            logger.error("error handling schema", e);
            throw new DataException(e);
        }
    }
}
