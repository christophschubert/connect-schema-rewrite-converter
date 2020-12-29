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
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;

public class SchemaIdRewriter {
    static private final Logger logger = LoggerFactory.getLogger(SchemaIdRewriter.class);

    private final SchemaRegistryClient srcClient;
    private final SchemaRegistryClient destClient;
    private final SubjectNameStrategy nameStrategy;
    private final boolean isKey;
    private final boolean failOnUnknownMagicByte;

    private final Map<Integer, Integer> idMapping = new HashMap<>();

    public SchemaIdRewriter(SchemaRegistryClient srcClient, SchemaRegistryClient destClient, boolean isKey, boolean failOnUnknownMagicByte) {
        this(srcClient, destClient, isKey, failOnUnknownMagicByte, new TopicNameStrategy());
    }

    public SchemaIdRewriter(SchemaRegistryClient srcClient, SchemaRegistryClient destClient, boolean isKey, boolean failOnUnknownMagicByte, SubjectNameStrategy subjectNameStrategy) {
        this.srcClient = srcClient;
        this.destClient = destClient;
        this.isKey = isKey;
        this.failOnUnknownMagicByte = failOnUnknownMagicByte;
        this.nameStrategy = subjectNameStrategy;
    }


    /**
     * Modifies the value parameter inplace by looking up a schema from the source schema registry and replacing the
     * it with corresponding ID in the destination schema registry.
     * @param topic
     * @param value
     * @return
     */
    public byte[] rewriteId(String topic, byte[] value) {
        final ByteBuffer buffer = ByteBuffer.wrap(value);
        if (buffer.get(0) != 0) {
            // so far, we only have one format version (magic-byte == 0)
            if (failOnUnknownMagicByte) {
                final String msg = String.format("Unknown magic byte '%d' in topic '%s'.", buffer.get(0), topic);
                throw new DataException(msg);
            }
            logger.debug("Unknown magic byte '{}' in topic {}, not attempting to rewrite", buffer.get(0), topic);
            return value;
        }
        buffer.order(ByteOrder.BIG_ENDIAN); //ensure standard network byte order
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
            final String msg = String.format("error handling schema for topic '%s' with id %d", topic, originalId);
            logger.error(msg, e);
            throw new DataException(msg, e);
        }
    }
}
