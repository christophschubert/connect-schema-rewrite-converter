package net.christophschubert.kafka.connect.converter;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.kafka.connect.errors.DataException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class SchemaIdRewriterTest {

    @Test
    public void failOnUnknownMagicByteIfConfigured() {
        final SchemaRegistryClient srcClient = new MockSchemaRegistryClient();
        final SchemaRegistryClient destClient = new MockSchemaRegistryClient();
        final var rewriter = new SchemaIdRewriter(srcClient, destClient, false, true);

        final ByteBuffer buffer = ByteBuffer.allocate(15);
        buffer.put(SchemaIdRewriter.magicBytePosition, (byte)1);
        assertThrows(DataException.class, () ->  rewriter.rewriteId("topic", buffer.array()));
    }

    @Test
    public void passMessageOnUnkownMagicByteIfConfigured() {
        final SchemaRegistryClient srcClient = new MockSchemaRegistryClient();
        final SchemaRegistryClient destClient = new MockSchemaRegistryClient();
        final var rewriter = new SchemaIdRewriter(srcClient, destClient, false, false);

        byte[] original = {1, 0, 0, 0, 2, 1, 1, 1, 1, 1, 1, };
        byte[] clone = original.clone(); // needed since rewriter modifies array in place

        assertArrayEquals(original, rewriter.rewriteId("topic", clone));
    }

    @Test
    public void rewriteSchema() throws IOException, RestClientException {
        final MockSchemaRegistryClient srcClient = new MockSchemaRegistryClient();
        final SchemaRegistryClient destClient = new MockSchemaRegistryClient();
        final var rewriter = new SchemaIdRewriter(srcClient, destClient, false, true);

        Schema s = SchemaBuilder.builder().record("User").fields().requiredString("email").requiredString("user").endRecord();
        Schema t = SchemaBuilder.builder().record("User").fields().requiredString("age").requiredString("nage").endRecord();

        SchemaProvider provider = new AvroSchemaProvider();
        final var parsedSchema = provider.parseSchema(s.toString(), Collections.emptyList()).get();
        destClient.register("test-value", provider.parseSchema(t.toString(), Collections.emptyList()).get());

        srcClient.register("test-value", parsedSchema, 1, 15);
        byte[] original = {0, 0, 0, 0, 15, 1, 1, 1, 1, 1, 1, };
        byte[] clone = original.clone(); // needed since rewriter modifies array in place

        byte[] rewritten = {0, 0, 0, 0, 2, 1, 1, 1, 1, 1, 1,};
        assertArrayEquals(rewritten, rewriter.rewriteId("test", clone));
        assertEquals(parsedSchema.canonicalString() ,destClient.getSchemaById(2).canonicalString());

    }
}