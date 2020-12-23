package net.christophschubert.kafka.connect.converter;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class SchemaRewriteConverter implements Converter {

    private static final String SCHEMA_ID_FIELD = "schema_id";
    private static final String WF0_ID = "WireFormat0";
    private static final String PAYLOAD_FIELD = "payload";
    private static final Schema wireFormat0Schema = SchemaBuilder.struct().
            name(WF0_ID).
            field(SCHEMA_ID_FIELD, Schema.INT32_SCHEMA).
            field(PAYLOAD_FIELD, Schema.BYTES_SCHEMA);
    private static final Set<String> knownSchemaNames = Set.of(WF0_ID);
    private SchemaRegistryClient sourceClient;
    private SchemaRegistryClient destinationClient;

    private final Map<Integer, Integer> schemaIdMapping = new HashMap<>();

    private boolean isKey;


    private static final String SOURCE_SR_URL_CONFIG = "";
    private final static ConfigDef configDef = new ConfigDef();

    public ConfigDef config() {
        return configDef;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        //TODO: configure schema registry clients
        sourceClient = new CachedSchemaRegistryClient("http://localhost:8081", 20);
        destinationClient = new CachedSchemaRegistryClient("http://localhost:8081", 20);

        this.isKey = isKey;
    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        if (!knownSchemaNames.contains(schema.name())) {
            final String msg = String.format("bad schema"); //TODO: improve message
            throw new DataException(msg);
        }
        //TODO: improve errorhandling:
        final Struct s = (Struct)value;

        final Integer sourceSchemaId = s.getInt32(SCHEMA_ID_FIELD);
        if (!schemaIdMapping.containsKey(sourceSchemaId)) {
            try {
                final var srcSchema = sourceClient.getSchemaById(sourceSchemaId);
                final String destinationSubject = String.format("%s-%s", topic, isKey ? "key" : "value");
                final var destinationSchemaId = destinationClient.register(destinationSubject, srcSchema);
                schemaIdMapping.put(sourceSchemaId, destinationSchemaId);
            } catch (IOException | RestClientException e) {
                final String msg = String.format("error re-registering schema with Id '%d'", sourceSchemaId);
                throw new DataException(msg, e);
            }
        }
        final int destinationSchemaId = schemaIdMapping.get(sourceSchemaId);
        final ByteBuffer buffer = ByteBuffer.allocate(s.getBytes(PAYLOAD_FIELD).length + 5);
        buffer.put((byte)0);
        buffer.putInt(destinationSchemaId);
        buffer.put(s.getBytes(PAYLOAD_FIELD));

        return buffer.array();
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        if (value[0] != 0) {
            final String msg = String.format("Unknown magic byte '%s' in message from topic %s", value[0], topic);
            throw new DataException(msg);
        }

        final ByteBuffer buffer = ByteBuffer.wrap(value);
        // TODO: double check and fix byte order
        final int schemaId = buffer.getInt(1);
        final byte[] payload = new byte[value.length - 5];
        buffer.get(payload, 5, payload.length);
        final Struct s = new Struct(wireFormat0Schema).
                put(SCHEMA_ID_FIELD, schemaId).
                put(PAYLOAD_FIELD, payload);
        return new SchemaAndValue(wireFormat0Schema, s);
    }
}
