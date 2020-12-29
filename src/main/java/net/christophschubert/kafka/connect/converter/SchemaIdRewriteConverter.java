package net.christophschubert.kafka.connect.converter;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SchemaIdRewriteConverter implements Converter {

    private final static Logger logger = LoggerFactory.getLogger(SchemaIdRewriteConverter.class);

    public final static String SOURCE_SCHEMA_REGISTRY_URL_CONFIG = "source.schema.registry.url";
    public final static String DESTINATION_SCHEMA_REGISTRY_URL_CONFIG = "destination.schema.registry.url";
    public final static String FAIL_ON_UNKNOWN_MAGIC_BYTE = "fail.on.unknown.magic.byte";


    private final static ConfigDef configDef = new ConfigDef()
            .define(SOURCE_SCHEMA_REGISTRY_URL_CONFIG, Type.STRING, Importance.HIGH, "source schema registry URL")
            .define(DESTINATION_SCHEMA_REGISTRY_URL_CONFIG, Type.STRING, Importance.HIGH, "destination schema registry URL")
            .define(FAIL_ON_UNKNOWN_MAGIC_BYTE, Type.BOOLEAN, true, Importance.MEDIUM, "should converter fail on an unknown magic byte");


    private SchemaIdRewriter rewriter;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        logger.info(configs.toString());



        rewriter = new SchemaIdRewriter(
                    new CachedSchemaRegistryClient(configs.get(SOURCE_SCHEMA_REGISTRY_URL_CONFIG).toString(), 10),
                    new CachedSchemaRegistryClient(configs.get(DESTINATION_SCHEMA_REGISTRY_URL_CONFIG).toString(), 10),
                    isKey,
                    (boolean)configs.get(FAIL_ON_UNKNOWN_MAGIC_BYTE)
        );
    }

    public ConfigDef config() {
        return configDef;
    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        if (! schema.equals(Schema.BYTES_SCHEMA)) {
            final var msg = String.format("cannot convert: wrong input schema (%s), expecting Schema.BYTES_SCHEMA", schema.toString());
            throw new DataException(msg);
        }
        if (! (value instanceof byte[])) {
            throw new DataException("cannot convert: input object is not an instance of byte[]");
        }
        return rewriter.rewriteId(topic, (byte[])value);
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        return new SchemaAndValue(Schema.BYTES_SCHEMA, value);
    }
}
