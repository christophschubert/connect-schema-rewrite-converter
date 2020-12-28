package net.christophschubert.kafka.connect.converter;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SchemaIdRewriteConverter implements Converter {

    private final static Logger logger = LoggerFactory.getLogger(SchemaIdRewriteConverter.class);

    private SchemaIdRewriter rewriter;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        logger.info(configs.toString());


        //TODO: use constants for field-names
        rewriter = new SchemaIdRewriter(new CachedSchemaRegistryClient(configs.get("source.schema.registry.url").toString(), 10), new
                CachedSchemaRegistryClient(configs.get("destination.schema.registry.url").toString(), 10), isKey);
    }

    private final static ConfigDef configDef = new ConfigDef()
            .define("source.schema.registry.url", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "source schema registry URL")
            .define("destination.schema.registry.url", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "source schema registry URL");


    public ConfigDef config() {
        return configDef;
    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        //TODO: check whether schema is of proper type
        if (! (value instanceof byte[]))
            throw new DataException("cannot convert");
        return rewriter.rewriteId(topic, (byte[])value);
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        return new SchemaAndValue(Schema.BYTES_SCHEMA, value);
    }
}
