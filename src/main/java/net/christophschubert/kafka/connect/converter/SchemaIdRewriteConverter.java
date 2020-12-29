package net.christophschubert.kafka.connect.converter;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SchemaIdRewriteConverter implements Converter {

    private final static Logger logger = LoggerFactory.getLogger(SchemaIdRewriteConverter.class);

    public final static String SOURCE_SCHEMA_REGISTRY_URL_CONFIG = "source.schema.registry.url";
    public final static String DESTINATION_SCHEMA_REGISTRY_URL_CONFIG = "destination.schema.registry.url";
    public final static String FAIL_ON_UNKNOWN_MAGIC_BYTE_CONFIG = "fail.on.unknown.magic.byte";
    public final static String TOPIC_WHITELIST_CONFIG = "topic.whitelist";
    public final static String TOPIC_BLACKLIST_CONFIG = "topic.blacklist";
    public final static String TOPIC_REGEX_CONFIG = "topic.regex";

    private final static String exclusionMessage = String.format("Only one of `%s`, `%s`, and `%s` can be specified.", TOPIC_WHITELIST_CONFIG, TOPIC_BLACKLIST_CONFIG, TOPIC_REGEX_CONFIG);

    //TODO: add config properties for schema registry (e.g. authentication)

    private final static ConfigDef configDef = new ConfigDef()
            .define(SOURCE_SCHEMA_REGISTRY_URL_CONFIG, Type.STRING, Importance.HIGH, "source schema registry URL")
            .define(DESTINATION_SCHEMA_REGISTRY_URL_CONFIG, Type.STRING, Importance.HIGH, "destination schema registry URL")
            .define(FAIL_ON_UNKNOWN_MAGIC_BYTE_CONFIG, Type.BOOLEAN, true, Importance.MEDIUM, "should converter fail on an unknown magic byte")
            .define(TOPIC_WHITELIST_CONFIG, Type.LIST, Importance.MEDIUM, "List of topics for which schemas will be rewritten. " + exclusionMessage)
            .define(TOPIC_BLACKLIST_CONFIG, Type.LIST, Importance.MEDIUM, "List of topics for which schemas will not be rewritten. " + exclusionMessage)
            .define(TOPIC_REGEX_CONFIG, Type.STRING, Importance.MEDIUM, "Pattern on which topics whose schema IDs will be rewritten should be matched. " + exclusionMessage);

    private SchemaIdRewriter rewriter;
    private Predicate<String> topicNameFilter = s -> true;

    Set<String> splitConfigList(String commaSeparatedFields) {
        return Arrays.stream(commaSeparatedFields.split(",")).map(String::trim).filter(Predicate.not(String::isEmpty)).collect(Collectors.toSet());
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        logger.info(configs.toString());

        final var topicConfigCount = Stream.of(TOPIC_BLACKLIST_CONFIG, TOPIC_WHITELIST_CONFIG, TOPIC_REGEX_CONFIG)
                .filter(n -> configs.get(n) != null).count();
        if (topicConfigCount > 1) {
            throw new ConfigException(exclusionMessage);
        }
        if (configs.get(TOPIC_BLACKLIST_CONFIG) != null) {
            final var excludedTopicNames = splitConfigList(configs.get(TOPIC_BLACKLIST_CONFIG).toString());
            topicNameFilter = Predicate.not(excludedTopicNames::contains);
        } else if (configs.get(TOPIC_WHITELIST_CONFIG) != null) {
            final var includedTopicNames = splitConfigList(configs.get(TOPIC_WHITELIST_CONFIG).toString());
            topicNameFilter = includedTopicNames::contains;
        } else if (configs.get(TOPIC_REGEX_CONFIG) != null) {
            final var pattern = Pattern.compile(configs.get(TOPIC_REGEX_CONFIG).toString());
            topicNameFilter = pattern.asMatchPredicate();
        } else {
            topicNameFilter = s -> true;
        }

        rewriter = new SchemaIdRewriter(
                    new CachedSchemaRegistryClient(configs.get(SOURCE_SCHEMA_REGISTRY_URL_CONFIG).toString(), 10),
                    new CachedSchemaRegistryClient(configs.get(DESTINATION_SCHEMA_REGISTRY_URL_CONFIG).toString(), 10),
                    isKey,
                    (boolean)configs.get(FAIL_ON_UNKNOWN_MAGIC_BYTE_CONFIG)
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
        if (! topicNameFilter.test(topic)) {
            return (byte[])value;
        }
        return rewriter.rewriteId(topic, (byte[])value);
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        return new SchemaAndValue(Schema.BYTES_SCHEMA, value);
    }
}
