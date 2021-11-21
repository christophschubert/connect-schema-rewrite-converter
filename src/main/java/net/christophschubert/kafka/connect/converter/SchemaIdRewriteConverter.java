package net.christophschubert.kafka.connect.converter;

import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
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

import java.util.*;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SchemaIdRewriteConverter implements Converter {

    private final static Logger logger = LoggerFactory.getLogger(SchemaIdRewriteConverter.class);

    public final static String SOURCE_SCHEMA_REGISTRY_URL_CONFIG = "source.schema.registry.url";
    public final static String DESTINATION_SCHEMA_REGISTRY_URL_CONFIG = "destination.schema.registry.url";
    public final static String FAIL_ON_UNKNOWN_MAGIC_BYTE_CONFIG = "fail.on.unknown.magic.byte";
    public final static String TOPIC_INCLUDE_CONFIG = "topics.include";
    public final static String TOPIC_EXCLUDE_CONFIG = "topics.exclude";
    public final static String TOPIC_REGEX_CONFIG = "topic.regex";

    private final static String exclusionMessage = String.format("Only one of `%s`, `%s`, and `%s` can be specified.", TOPIC_INCLUDE_CONFIG, TOPIC_EXCLUDE_CONFIG, TOPIC_REGEX_CONFIG);

    //TODO: add config properties for schema registry (e.g. authentication)
    //could be modeled after: https://github.com/confluentinc/schema-registry/blob/master/schema-serializer/src/main/java/io/confluent/kafka/serializers/AbstractKafkaSchemaSerDeConfig.java
    private final static ConfigDef configDef = new ConfigDef()
            .define(FAIL_ON_UNKNOWN_MAGIC_BYTE_CONFIG, Type.BOOLEAN, true, Importance.MEDIUM, "should converter fail on an unknown magic byte")
            .define(TOPIC_INCLUDE_CONFIG, Type.LIST, Importance.MEDIUM, "List of topics for which schemas will be rewritten. " + exclusionMessage)
            .define(TOPIC_EXCLUDE_CONFIG, Type.LIST, Importance.MEDIUM, "List of topics for which schemas will not be rewritten. " + exclusionMessage)
            .define(TOPIC_REGEX_CONFIG, Type.STRING, Importance.MEDIUM, "Pattern on which topics whose schema IDs will be rewritten should be matched. " + exclusionMessage);

    public static final String SOURCE_PREFIX = "source.";
    public static final String DESTINATION_PREFIX = "destination.";

    static {
        addSchemaRegistryConfig(configDef, SOURCE_PREFIX);
        SchemaRegistryClientConfig.withClientSslSupport(configDef, SOURCE_PREFIX + SchemaRegistryClientConfig.CLIENT_NAMESPACE);
        addSchemaRegistryConfig(configDef, DESTINATION_PREFIX);
        SchemaRegistryClientConfig.withClientSslSupport(configDef, DESTINATION_PREFIX + SchemaRegistryClientConfig.CLIENT_NAMESPACE);
    }

    private static void addSchemaRegistryConfig(ConfigDef configDef, String namespace) {
        final var prefix = namespace + SchemaRegistryClientConfig.CLIENT_NAMESPACE;
        //TODO: add docu-strings
        configDef.define(prefix + "url", Type.STRING, Importance.HIGH, namespace + " schema registry URK");
        configDef.define(prefix + SchemaRegistryClientConfig.BASIC_AUTH_CREDENTIALS_SOURCE, Type.STRING, Importance.MEDIUM, "TODD");
        configDef.define(prefix + SchemaRegistryClientConfig.USER_INFO_CONFIG, Type.STRING, Importance.MEDIUM, "TODO");
        configDef.define(prefix + SchemaRegistryClientConfig.BEARER_AUTH_TOKEN_CONFIG, Type.STRING, Importance.MEDIUM, "TODO");
        configDef.define(prefix + SchemaRegistryClientConfig.BEARER_AUTH_CREDENTIALS_SOURCE, Type.STRING, Importance.MEDIUM, "TODO");
    }

    private SchemaIdRewriter rewriter;
    private Predicate<String> topicNameFilter = s -> true;

    //TODO: comment this, also in README
    private final static List<SchemaProvider> allProviders = List.of(new AvroSchemaProvider(), new ProtobufSchemaProvider(), new JsonSchemaProvider());

    Set<String> splitConfigList(String commaSeparatedFields) {
        return Arrays.stream(commaSeparatedFields.split(",")).map(String::trim).filter(Predicate.not(String::isEmpty)).collect(Collectors.toSet());
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        logger.info(configs.toString());

        final var topicConfigCount = Stream.of(TOPIC_EXCLUDE_CONFIG, TOPIC_INCLUDE_CONFIG, TOPIC_REGEX_CONFIG)
                .filter(n -> configs.get(n) != null).count();
        if (topicConfigCount > 1) {
            throw new ConfigException(exclusionMessage);
        }
        if (configs.get(TOPIC_EXCLUDE_CONFIG) != null) {
            final var excludedTopicNames = splitConfigList(configs.get(TOPIC_EXCLUDE_CONFIG).toString());
            topicNameFilter = Predicate.not(excludedTopicNames::contains);
        } else if (configs.get(TOPIC_INCLUDE_CONFIG) != null) {
            final var includedTopicNames = splitConfigList(configs.get(TOPIC_INCLUDE_CONFIG).toString());
            topicNameFilter = includedTopicNames::contains;
        } else if (configs.get(TOPIC_REGEX_CONFIG) != null) {
            final var pattern = Pattern.compile(configs.get(TOPIC_REGEX_CONFIG).toString());
            topicNameFilter = pattern.asMatchPredicate();
        } else {
            topicNameFilter = s -> true;
        }

        rewriter = new SchemaIdRewriter(
                buildSrClient(configs, SOURCE_PREFIX),
                    buildSrClient(configs, DESTINATION_PREFIX),
                    isKey,
                    saveParseBooleanDefaultTrue(configs.get(FAIL_ON_UNKNOWN_MAGIC_BYTE_CONFIG))
        );
    }

    CachedSchemaRegistryClient buildSrClient(Map<String, ?> configs, String prefix) {
        final List<String> urls = Arrays.asList(Objects.toString(configs.get(prefix + "schema.registry.url")).split(","));
        final Map<String, ?> strippedConfigs = configs.entrySet().stream().filter(e -> e.getKey().startsWith(prefix)).collect(Collectors.toMap(e -> e.getKey().substring(prefix.length()), Map.Entry::getValue));

        //logger.info("Building sr-client for prefix{} with configs {}", prefix, strippedConfigs);
        return new CachedSchemaRegistryClient(urls, 10, allProviders, strippedConfigs);
    }

    //TODO: double check code & write test case
    boolean saveParseBooleanDefaultTrue(Object o) {
        if (o == null) {
            return true;
        }
        if (o instanceof Boolean) {
            return (boolean) o;
        }
        if (o instanceof String) {
            return Boolean.parseBoolean((String)o);
        }
        return false;
    }

    public ConfigDef config() {
        return configDef;
    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        if (! (schema.equals(Schema.BYTES_SCHEMA) || schema.equals(Schema.OPTIONAL_BYTES_SCHEMA) )) {
            final var msg = String.format("cannot convert: wrong input schema (%s), topic '%s', expecting Schema.BYTES_SCHEMA", schema, topic);
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
