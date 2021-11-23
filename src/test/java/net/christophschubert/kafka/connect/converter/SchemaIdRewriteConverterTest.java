package net.christophschubert.kafka.connect.converter;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class SchemaIdRewriteConverterTest {

    @Test
    void onlySingleTopicSelectionOption() {
        final var config = Map.of(
                SchemaIdRewriteConverter.TOPIC_EXCLUDE_CONFIG, "a,b",
                SchemaIdRewriteConverter.TOPIC_INCLUDE_CONFIG, "x,y"
        );
        final var converter = new SchemaIdRewriteConverter();

        assertThrows(ConfigException.class, () -> converter.configure(config, false));
    }

    @Test
    void topicFilterIncludeTest() {
        final var config = Map.of(
             SchemaIdRewriteConverter.TOPIC_INCLUDE_CONFIG, "topicA,topicB,  topicC"
        );
        final var converter = new SchemaIdRewriteConverter();
        converter.configure(config, true);
        assertAll(
                () -> assertFalse(converter.shouldRewriteForTopic("notInList")),
                () -> assertTrue(converter.shouldRewriteForTopic("topicA")),
                () -> assertTrue(converter.shouldRewriteForTopic("topicB")),
                () -> assertTrue(converter.shouldRewriteForTopic("topicC"))
        );
    }

    @Test
    void topicFilterExcludeTest() {
        final var config = Map.of(
                SchemaIdRewriteConverter.TOPIC_EXCLUDE_CONFIG, "topicA,topicB,  topicC"
        );
        final var converter = new SchemaIdRewriteConverter();
        converter.configure(config, true);
        assertAll(
                () -> assertTrue(converter.shouldRewriteForTopic("notInList")),
                () -> assertFalse(converter.shouldRewriteForTopic("topicA")),
                () -> assertFalse(converter.shouldRewriteForTopic("topicB")),
                () -> assertFalse(converter.shouldRewriteForTopic("topicC"))
        );
    }

    @Test
    void topicFilterRegexTest() {
        final var config = Map.of(
                SchemaIdRewriteConverter.TOPIC_REGEX_CONFIG, ".*\\.avro"
        );
        final var converter = new SchemaIdRewriteConverter();
        converter.configure(config, true);
        assertAll(
                () -> assertFalse(converter.shouldRewriteForTopic("avro")),
                () -> assertTrue(converter.shouldRewriteForTopic("xxx.avro")),
                () -> assertTrue(converter.shouldRewriteForTopic(".avro")),
                () -> assertFalse(converter.shouldRewriteForTopic("unknown")),
                () -> assertFalse(converter.shouldRewriteForTopic("hello.avro.bye")),
                () -> assertFalse(converter.shouldRewriteForTopic("xxx.raw"))
        );
    }

    @Test
    void booleanConfigParsing() {
        assertAll(
                () -> assertTrue(SchemaIdRewriteConverter.saveParseBooleanDefaultTrue(null)),
                () -> assertFalse(SchemaIdRewriteConverter.saveParseBooleanDefaultTrue(false)),
                () -> assertTrue(SchemaIdRewriteConverter.saveParseBooleanDefaultTrue(true)),
                () -> assertFalse(SchemaIdRewriteConverter.saveParseBooleanDefaultTrue(false)),
                () -> assertTrue(SchemaIdRewriteConverter.saveParseBooleanDefaultTrue("true")),
                () -> assertFalse(SchemaIdRewriteConverter.saveParseBooleanDefaultTrue("false")),
                () -> assertFalse(SchemaIdRewriteConverter.saveParseBooleanDefaultTrue("test")),
                () -> assertFalse(SchemaIdRewriteConverter.saveParseBooleanDefaultTrue(new Object()))
        );
    }
}