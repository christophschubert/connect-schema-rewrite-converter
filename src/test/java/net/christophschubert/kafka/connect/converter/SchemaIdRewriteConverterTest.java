package net.christophschubert.kafka.connect.converter;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class SchemaIdRewriteConverterTest {

    @Test
    public void onlySingleTopicSelectionOption() {
        final var config = Map.of(SchemaIdRewriteConverter.TOPIC_BLACKLIST_CONFIG, "a,b", SchemaIdRewriteConverter.TOPIC_WHITELIST_CONFIG, "x,y");
        final var converter = new SchemaIdRewriteConverter();

        assertThrows(ConfigException.class, () -> converter.configure(config, false));
    }

}