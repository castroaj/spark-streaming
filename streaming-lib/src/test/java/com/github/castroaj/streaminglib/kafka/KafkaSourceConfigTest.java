package com.github.castroaj.streaminglib.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import jakarta.validation.ConstraintViolationException;
import org.junit.jupiter.api.Test;

class KafkaSourceConfigTest {

    // -------------------------------------------------------------------------
    // Happy path
    // -------------------------------------------------------------------------

    @Test
    void build_allRequiredFields_returnsConfig() {
        KafkaSourceConfig config = KafkaSourceConfig.builder()
            .bootstrapServers("kafka:9092")
            .topic("my-topic")
            .groupId("my-group")
            .build();

        assertEquals("kafka:9092", config.getBootstrapServers());
        assertEquals("my-topic", config.getTopic());
        assertEquals("my-group", config.getGroupId());
    }

    // -------------------------------------------------------------------------
    // Required field validation
    // -------------------------------------------------------------------------

    @Test
    void build_missingBootstrapServers_throwsConstraintViolationException() {
        ConstraintViolationException ex = assertThrows(ConstraintViolationException.class, () ->
            KafkaSourceConfig.builder()
                .topic("my-topic")
                .groupId("my-group")
                .build());
        Set<String> paths = violationPaths(ex);
        assertTrue(paths.contains("bootstrapServers"),
            "Violations should include 'bootstrapServers'");
    }

    @Test
    void build_missingTopic_throwsConstraintViolationException() {
        ConstraintViolationException ex = assertThrows(ConstraintViolationException.class, () ->
            KafkaSourceConfig.builder()
                .bootstrapServers("kafka:9092")
                .groupId("my-group")
                .build());
        Set<String> paths = violationPaths(ex);
        assertTrue(paths.contains("topic"), "Violations should include 'topic'");
    }

    @Test
    void build_missingGroupId_throwsConstraintViolationException() {
        ConstraintViolationException ex = assertThrows(ConstraintViolationException.class, () ->
            KafkaSourceConfig.builder()
                .bootstrapServers("kafka:9092")
                .topic("my-topic")
                .build());
        Set<String> paths = violationPaths(ex);
        assertTrue(paths.contains("groupId"), "Violations should include 'groupId'");
    }

    @Test
    void build_multipleMissingFields_violationsListAll() {
        ConstraintViolationException ex = assertThrows(ConstraintViolationException.class, () ->
            KafkaSourceConfig.builder().build());
        Set<String> paths = violationPaths(ex);
        assertTrue(paths.contains("bootstrapServers"), "Should report bootstrapServers");
        assertTrue(paths.contains("topic"), "Should report topic");
        assertTrue(paths.contains("groupId"), "Should report groupId");
    }

    // -------------------------------------------------------------------------
    // Default values
    // -------------------------------------------------------------------------

    @Test
    void build_defaultStartingOffsets_isEarliest() {
        KafkaSourceConfig config = minimalConfig();
        assertEquals(StartingOffsets.EARLIEST, config.getStartingOffsets());
    }

    @Test
    void build_defaultMaxOffsets_is100000() {
        KafkaSourceConfig config = minimalConfig();
        assertEquals(100_000, config.getMaxOffsetsPerTrigger());
    }

    @Test
    void build_defaultPollTimeout_is120Seconds() {
        KafkaSourceConfig config = minimalConfig();
        assertEquals(Duration.ofSeconds(120), config.getPollTimeout());
    }

    @Test
    void build_defaultExtraOptions_isEmpty() {
        KafkaSourceConfig config = minimalConfig();
        assertTrue(config.getExtraOptions().isEmpty());
    }

    // -------------------------------------------------------------------------
    // Optional field overrides
    // -------------------------------------------------------------------------

    @Test
    void build_customStartingOffsets_isStored() {
        KafkaSourceConfig config = KafkaSourceConfig.builder()
            .bootstrapServers("kafka:9092")
            .topic("t")
            .groupId("g")
            .startingOffsets(StartingOffsets.LATEST)
            .build();
        assertEquals(StartingOffsets.LATEST, config.getStartingOffsets());
    }

    @Test
    void build_extraOption_isStored() {
        KafkaSourceConfig config = KafkaSourceConfig.builder()
            .bootstrapServers("kafka:9092")
            .topic("t")
            .groupId("g")
            .extraOption("security.protocol", "SASL_PLAINTEXT")
            .build();
        assertEquals("SASL_PLAINTEXT", config.getExtraOptions().get("security.protocol"));
    }

    @Test
    void build_multipleExtraOptions_allStored() {
        KafkaSourceConfig config = KafkaSourceConfig.builder()
            .bootstrapServers("kafka:9092")
            .topic("t")
            .groupId("g")
            .extraOption("key1", "value1")
            .extraOption("key2", "value2")
            .build();
        Map<String, String> opts = config.getExtraOptions();
        assertEquals("value1", opts.get("key1"));
        assertEquals("value2", opts.get("key2"));
    }

    @Test
    void build_extraOptionsAreCopied_mutationDoesNotAffectConfig() {
        KafkaSourceConfig.Builder builder = KafkaSourceConfig.builder()
            .bootstrapServers("kafka:9092")
            .topic("t")
            .groupId("g")
            .extraOption("key", "original");
        KafkaSourceConfig config = builder.build();

        // Attempt to mutate the returned map — should throw since it's unmodifiable
        assertThrows(UnsupportedOperationException.class, () ->
            config.getExtraOptions().put("key", "mutated"));
        assertEquals("original", config.getExtraOptions().get("key"));
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private Set<String> violationPaths(ConstraintViolationException ex) {
        return ex.getConstraintViolations().stream()
            .map(v -> v.getPropertyPath().toString())
            .collect(Collectors.toSet());
    }

    private KafkaSourceConfig minimalConfig() {
        return KafkaSourceConfig.builder()
            .bootstrapServers("kafka:9092")
            .topic("my-topic")
            .groupId("my-group")
            .build();
    }
}
