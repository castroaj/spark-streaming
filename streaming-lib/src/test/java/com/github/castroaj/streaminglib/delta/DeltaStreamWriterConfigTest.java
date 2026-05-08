package com.github.castroaj.streaminglib.delta;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import jakarta.validation.ConstraintViolationException;
import org.junit.jupiter.api.Test;

class DeltaStreamWriterConfigTest {

    // -------------------------------------------------------------------------
    // Happy path
    // -------------------------------------------------------------------------

    @Test
    void build_allRequiredFields_returnsConfig() {
        DeltaStreamWriterConfig config = DeltaStreamWriterConfig.builder()
            .destinationPath("/tmp/dest")
            .checkpointBasePath("/tmp/checkpoints")
            .checkpointName("stream-1")
            .build();

        assertEquals("/tmp/dest", config.getDestinationPath());
        assertEquals("/tmp/checkpoints", config.getCheckpointBasePath());
        assertEquals("stream-1", config.getCheckpointName());
    }

    @Test
    void build_requiredAndOptionalFields_returnsConfig() {
        DeltaStreamWriterConfig config = DeltaStreamWriterConfig.builder()
            .destinationPath("/tmp/dest")
            .checkpointBasePath("/tmp/checkpoints")
            .checkpointName("stream-1")
            .outputMode(OutputMode.COMPLETE)
            .trigger(new TriggerConfig.Once())
            .clusteringColumns(List.of("date", "region"))
            .maxRecordsPerFile(1_000_000L)
            .extraOption("k", "v")
            .build();

        assertEquals(OutputMode.COMPLETE, config.getOutputMode());
        assertInstanceOf(TriggerConfig.Once.class, config.getTrigger());
        assertEquals(List.of("date", "region"), config.getClusteringColumns());
        assertEquals(1_000_000L, config.getMaxRecordsPerFile());
        assertEquals("v", config.getExtraOptions().get("k"));
    }

    // -------------------------------------------------------------------------
    // Required field validation
    // -------------------------------------------------------------------------

    @Test
    void build_missingDestinationPath_throwsConstraintViolationException() {
        ConstraintViolationException ex = assertThrows(ConstraintViolationException.class, () ->
            DeltaStreamWriterConfig.builder()
                .checkpointBasePath("/tmp/checkpoints")
                .checkpointName("stream-1")
                .build());
        assertTrue(violationPaths(ex).contains("destinationPath"));
    }

    @Test
    void build_missingCheckpointBasePath_throwsConstraintViolationException() {
        ConstraintViolationException ex = assertThrows(ConstraintViolationException.class, () ->
            DeltaStreamWriterConfig.builder()
                .destinationPath("/tmp/dest")
                .checkpointName("stream-1")
                .build());
        assertTrue(violationPaths(ex).contains("checkpointBasePath"));
    }

    @Test
    void build_missingCheckpointName_throwsConstraintViolationException() {
        ConstraintViolationException ex = assertThrows(ConstraintViolationException.class, () ->
            DeltaStreamWriterConfig.builder()
                .destinationPath("/tmp/dest")
                .checkpointBasePath("/tmp/checkpoints")
                .build());
        assertTrue(violationPaths(ex).contains("checkpointName"));
    }

    @Test
    void build_blankDestinationPath_throwsConstraintViolationException() {
        ConstraintViolationException ex = assertThrows(ConstraintViolationException.class, () ->
            DeltaStreamWriterConfig.builder()
                .destinationPath("   ")
                .checkpointBasePath("/tmp/checkpoints")
                .checkpointName("stream-1")
                .build());
        assertTrue(violationPaths(ex).contains("destinationPath"));
    }

    @Test
    void build_blankCheckpointBasePath_throwsConstraintViolationException() {
        ConstraintViolationException ex = assertThrows(ConstraintViolationException.class, () ->
            DeltaStreamWriterConfig.builder()
                .destinationPath("/tmp/dest")
                .checkpointBasePath("  ")
                .checkpointName("stream-1")
                .build());
        assertTrue(violationPaths(ex).contains("checkpointBasePath"));
    }

    @Test
    void build_blankCheckpointName_throwsConstraintViolationException() {
        ConstraintViolationException ex = assertThrows(ConstraintViolationException.class, () ->
            DeltaStreamWriterConfig.builder()
                .destinationPath("/tmp/dest")
                .checkpointBasePath("/tmp/checkpoints")
                .checkpointName(" ")
                .build());
        assertTrue(violationPaths(ex).contains("checkpointName"));
    }

    @Test
    void build_allRequiredFieldsMissing_violationsListAll() {
        ConstraintViolationException ex = assertThrows(ConstraintViolationException.class, () ->
            DeltaStreamWriterConfig.builder().build());
        Set<String> paths = violationPaths(ex);
        assertTrue(paths.contains("destinationPath"));
        assertTrue(paths.contains("checkpointBasePath"));
        assertTrue(paths.contains("checkpointName"));
    }

    // -------------------------------------------------------------------------
    // Default values
    // -------------------------------------------------------------------------

    @Test
    void build_defaultOutputMode_isAppend() {
        assertEquals(OutputMode.APPEND, minimalConfig().getOutputMode());
    }

    @Test
    void build_defaultTrigger_isProcessingTime30Seconds() {
        TriggerConfig trigger = minimalConfig().getTrigger();
        assertInstanceOf(TriggerConfig.ProcessingTime.class, trigger);
        assertEquals(Duration.ofSeconds(30),
            ((TriggerConfig.ProcessingTime) trigger).interval());
    }

    @Test
    void build_defaultClusteringColumns_isEmpty() {
        assertTrue(minimalConfig().getClusteringColumns().isEmpty());
    }

    @Test
    void build_defaultMaxRecordsPerFile_is5000000() {
        assertEquals(5_000_000L, minimalConfig().getMaxRecordsPerFile());
    }

    @Test
    void build_defaultExtraOptions_isEmpty() {
        assertTrue(minimalConfig().getExtraOptions().isEmpty());
    }

    // -------------------------------------------------------------------------
    // Optional field overrides
    // -------------------------------------------------------------------------

    @Test
    void build_customOutputMode_isStored() {
        DeltaStreamWriterConfig config = DeltaStreamWriterConfig.builder()
            .destinationPath("/tmp/dest")
            .checkpointBasePath("/tmp/chk")
            .checkpointName("s")
            .outputMode(OutputMode.COMPLETE)
            .build();
        assertEquals(OutputMode.COMPLETE, config.getOutputMode());
    }

    @Test
    void build_customTrigger_once_isStored() {
        DeltaStreamWriterConfig config = DeltaStreamWriterConfig.builder()
            .destinationPath("/tmp/dest")
            .checkpointBasePath("/tmp/chk")
            .checkpointName("s")
            .trigger(new TriggerConfig.Once())
            .build();
        assertInstanceOf(TriggerConfig.Once.class, config.getTrigger());
    }

    @Test
    void build_customTrigger_availableNow_isStored() {
        DeltaStreamWriterConfig config = DeltaStreamWriterConfig.builder()
            .destinationPath("/tmp/dest")
            .checkpointBasePath("/tmp/chk")
            .checkpointName("s")
            .trigger(new TriggerConfig.AvailableNow())
            .build();
        assertInstanceOf(TriggerConfig.AvailableNow.class, config.getTrigger());
    }

    @Test
    void build_customTrigger_continuous_isStored() {
        DeltaStreamWriterConfig config = DeltaStreamWriterConfig.builder()
            .destinationPath("/tmp/dest")
            .checkpointBasePath("/tmp/chk")
            .checkpointName("s")
            .trigger(new TriggerConfig.Continuous(Duration.ofSeconds(10)))
            .build();
        assertInstanceOf(TriggerConfig.Continuous.class, config.getTrigger());
        assertEquals(Duration.ofSeconds(10),
            ((TriggerConfig.Continuous) config.getTrigger()).checkpointInterval());
    }

    @Test
    void build_customMaxRecordsPerFile_isStored() {
        DeltaStreamWriterConfig config = DeltaStreamWriterConfig.builder()
            .destinationPath("/tmp/dest")
            .checkpointBasePath("/tmp/chk")
            .checkpointName("s")
            .maxRecordsPerFile(1_000_000L)
            .build();
        assertEquals(1_000_000L, config.getMaxRecordsPerFile());
    }

    @Test
    void build_extraOption_isStored() {
        DeltaStreamWriterConfig config = DeltaStreamWriterConfig.builder()
            .destinationPath("/tmp/dest")
            .checkpointBasePath("/tmp/chk")
            .checkpointName("s")
            .extraOption("myKey", "myValue")
            .build();
        assertEquals("myValue", config.getExtraOptions().get("myKey"));
    }

    @Test
    void build_multipleExtraOptions_allStored() {
        DeltaStreamWriterConfig config = DeltaStreamWriterConfig.builder()
            .destinationPath("/tmp/dest")
            .checkpointBasePath("/tmp/chk")
            .checkpointName("s")
            .extraOption("key1", "value1")
            .extraOption("key2", "value2")
            .build();
        Map<String, String> opts = config.getExtraOptions();
        assertEquals("value1", opts.get("key1"));
        assertEquals("value2", opts.get("key2"));
    }

    // -------------------------------------------------------------------------
    // Immutability
    // -------------------------------------------------------------------------

    @Test
    void build_clusteringColumnsAreCopied_mutationDoesNotAffectConfig() {
        DeltaStreamWriterConfig config = DeltaStreamWriterConfig.builder()
            .destinationPath("/tmp/dest")
            .checkpointBasePath("/tmp/chk")
            .checkpointName("s")
            .clusteringColumns(List.of("col1"))
            .build();
        assertThrows(UnsupportedOperationException.class, () ->
            config.getClusteringColumns().add("col2"));
    }

    @Test
    void build_extraOptionsAreCopied_mutationDoesNotAffectConfig() {
        DeltaStreamWriterConfig config = DeltaStreamWriterConfig.builder()
            .destinationPath("/tmp/dest")
            .checkpointBasePath("/tmp/chk")
            .checkpointName("s")
            .extraOption("key", "original")
            .build();
        assertThrows(UnsupportedOperationException.class, () ->
            config.getExtraOptions().put("key", "mutated"));
        assertEquals("original", config.getExtraOptions().get("key"));
    }

    @Test
    void build_clusteringColumnsInputListMutation_doesNotAffectConfig() {
        List<String> source = new java.util.ArrayList<>(List.of("col1", "col2"));
        DeltaStreamWriterConfig config = DeltaStreamWriterConfig.builder()
            .destinationPath("/tmp/dest")
            .checkpointBasePath("/tmp/chk")
            .checkpointName("s")
            .clusteringColumns(source)
            .build();
        source.add("col3");
        assertEquals(2, config.getClusteringColumns().size());
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private Set<String> violationPaths(ConstraintViolationException ex) {
        return ex.getConstraintViolations().stream()
            .map(v -> v.getPropertyPath().toString())
            .collect(Collectors.toSet());
    }

    private DeltaStreamWriterConfig minimalConfig() {
        return DeltaStreamWriterConfig.builder()
            .destinationPath("/tmp/dest")
            .checkpointBasePath("/tmp/checkpoints")
            .checkpointName("stream-1")
            .build();
    }
}
