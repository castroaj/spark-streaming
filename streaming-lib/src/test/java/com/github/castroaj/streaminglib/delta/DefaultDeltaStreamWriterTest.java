package com.github.castroaj.streaminglib.delta;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DefaultDeltaStreamWriterTest {

    private SparkSession mockSpark;
    private Dataset<Row> mockDataset;
    private org.apache.spark.sql.streaming.DataStreamWriter<Row> mockWriter;
    private StreamingQuery mockQuery;
    private DeltaStreamWriterConfig baseConfig;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() throws Exception {
        mockSpark = mock(SparkSession.class);
        mockDataset = mock(Dataset.class);
        mockWriter = mock(org.apache.spark.sql.streaming.DataStreamWriter.class);
        mockQuery = mock(StreamingQuery.class);

        when(mockDataset.writeStream()).thenReturn(mockWriter);
        when(mockWriter.format(anyString())).thenReturn(mockWriter);
        when(mockWriter.outputMode(
            any(org.apache.spark.sql.streaming.OutputMode.class))).thenReturn(mockWriter);
        when(mockWriter.option(anyString(), anyString())).thenReturn(mockWriter);
        when(mockWriter.option(anyString(), anyLong())).thenReturn(mockWriter);
        when(mockWriter.trigger(any())).thenReturn(mockWriter);
        when(mockWriter.start(anyString())).thenReturn(mockQuery);

        when(mockQuery.exception()).thenReturn(scala.Option.empty());
        doReturn(true).when(mockQuery).awaitTermination(anyLong());

        baseConfig = DeltaStreamWriterConfig.builder()
            .destinationPath("/tmp/dest")
            .checkpointBasePath("/tmp/chk")
            .checkpointName("stream-1")
            .build();
    }

    // -------------------------------------------------------------------------
    // Post-close guards
    // -------------------------------------------------------------------------

    @Test
    void start_afterClose_throwsIllegalStateException() {
        DeltaStreamWriter writer = DeltaStreamWriterFactory.create(mockSpark, baseConfig);
        writer.close();
        assertThrows(IllegalStateException.class, () -> writer.start(mockDataset));
    }

    @Test
    void startAndAwait_afterClose_throwsIllegalStateException() {
        DeltaStreamWriter writer = DeltaStreamWriterFactory.create(mockSpark, baseConfig);
        writer.close();
        assertThrows(IllegalStateException.class,
            () -> writer.startAndAwait(mockDataset, Duration.ofSeconds(5)));
    }

    // -------------------------------------------------------------------------
    // Null parameter checks
    // -------------------------------------------------------------------------

    @Test
    void start_nullDataset_throwsNullPointerException() {
        DeltaStreamWriter writer = DeltaStreamWriterFactory.create(mockSpark, baseConfig);
        assertThrows(NullPointerException.class, () -> writer.start(null));
    }

    @Test
    void startAndAwait_nullDataset_throwsNullPointerException() {
        DeltaStreamWriter writer = DeltaStreamWriterFactory.create(mockSpark, baseConfig);
        assertThrows(NullPointerException.class,
            () -> writer.startAndAwait(null, Duration.ofSeconds(5)));
    }

    @Test
    void startAndAwait_nullTimeout_throwsNullPointerException() {
        DeltaStreamWriter writer = DeltaStreamWriterFactory.create(mockSpark, baseConfig);
        assertThrows(NullPointerException.class, () -> writer.startAndAwait(mockDataset, null));
    }

    // -------------------------------------------------------------------------
    // Format
    // -------------------------------------------------------------------------

    @Test
    void start_setsFormatDelta() {
        DeltaStreamWriterFactory.create(mockSpark, baseConfig).start(mockDataset);
        verify(mockWriter).format("delta");
    }

    // -------------------------------------------------------------------------
    // Checkpoint location
    // -------------------------------------------------------------------------

    @Test
    void start_setsCheckpointLocationOption() {
        DeltaStreamWriterFactory.create(mockSpark, baseConfig).start(mockDataset);
        verify(mockWriter).option("checkpointLocation", "/tmp/chk/stream-1");
    }

    // -------------------------------------------------------------------------
    // maxRecordsPerFile
    // -------------------------------------------------------------------------

    @Test
    void start_setsMaxRecordsPerFileOption() {
        DeltaStreamWriterFactory.create(mockSpark, baseConfig).start(mockDataset);
        verify(mockWriter).option("maxRecordsPerFile", 5_000_000L);
    }

    // -------------------------------------------------------------------------
    // Output mode mapping
    // -------------------------------------------------------------------------

    @Test
    void start_defaultOutputMode_usesAppend() {
        DeltaStreamWriterFactory.create(mockSpark, baseConfig).start(mockDataset);
        verify(mockWriter).outputMode(org.apache.spark.sql.streaming.OutputMode.Append());
    }

    @Test
    void start_completeModeConfig_usesComplete() {
        DeltaStreamWriterConfig config = DeltaStreamWriterConfig.builder()
            .destinationPath("/tmp/dest")
            .checkpointBasePath("/tmp/chk")
            .checkpointName("stream-1")
            .outputMode(OutputMode.COMPLETE)
            .build();
        DeltaStreamWriterFactory.create(mockSpark, config).start(mockDataset);
        verify(mockWriter).outputMode(org.apache.spark.sql.streaming.OutputMode.Complete());
    }

    @Test
    void start_updateModeConfig_usesUpdate() {
        DeltaStreamWriterConfig config = DeltaStreamWriterConfig.builder()
            .destinationPath("/tmp/dest")
            .checkpointBasePath("/tmp/chk")
            .checkpointName("stream-1")
            .outputMode(OutputMode.UPDATE)
            .build();
        DeltaStreamWriterFactory.create(mockSpark, config).start(mockDataset);
        verify(mockWriter).outputMode(org.apache.spark.sql.streaming.OutputMode.Update());
    }

    // -------------------------------------------------------------------------
    // Trigger mapping
    // -------------------------------------------------------------------------

    @Test
    void start_defaultTrigger_usesProcessingTime30Seconds() {
        DeltaStreamWriterFactory.create(mockSpark, baseConfig).start(mockDataset);
        verify(mockWriter).trigger(
            org.apache.spark.sql.streaming.Trigger.ProcessingTime(30_000L, TimeUnit.MILLISECONDS));
    }

    @Test
    @SuppressWarnings("deprecation")
    void start_onceTrigger_usesTriggerOnce() {
        DeltaStreamWriterConfig config = DeltaStreamWriterConfig.builder()
            .destinationPath("/tmp/dest")
            .checkpointBasePath("/tmp/chk")
            .checkpointName("stream-1")
            .trigger(new TriggerConfig.Once())
            .build();
        DeltaStreamWriterFactory.create(mockSpark, config).start(mockDataset);
        verify(mockWriter).trigger(org.apache.spark.sql.streaming.Trigger.Once());
    }

    @Test
    void start_availableNowTrigger_usesTriggerAvailableNow() {
        DeltaStreamWriterConfig config = DeltaStreamWriterConfig.builder()
            .destinationPath("/tmp/dest")
            .checkpointBasePath("/tmp/chk")
            .checkpointName("stream-1")
            .trigger(new TriggerConfig.AvailableNow())
            .build();
        DeltaStreamWriterFactory.create(mockSpark, config).start(mockDataset);
        verify(mockWriter).trigger(org.apache.spark.sql.streaming.Trigger.AvailableNow());
    }

    @Test
    void start_continuousTrigger_usesContinuousTrigger() {
        DeltaStreamWriterConfig config = DeltaStreamWriterConfig.builder()
            .destinationPath("/tmp/dest")
            .checkpointBasePath("/tmp/chk")
            .checkpointName("stream-1")
            .trigger(new TriggerConfig.Continuous(Duration.ofSeconds(10)))
            .build();
        DeltaStreamWriterFactory.create(mockSpark, config).start(mockDataset);
        verify(mockWriter).trigger(
            org.apache.spark.sql.streaming.Trigger.Continuous(10_000L, TimeUnit.MILLISECONDS));
    }

    // -------------------------------------------------------------------------
    // Clustering columns
    // -------------------------------------------------------------------------

    @Test
    void start_noClusteringColumns_doesNotSetClusterByColumnsOption() {
        DeltaStreamWriterFactory.create(mockSpark, baseConfig).start(mockDataset);
        verify(mockWriter, never()).option(eq("clusterByColumns"), anyString());
    }

    @Test
    void start_withClusteringColumns_setsClusterByColumnsOption() {
        DeltaStreamWriterConfig config = DeltaStreamWriterConfig.builder()
            .destinationPath("/tmp/dest")
            .checkpointBasePath("/tmp/chk")
            .checkpointName("stream-1")
            .clusteringColumns(List.of("date", "region"))
            .build();
        DeltaStreamWriterFactory.create(mockSpark, config).start(mockDataset);
        verify(mockWriter).option("clusterByColumns", "date,region");
    }

    // -------------------------------------------------------------------------
    // Extra options
    // -------------------------------------------------------------------------

    @Test
    void start_withExtraOptions_appliesExtraOptions() {
        DeltaStreamWriterConfig config = DeltaStreamWriterConfig.builder()
            .destinationPath("/tmp/dest")
            .checkpointBasePath("/tmp/chk")
            .checkpointName("stream-1")
            .extraOption("myKey", "myValue")
            .build();
        DeltaStreamWriterFactory.create(mockSpark, config).start(mockDataset);
        verify(mockWriter).option("myKey", "myValue");
    }

    // -------------------------------------------------------------------------
    // Destination path
    // -------------------------------------------------------------------------

    @Test
    void start_callsStartWithDestinationPath() {
        DeltaStreamWriterFactory.create(mockSpark, baseConfig).start(mockDataset);
        verify(mockWriter).start("/tmp/dest");
    }

    // -------------------------------------------------------------------------
    // Return value
    // -------------------------------------------------------------------------

    @Test
    void start_returnsStreamingQueryHandle() {
        StreamingQueryHandle handle =
            DeltaStreamWriterFactory.create(mockSpark, baseConfig).start(mockDataset);
        assertNotNull(handle);
    }

    // -------------------------------------------------------------------------
    // startAndAwait delegation
    // -------------------------------------------------------------------------

    @Test
    void startAndAwait_callsAwaitTerminationWithTimeout() throws Exception {
        DeltaStreamWriterFactory.create(mockSpark, baseConfig)
            .startAndAwait(mockDataset, Duration.ofSeconds(3));
        verify(mockQuery).awaitTermination(3_000L);
    }

    // -------------------------------------------------------------------------
    // close idempotency
    // -------------------------------------------------------------------------

    @Test
    void close_calledTwice_doesNotThrow() {
        DeltaStreamWriter writer = DeltaStreamWriterFactory.create(mockSpark, baseConfig);
        writer.close();
        writer.close();
    }
}
