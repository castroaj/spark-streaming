package com.github.castroaj.streaminglib.delta;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;

import com.github.castroaj.streaminglib.exception.DeltaStreamException;

import lombok.extern.slf4j.Slf4j;

/**
 * Package-private default implementation of {@link DeltaStreamWriter}.
 *
 * <p>Wraps Spark Structured Streaming's {@code writeStream} API for Delta Lake.
 * Obtain instances via {@link DeltaStreamWriterFactory#create}.
 */
@Slf4j
final class DefaultDeltaStreamWriter implements DeltaStreamWriter {

    private final DeltaStreamWriterConfig config;
    private volatile boolean closed = false;

    DefaultDeltaStreamWriter(DeltaStreamWriterConfig config) {
        this.config = config;
    }

    /** {@inheritDoc} */
    @Override
    public StreamingQueryHandle start(Dataset<Row> streamingDataset) {
        Objects.requireNonNull(streamingDataset, "streamingDataset must not be null");
        checkNotClosed();
        String checkpointLocation = config.getCheckpointBasePath() + "/" + config.getCheckpointName();
        log.debug("Starting Delta streaming write to '{}' with checkpoint at '{}'",
            config.getDestinationPath(), checkpointLocation);
        try {
            DataStreamWriter<Row> writer = buildWriter(streamingDataset, checkpointLocation);
            org.apache.spark.sql.streaming.StreamingQuery query =
                writer.start(config.getDestinationPath());
            log.info("Started Delta streaming query for destination '{}'",
                config.getDestinationPath());
            return new DefaultStreamingQueryHandle(query);
        } catch (DeltaStreamException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new DeltaStreamException(
                "Failed to start streaming write to '"
                    + config.getDestinationPath() + "' — " + ex.getMessage(),
                ex);
        }
    }

    /** {@inheritDoc} */
    @Override
    public StreamingQueryHandle startAndAwait(Dataset<Row> streamingDataset, Duration timeout) {
        Objects.requireNonNull(streamingDataset, "streamingDataset must not be null");
        Objects.requireNonNull(timeout, "timeout must not be null");
        checkNotClosed();
        StreamingQueryHandle handle = start(streamingDataset);
        handle.awaitTermination(timeout);
        return handle;
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
        if (!closed) {
            closed = true;
            log.info("Closed DeltaStreamWriter for destination '{}'",
                config.getDestinationPath());
        }
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    private void checkNotClosed() {
        if (closed) {
            throw new IllegalStateException(
                "DeltaStreamWriter for '" + config.getDestinationPath() + "' is closed");
        }
    }

    private DataStreamWriter<Row> buildWriter(Dataset<Row> dataset, String checkpointLocation) {
        DataStreamWriter<Row> writer = dataset
            .writeStream()
            .format("delta")
            .outputMode(toSparkOutputMode(config.getOutputMode()))
            .option("checkpointLocation", checkpointLocation)
            .option("maxRecordsPerFile", config.getMaxRecordsPerFile())
            .trigger(toSparkTrigger(config.getTrigger()));

        if (!config.getClusteringColumns().isEmpty()) {
            writer = writer.option("clusterByColumns",
                String.join(",", config.getClusteringColumns()));
        }

        for (Map.Entry<String, String> entry : config.getExtraOptions().entrySet()) {
            writer = writer.option(entry.getKey(), entry.getValue());
        }
        return writer;
    }

    /**
     * Maps the library {@link OutputMode} enum to the corresponding Spark
     * {@code org.apache.spark.sql.streaming.OutputMode} factory method result.
     */
    private org.apache.spark.sql.streaming.OutputMode toSparkOutputMode(OutputMode mode) {
        return switch (mode) {
            case APPEND   -> org.apache.spark.sql.streaming.OutputMode.Append();
            case COMPLETE -> org.apache.spark.sql.streaming.OutputMode.Complete();
            case UPDATE   -> org.apache.spark.sql.streaming.OutputMode.Update();
        };
    }

    /**
     * Maps a {@link TriggerConfig} to the corresponding Spark
     * {@code org.apache.spark.sql.streaming.Trigger} instance.
     */
    @SuppressWarnings("deprecation")
    private org.apache.spark.sql.streaming.Trigger toSparkTrigger(TriggerConfig trigger) {
        if (trigger instanceof TriggerConfig.ProcessingTime pt) {
            return org.apache.spark.sql.streaming.Trigger.ProcessingTime(
                pt.interval().toMillis(), TimeUnit.MILLISECONDS);
        } else if (trigger instanceof TriggerConfig.Once) {
            return org.apache.spark.sql.streaming.Trigger.Once();
        } else if (trigger instanceof TriggerConfig.AvailableNow) {
            return org.apache.spark.sql.streaming.Trigger.AvailableNow();
        } else if (trigger instanceof TriggerConfig.Continuous ct) {
            return org.apache.spark.sql.streaming.Trigger.Continuous(
                ct.checkpointInterval().toMillis(), TimeUnit.MILLISECONDS);
        }
        throw new IllegalArgumentException("Unsupported TriggerConfig type: " + trigger.getClass().getName());
    }
}
