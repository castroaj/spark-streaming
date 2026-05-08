package com.github.castroaj.streaminglib.kafka;

import java.util.Map;
import java.util.Objects;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamReader;

import com.github.castroaj.streaminglib.exception.KafkaSourceException;

import lombok.extern.slf4j.Slf4j;

/**
 * Package-private default implementation of {@link KafkaSource}.
 *
 * <p>Wraps Spark Structured Streaming's native Kafka source connector.
 * Obtain instances via {@link KafkaSourceFactory#create}.
 */
@Slf4j
final class DefaultKafkaSource implements KafkaSource {

    private final SparkSession spark;
    private final KafkaSourceConfig config;
    private volatile boolean closed = false;

    DefaultKafkaSource(SparkSession spark, KafkaSourceConfig config) {
        this.spark = spark;
        this.config = config;
    }

    /** {@inheritDoc} */
    @Override
    public Dataset<Row> readStream() {
        checkNotClosed();
        log.debug("Building streaming reader for topic '{}' at '{}'",
            config.getTopic(), config.getBootstrapServers());
        try {
            DataStreamReader reader = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", config.getBootstrapServers())
                .option("subscribe", config.getTopic())
                .option("groupIdPrefix", config.getGroupId())
                .option("startingOffsets", config.getStartingOffsets().getValue())
                .option("maxOffsetsPerTrigger", config.getMaxOffsetsPerTrigger())
                .option("kafkaConsumer.pollTimeoutMs",
                    String.valueOf(config.getPollTimeout().toMillis()));
            for (Map.Entry<String, String> entry : config.getExtraOptions().entrySet()) {
                reader = reader.option(entry.getKey(), entry.getValue());
            }
            return reader.load();
        } catch (Exception ex) {
            throw new KafkaSourceException(
                "Failed to start streaming read on topic '"
                    + config.getTopic() + "' at " + config.getBootstrapServers()
                    + " — " + ex.getMessage(),
                ex);
        }
    }

    /** {@inheritDoc} */
    @Override
    public Dataset<Row> readBatch(KafkaOffsetRange range) {
        Objects.requireNonNull(range, "range must not be null");
        checkNotClosed();
        log.debug("Building batch reader for topic '{}' at '{}'",
            config.getTopic(), config.getBootstrapServers());
        try {
            String assignJson = buildAssignJson(range);
            String startingOffsetsJson = buildOffsetsJson(range.topic(), range.fromOffsets());
            String endingOffsetsJson = buildOffsetsJson(range.topic(), range.untilOffsets());
            return spark.read()
                .format("kafka")
                .option("kafka.bootstrap.servers", config.getBootstrapServers())
                .option("assign", assignJson)
                .option("startingOffsets", startingOffsetsJson)
                .option("endingOffsets", endingOffsetsJson)
                .load();
        } catch (Exception ex) {
            throw new KafkaSourceException(
                "Failed to start batch read on topic '"
                    + config.getTopic() + "' at " + config.getBootstrapServers()
                    + " — " + ex.getMessage(),
                ex);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
        if (!closed) {
            closed = true;
            log.info("Closed KafkaSource for topic '{}' at '{}'",
                config.getTopic(), config.getBootstrapServers());
        }
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    private void checkNotClosed() {
        if (closed) {
            throw new IllegalStateException(
                "KafkaSource for topic '" + config.getTopic() + "' is closed");
        }
    }

    /**
     * Builds the {@code assign} JSON used to select specific topic-partitions.
     * Format: {"topic": [partition, ...]}
     */
    private String buildAssignJson(KafkaOffsetRange range) {
        StringBuilder sb = new StringBuilder("{\"").append(range.topic()).append("\":[");
        boolean first = true;
        for (Integer partition : range.fromOffsets().keySet()) {
            if (!first) {
                sb.append(',');
            }
            sb.append(partition);
            first = false;
        }
        sb.append("]}");
        return sb.toString();
    }

    /**
     * Builds the per-partition offset JSON.
     * Format: {"topic": {"partition": offset, ...}}
     */
    private String buildOffsetsJson(String topic, Map<Integer, Long> offsets) {
        StringBuilder sb = new StringBuilder("{\"").append(topic).append("\":{");
        boolean first = true;
        for (Map.Entry<Integer, Long> entry : offsets.entrySet()) {
            if (!first) {
                sb.append(',');
            }
            sb.append('"').append(entry.getKey()).append("\":").append(entry.getValue());
            first = false;
        }
        sb.append("}}");
        return sb.toString();
    }
}
