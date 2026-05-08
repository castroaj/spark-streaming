package com.github.castroaj.streaminglib.delta;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;

import com.github.castroaj.streaminglib.util.ValidationUtils;

/**
 * Immutable configuration for a {@link DeltaStreamWriter}.
 *
 * <p>Constructed exclusively via the inner {@link Builder}. Required fields are validated
 * at {@link Builder#build()} time via Jakarta Bean Validation; constraint violations are reported
 * as a {@link jakarta.validation.ConstraintViolationException} before any Spark interaction occurs.
 *
 * <p>Instances are immutable and thread-safe.
 */
@Getter
public final class DeltaStreamWriterConfig {

    private static final long DEFAULT_MAX_RECORDS_PER_FILE = 5_000_000L;
    private static final TriggerConfig DEFAULT_TRIGGER =
        new TriggerConfig.ProcessingTime(Duration.ofSeconds(30));

    @NotBlank
    private final String destinationPath;
    @NotBlank
    private final String checkpointBasePath;
    @NotBlank
    private final String checkpointName;
    @NotNull
    private final OutputMode outputMode;
    @NotNull
    private final TriggerConfig trigger;
    @NotNull
    private final List<String> clusteringColumns;
    private final long maxRecordsPerFile;
    @NotNull
    private final Map<String, String> extraOptions;

    private DeltaStreamWriterConfig(Builder builder) {
        this.destinationPath = builder.destinationPath;
        this.checkpointBasePath = builder.checkpointBasePath;
        this.checkpointName = builder.checkpointName;
        this.outputMode = builder.outputMode;
        this.trigger = builder.trigger;
        this.clusteringColumns = List.copyOf(builder.clusteringColumns);
        this.maxRecordsPerFile = builder.maxRecordsPerFile;
        this.extraOptions = Map.copyOf(builder.extraOptions);
    }

    /**
     * Returns a new {@link Builder} for constructing a {@code DeltaStreamWriterConfig}.
     *
     * @return a fresh builder with default values applied for optional fields
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for {@link DeltaStreamWriterConfig}.
     *
     * <p>Required fields: {@code destinationPath}, {@code checkpointBasePath}, {@code checkpointName}.
     * All other fields are optional with documented defaults.
     */
    public static final class Builder {

        private String destinationPath;
        private String checkpointBasePath;
        private String checkpointName;
        private OutputMode outputMode = OutputMode.APPEND;
        private TriggerConfig trigger = DEFAULT_TRIGGER;
        private final List<String> clusteringColumns = new ArrayList<>();
        private long maxRecordsPerFile = DEFAULT_MAX_RECORDS_PER_FILE;
        private final Map<String, String> extraOptions = new HashMap<>();

        private Builder() {
        }

        /**
         * Sets the Delta destination path where data will be written. Required.
         *
         * @param path the destination path (e.g., {@code "s3a://warehouse/stream_data"})
         * @return this builder
         */
        public Builder destinationPath(String path) {
            this.destinationPath = path;
            return this;
        }

        /**
         * Sets the base path under which checkpoint directories are created. Required.
         *
         * @param path the checkpoint base path (e.g., {@code "s3a://warehouse/_checkpoints"})
         * @return this builder
         */
        public Builder checkpointBasePath(String path) {
            this.checkpointBasePath = path;
            return this;
        }

        /**
         * Sets the checkpoint name, appended to {@code checkpointBasePath}. Required.
         *
         * <p>The effective checkpoint location will be {@code checkpointBasePath/checkpointName}.
         *
         * @param name a unique name for this streaming query's checkpoint directory
         * @return this builder
         */
        public Builder checkpointName(String name) {
            this.checkpointName = name;
            return this;
        }

        /**
         * Sets the output mode for the streaming write. Optional; defaults to {@link OutputMode#APPEND}.
         *
         * @param mode the output mode
         * @return this builder
         */
        public Builder outputMode(OutputMode mode) {
            this.outputMode = mode;
            return this;
        }

        /**
         * Sets the trigger policy for micro-batch execution. Optional;
         * defaults to {@code ProcessingTime(30 seconds)}.
         *
         * @param triggerConfig the trigger configuration
         * @return this builder
         */
        public Builder trigger(TriggerConfig triggerConfig) {
            this.trigger = triggerConfig;
            return this;
        }

        /**
         * Sets the clustering columns used for Delta Lake liquid clustering. Optional; defaults to empty.
         *
         * @param columns list of column names to cluster by
         * @return this builder
         */
        public Builder clusteringColumns(List<String> columns) {
            this.clusteringColumns.clear();
            this.clusteringColumns.addAll(columns);
            return this;
        }

        /**
         * Sets the maximum number of records written per output file. Optional; defaults to 5,000,000.
         *
         * @param max maximum records per file
         * @return this builder
         */
        public Builder maxRecordsPerFile(long max) {
            this.maxRecordsPerFile = max;
            return this;
        }

        /**
         * Adds a single raw Spark streaming write option. Optional; accumulated across multiple calls.
         *
         * <p>Extra options are applied after all standard options and may override library defaults.
         *
         * @param key   the DataStreamWriter option key
         * @param value the option value
         * @return this builder
         */
        public Builder extraOption(String key, String value) {
            this.extraOptions.put(key, value);
            return this;
        }

        /**
         * Builds and returns an immutable {@link DeltaStreamWriterConfig}.
         *
         * <p>Required fields: {@code destinationPath}, {@code checkpointBasePath}, {@code checkpointName}.
         *
         * @return a validated, immutable config
         * @throws jakarta.validation.ConstraintViolationException if any required field is null or blank
         */
        public DeltaStreamWriterConfig build() {
            DeltaStreamWriterConfig config = new DeltaStreamWriterConfig(this);
            ValidationUtils.validate(config);
            return config;
        }
    }
}
