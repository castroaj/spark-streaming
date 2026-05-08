package com.github.castroaj.streaminglib.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.Getter;

/**
 * Immutable configuration for a {@link KafkaSource}.
 *
 * <p>Constructed exclusively via the inner {@link Builder}. Required fields are validated
 * at {@link Builder#build()} time; missing fields cause an {@link IllegalArgumentException}
 * listing all absent fields in a single message, before any Spark or Kafka connection is made.
 *
 * <p>Instances are immutable and thread-safe.
 */
@Getter
public final class KafkaSourceConfig {

    private static final int DEFAULT_MAX_OFFSETS_PER_TRIGGER = 100_000;
    private static final Duration DEFAULT_POLL_TIMEOUT = Duration.ofSeconds(120);

    private final String bootstrapServers;
    private final String topic;
    private final String groupId;
    private final StartingOffsets startingOffsets;
    private final int maxOffsetsPerTrigger;
    private final Duration pollTimeout;
    private final Map<String, String> extraOptions;

    private KafkaSourceConfig(Builder builder) {
        this.bootstrapServers = builder.bootstrapServers;
        this.topic = builder.topic;
        this.groupId = builder.groupId;
        this.startingOffsets = builder.startingOffsets;
        this.maxOffsetsPerTrigger = builder.maxOffsetsPerTrigger;
        this.pollTimeout = builder.pollTimeout;
        this.extraOptions = Map.copyOf(builder.extraOptions);
    }

    /**
     * Returns a new {@link Builder} for constructing a {@code KafkaSourceConfig}.
     *
     * @return a fresh builder with default values applied for optional fields
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for {@link KafkaSourceConfig}.
     *
     * <p>Required fields: {@code bootstrapServers}, {@code topic}, {@code groupId}.
     * All other fields are optional with documented defaults.
     */
    public static final class Builder {

        private String bootstrapServers;
        private String topic;
        private String groupId;
        private StartingOffsets startingOffsets = StartingOffsets.EARLIEST;
        private int maxOffsetsPerTrigger = DEFAULT_MAX_OFFSETS_PER_TRIGGER;
        private Duration pollTimeout = DEFAULT_POLL_TIMEOUT;
        private final Map<String, String> extraOptions = new HashMap<>();

        private Builder() {
        }

        /**
         * Sets the Kafka bootstrap servers. Required.
         *
         * @param servers comma-separated host:port list (e.g., {@code "kafka:9092"})
         * @return this builder
         */
        public Builder bootstrapServers(String servers) {
            this.bootstrapServers = servers;
            return this;
        }

        /**
         * Sets the Kafka topic to subscribe to. Required.
         *
         * @param topicName name of the topic
         * @return this builder
         */
        public Builder topic(String topicName) {
            this.topic = topicName;
            return this;
        }

        /**
         * Sets the consumer group ID prefix. Required.
         *
         * @param groupIdValue the consumer group identifier
         * @return this builder
         */
        public Builder groupId(String groupIdValue) {
            this.groupId = groupIdValue;
            return this;
        }

        /**
         * Sets the starting offset policy. Optional; defaults to {@link StartingOffsets#EARLIEST}.
         *
         * @param offsets the starting offset policy
         * @return this builder
         */
        public Builder startingOffsets(StartingOffsets offsets) {
            this.startingOffsets = offsets;
            return this;
        }

        /**
         * Sets the maximum number of offsets consumed per micro-batch. Optional; defaults to 100,000.
         *
         * @param max positive integer limit
         * @return this builder
         */
        public Builder maxOffsetsPerTrigger(int max) {
            this.maxOffsetsPerTrigger = max;
            return this;
        }

        /**
         * Sets the Kafka consumer poll timeout. Optional; defaults to 120 seconds.
         *
         * @param timeout positive duration for each poll cycle
         * @return this builder
         */
        public Builder pollTimeout(Duration timeout) {
            this.pollTimeout = timeout;
            return this;
        }

        /**
         * Adds a single raw Spark-Kafka source option. Optional; accumulated across multiple calls.
         *
         * <p>Extra options are applied after all standard options and may override library defaults.
         *
         * @param key   the Spark-Kafka option key
         * @param value the option value
         * @return this builder
         */
        public Builder extraOption(String key, String value) {
            this.extraOptions.put(key, value);
            return this;
        }

        /**
         * Builds and returns an immutable {@link KafkaSourceConfig}.
         *
         * <p>Required fields: {@code bootstrapServers}, {@code topic}, {@code groupId}.
         *
         * @return a validated, immutable config
         * @throws IllegalArgumentException if any required fields are missing, listing all absent fields
         */
        public KafkaSourceConfig build() {
            List<String> missing = new ArrayList<>();
            if (bootstrapServers == null || bootstrapServers.isBlank()) {
                missing.add("bootstrapServers");
            }
            if (topic == null || topic.isBlank()) {
                missing.add("topic");
            }
            if (groupId == null || groupId.isBlank()) {
                missing.add("groupId");
            }
            if (!missing.isEmpty()) {
                throw new IllegalArgumentException(
                    "KafkaSourceConfig is missing required fields: " + String.join(", ", missing));
            }
            return new KafkaSourceConfig(this);
        }
    }
}
