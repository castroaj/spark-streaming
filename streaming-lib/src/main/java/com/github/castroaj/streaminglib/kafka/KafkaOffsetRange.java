package com.github.castroaj.streaminglib.kafka;

import java.util.Map;
import java.util.Objects;

/**
 * Immutable value object describing a bounded offset range for a single Kafka topic.
 *
 * <p>Used with {@link KafkaSource#readBatch(KafkaOffsetRange)} to perform bounded reads
 * for backfill or replay scenarios. Maps to the Spark Kafka {@code assign} option with
 * separate {@code startingOffsets} and {@code endingOffsets} JSON payloads.
 *
 * <p>Instances are immutable and thread-safe.
 *
 * @param topic        the Kafka topic name
 * @param fromOffsets  map of partition to inclusive start offset
 * @param untilOffsets map of partition to exclusive end offset
 */
public record KafkaOffsetRange(
        String topic,
        Map<Integer, Long> fromOffsets,
        Map<Integer, Long> untilOffsets) {

    /** Compact canonical constructor — validates and defensively copies the maps. */
    public KafkaOffsetRange {
        Objects.requireNonNull(topic, "topic must not be null");
        Objects.requireNonNull(fromOffsets, "fromOffsets must not be null");
        Objects.requireNonNull(untilOffsets, "untilOffsets must not be null");
        fromOffsets = Map.copyOf(fromOffsets);
        untilOffsets = Map.copyOf(untilOffsets);
    }

    /**
     * Creates a new {@code KafkaOffsetRange} for the given topic and partition offset maps.
     *
     * @param topic        the Kafka topic name
     * @param fromOffsets  map of partition to inclusive start offset
     * @param untilOffsets map of partition to exclusive end offset
     * @return an immutable offset range
     */
    public static KafkaOffsetRange of(String topic,
                                      Map<Integer, Long> fromOffsets,
                                      Map<Integer, Long> untilOffsets) {
        return new KafkaOffsetRange(topic, fromOffsets, untilOffsets);
    }
}
