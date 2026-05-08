package com.github.castroaj.streaminglib.kafka;

import java.util.Map;
import java.util.Objects;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import com.github.castroaj.streaminglib.util.ValidationUtils;

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
        @NotBlank String topic,
        @NotNull @NotEmpty Map<Integer, Long> fromOffsets,
        @NotNull @NotEmpty Map<Integer, Long> untilOffsets) {

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
     * @return an immutable, validated offset range
     * @throws jakarta.validation.ConstraintViolationException if any argument violates a constraint
     */
    public static KafkaOffsetRange of(String topic,
                                      Map<Integer, Long> fromOffsets,
                                      Map<Integer, Long> untilOffsets) {
        KafkaOffsetRange range = new KafkaOffsetRange(topic, fromOffsets, untilOffsets);
        ValidationUtils.validate(range);
        return range;
    }
}
