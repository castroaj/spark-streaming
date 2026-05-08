package com.github.castroaj.streaminglib.kafka;

/**
 * Controls the starting position for a Kafka consumer when no committed offset exists.
 *
 * <p>Maps to the {@code startingOffsets} Spark-Kafka source option.
 */
public enum StartingOffsets {

    /** Begin reading from the earliest available offset in each partition. */
    EARLIEST("earliest"),

    /** Begin reading from the latest offset, skipping messages produced before consumer start. */
    LATEST("latest");

    private final String value;

    StartingOffsets(String value) {
        this.value = value;
    }

    /**
     * Returns the string value passed to the Spark Kafka source option.
     *
     * @return the Spark-compatible string representation of this offset position
     */
    public String getValue() {
        return value;
    }
}
