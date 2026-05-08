package com.github.castroaj.streaminglib.kafka;

import java.util.Objects;

import org.apache.spark.sql.SparkSession;

import lombok.experimental.UtilityClass;

/**
 * Factory for creating {@link KafkaSource} instances.
 *
 * <p>This is the sole public entry point for obtaining a {@code KafkaSource}.
 * The caller provides a live {@link SparkSession} and a fully-built {@link KafkaSourceConfig};
 * the library never creates or configures a {@code SparkSession} internally.
 */
@UtilityClass
public final class KafkaSourceFactory {

    /**
     * Creates a new {@link KafkaSource} bound to the given Spark session and configuration.
     *
     * @param spark  the active SparkSession; must not be null
     * @param config the Kafka source configuration; must not be null
     * @return a ready-to-use {@link KafkaSource}
     * @throws IllegalArgumentException if {@code spark} or {@code config} is null
     */
    public static KafkaSource create(SparkSession spark, KafkaSourceConfig config) {
        Objects.requireNonNull(spark, "spark must not be null");
        Objects.requireNonNull(config, "config must not be null");
        return new DefaultKafkaSource(spark, config);
    }
}
