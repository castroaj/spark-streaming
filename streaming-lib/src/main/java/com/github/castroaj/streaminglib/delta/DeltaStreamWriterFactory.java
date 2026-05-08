package com.github.castroaj.streaminglib.delta;

import java.util.Objects;

import lombok.experimental.UtilityClass;
import org.apache.spark.sql.SparkSession;

import com.github.castroaj.streaminglib.util.ValidationUtils;

/**
 * Factory for creating {@link DeltaStreamWriter} instances.
 *
 * <p>This is the sole public entry point for obtaining a {@code DeltaStreamWriter}.
 * The caller provides a live {@link SparkSession} and a fully-built {@link DeltaStreamWriterConfig};
 * the library never creates or configures a {@code SparkSession} internally.
 */
@UtilityClass
public final class DeltaStreamWriterFactory {

    /**
     * Creates a new {@link DeltaStreamWriter} bound to the given Spark session and configuration.
     *
     * @param spark  the active SparkSession; must not be null
     * @param config the Delta stream writer configuration; must not be null
     * @return a ready-to-use {@link DeltaStreamWriter}
     * @throws NullPointerException if {@code spark} or {@code config} is null
     * @throws jakarta.validation.ConstraintViolationException if {@code config} violates any constraint
     */
    public static DeltaStreamWriter create(SparkSession spark, DeltaStreamWriterConfig config) {
        Objects.requireNonNull(spark, "spark must not be null");
        Objects.requireNonNull(config, "config must not be null");
        ValidationUtils.validate(config);
        return new DefaultDeltaStreamWriter(config);
    }
}
