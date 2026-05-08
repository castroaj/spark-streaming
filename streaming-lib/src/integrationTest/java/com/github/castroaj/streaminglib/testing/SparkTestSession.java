package com.github.castroaj.streaminglib.testing;

import org.apache.spark.sql.SparkSession;

/**
 * Shared utility for creating a local-mode {@link SparkSession} in integration tests.
 *
 * <p>Not part of the public API. For use in {@code src/integrationTest} source set only.
 */
public final class SparkTestSession {

    private SparkTestSession() {
    }

    /**
     * Creates a new local-mode SparkSession configured for integration testing.
     *
     * <p>Runs in {@code local[2]} mode with Delta Lake extensions enabled,
     * shuffle partitions set to 2, and the Spark UI disabled.
     *
     * @param appName the application name for the Spark context
     * @return a ready-to-use SparkSession
     */
    public static SparkSession create(String appName) {
        return SparkSession.builder()
            .master("local[2]")
            .appName(appName)
            .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.ui.enabled", "false")
            .getOrCreate();
    }
}
