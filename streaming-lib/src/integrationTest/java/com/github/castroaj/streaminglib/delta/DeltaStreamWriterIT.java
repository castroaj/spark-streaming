package com.github.castroaj.streaminglib.delta;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.castroaj.streaminglib.testing.SparkTestSession;
import java.io.File;
import java.nio.file.Path;
import java.time.Duration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Integration tests for {@link DeltaStreamWriter} against a real local Spark session with Delta.
 *
 * <p>Expected wall-clock time: ~20–40 seconds (Spark local mode init + Delta write).
 */
@Tag("integration")
class DeltaStreamWriterIT {

    @TempDir
    Path tempDir;

    private static SparkSession spark;

    @BeforeAll
    static void setUpSpark() {
        spark = SparkTestSession.create("DeltaStreamWriterIT");
    }

    @AfterAll
    static void tearDownSpark() {
        if (spark != null) {
            spark.close();
        }
    }

    @Test
    void startAndAwait_rateSource_writesDeltaLogToDestination() {
        String destPath = tempDir.resolve("delta-output").toString();
        String checkpointPath = tempDir.resolve("checkpoints").toString();

        DeltaStreamWriterConfig config = DeltaStreamWriterConfig.builder()
            .destinationPath(destPath)
            .checkpointBasePath(checkpointPath)
            .checkpointName("rate-stream")
            .trigger(new TriggerConfig.AvailableNow())
            .build();

        DeltaStreamWriter writer = DeltaStreamWriterFactory.create(spark, config);
        try {
            Dataset<Row> source = spark.readStream()
                .format("rate")
                .option("rowsPerSecond", 10)
                .load();

            writer.startAndAwait(source, Duration.ofSeconds(30));

            File deltaLog = new File(destPath, "_delta_log");
            assertTrue(deltaLog.exists() && deltaLog.isDirectory(),
                "_delta_log directory must exist after a successful write");
            File[] logFiles = deltaLog.listFiles();
            assertNotNull(logFiles, "_delta_log must be listable");
            assertTrue(logFiles.length > 0,
                "_delta_log must contain at least one commit file");
        } finally {
            writer.close();
        }
    }

    @Test
    void start_rateSource_isActiveAndCanBeStopped() {
        String destPath = tempDir.resolve("delta-active").toString();
        String checkpointPath = tempDir.resolve("checkpoints-active").toString();

        DeltaStreamWriterConfig config = DeltaStreamWriterConfig.builder()
            .destinationPath(destPath)
            .checkpointBasePath(checkpointPath)
            .checkpointName("active-stream")
            .trigger(new TriggerConfig.ProcessingTime(Duration.ofSeconds(1)))
            .build();

        DeltaStreamWriter writer = DeltaStreamWriterFactory.create(spark, config);
        try {
            Dataset<Row> source = spark.readStream()
                .format("rate")
                .option("rowsPerSecond", 5)
                .load();

            StreamingQueryHandle handle = writer.start(source);
            assertTrue(handle.isActive(), "Handle must report active immediately after start");
            handle.stop();
        } finally {
            writer.close();
        }
    }
}
