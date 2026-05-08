package com.github.castroaj.streaminglib.delta;

import java.time.Duration;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Abstraction over Spark Structured Streaming's {@code writeStream} API for Delta Lake.
 *
 * <p>Encapsulates checkpoint path construction, trigger configuration, output mode, clustering
 * columns, and file size targets. Callers provide a streaming {@code Dataset<Row>} and a
 * destination config; this abstraction manages the {@code StreamingQuery} lifecycle.
 *
 * <p>Obtain instances via {@link DeltaStreamWriterFactory#create}.
 *
 * <p>Instances are not thread-safe. {@link #close()} is idempotent; any method called after
 * {@code close()} throws {@link IllegalStateException}.
 */
public interface DeltaStreamWriter {

    /**
     * Starts the streaming write for the given dataset. Does not block.
     *
     * <p>The dataset must be a streaming {@code Dataset} (i.e., {@code dataset.isStreaming()} is
     * {@code true}). The write is configured using the {@link DeltaStreamWriterConfig} supplied at
     * construction time.
     *
     * @param streamingDataset the streaming source dataset; must not be null
     * @return a {@link StreamingQueryHandle} for monitoring and controlling the running query
     * @throws NullPointerException if {@code streamingDataset} is null
     * @throws IllegalStateException if this writer has been closed
     * @throws DeltaStreamException if the streaming query could not be started
     */
    StreamingQueryHandle start(Dataset<Row> streamingDataset);

    /**
     * Starts the streaming write and blocks until the query terminates or the timeout elapses.
     *
     * <p>Equivalent to calling {@link #start} followed by {@link StreamingQueryHandle#awaitTermination(Duration)}.
     * The returned handle may already be inactive when this method returns.
     *
     * @param streamingDataset the streaming source dataset; must not be null
     * @param timeout          maximum time to wait for query termination; must not be null
     * @return a {@link StreamingQueryHandle} for the (possibly already-stopped) query
     * @throws NullPointerException if {@code streamingDataset} or {@code timeout} is null
     * @throws IllegalStateException if this writer has been closed
     * @throws DeltaStreamException if the query fails to start or terminates with an error
     */
    StreamingQueryHandle startAndAwait(Dataset<Row> streamingDataset, Duration timeout);

    /**
     * Releases resources held by this writer. Idempotent; safe to call multiple times.
     *
     * <p>After {@code close()} returns, any call to {@link #start} or {@link #startAndAwait}
     * will throw {@link IllegalStateException}.
     */
    void close();
}
