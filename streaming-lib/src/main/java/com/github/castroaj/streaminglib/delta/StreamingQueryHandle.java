package com.github.castroaj.streaminglib.delta;

import java.time.Duration;
import java.util.Optional;

import org.apache.spark.sql.streaming.StreamingQueryStatus;

/**
 * A handle to a running Spark {@link org.apache.spark.sql.streaming.StreamingQuery}.
 *
 * <p>Obtained by calling {@link DeltaStreamWriter#start} or {@link DeltaStreamWriter#startAndAwait}.
 * The underlying query is managed by the Spark runtime; this handle provides a typed, library-level
 * view of its lifecycle.
 *
 * <p>Instances are not thread-safe unless noted otherwise.
 */
public interface StreamingQueryHandle {

    /**
     * Returns {@code true} if the underlying streaming query is currently running.
     *
     * @return {@code true} if active, {@code false} if stopped or terminated
     */
    boolean isActive();

    /**
     * Blocks indefinitely until the streaming query terminates.
     *
     * @throws DeltaStreamException if the query terminates with an error
     */
    void awaitTermination();

    /**
     * Blocks until the streaming query terminates or the given timeout elapses.
     *
     * @param timeout maximum time to wait; must not be null
     * @return {@code true} if the query terminated within the timeout, {@code false} otherwise
     * @throws DeltaStreamException if the query terminates with an error
     */
    boolean awaitTermination(Duration timeout);

    /**
     * Requests a graceful stop of the streaming query.
     *
     * @throws DeltaStreamException if the stop operation fails
     */
    void stop();

    /**
     * Returns the current status of the streaming query.
     *
     * @return a {@link StreamingQueryStatus} snapshot from the Spark runtime
     */
    StreamingQueryStatus status();

    /**
     * Returns the terminal exception if the query ended due to an error, or
     * {@link Optional#empty()} if the query completed normally or is still running.
     *
     * @return an {@code Optional} containing the cause of failure, or empty if none
     */
    Optional<Throwable> lastException();
}
