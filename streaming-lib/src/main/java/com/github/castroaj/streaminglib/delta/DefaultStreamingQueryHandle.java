package com.github.castroaj.streaminglib.delta;

import java.time.Duration;
import java.util.Optional;

import org.apache.spark.sql.streaming.StreamingQueryStatus;

import com.github.castroaj.streaminglib.exception.DeltaStreamException;

import lombok.extern.slf4j.Slf4j;

/**
 * Package-private default implementation of {@link StreamingQueryHandle}.
 *
 * <p>Wraps a live Spark {@link org.apache.spark.sql.streaming.StreamingQuery} and delegates all
 * lifecycle calls to it. Obtain instances via {@link DefaultDeltaStreamWriter#start}.
 */
@Slf4j
final class DefaultStreamingQueryHandle implements StreamingQueryHandle {

    private final org.apache.spark.sql.streaming.StreamingQuery query;

    DefaultStreamingQueryHandle(org.apache.spark.sql.streaming.StreamingQuery query) {
        this.query = query;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isActive() {
        return query.isActive();
    }

    /** {@inheritDoc} */
    @Override
    public void awaitTermination() {
        try {
            query.awaitTermination();
        } catch (org.apache.spark.sql.streaming.StreamingQueryException ex) {
            throw new DeltaStreamException(
                "Streaming query terminated with exception — " + ex.getMessage(), ex);
        }
    }

    /** {@inheritDoc} */
    @Override
    public boolean awaitTermination(Duration timeout) {
        try {
            return query.awaitTermination(timeout.toMillis());
        } catch (org.apache.spark.sql.streaming.StreamingQueryException ex) {
            throw new DeltaStreamException(
                "Streaming query terminated with exception — " + ex.getMessage(), ex);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void stop() {
        try {
            query.stop();
        } catch (Exception ex) {
            throw new DeltaStreamException(
                "Failed to stop streaming query — " + ex.getMessage(), ex);
        }
    }

    /** {@inheritDoc} */
    @Override
    public StreamingQueryStatus status() {
        return query.status();
    }

    /** {@inheritDoc} */
    @Override
    public Optional<Throwable> lastException() {
        scala.Option<org.apache.spark.sql.streaming.StreamingQueryException> scalaOpt =
            query.exception();
        if (scalaOpt.isDefined()) {
            return Optional.of(scalaOpt.get());
        }
        return Optional.empty();
    }
}
