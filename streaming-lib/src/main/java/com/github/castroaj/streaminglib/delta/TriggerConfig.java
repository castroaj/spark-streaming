package com.github.castroaj.streaminglib.delta;

import java.time.Duration;

/**
 * Sealed configuration type for Spark Structured Streaming trigger policies.
 *
 * <p>Each permitted subtype maps to a specific
 * {@link org.apache.spark.sql.streaming.Trigger} factory method:
 *
 * <ul>
 *   <li>{@link ProcessingTime} — {@code Trigger.ProcessingTime(...)}</li>
 *   <li>{@link Once} — {@code Trigger.Once()} (deprecated in Spark 3.3+; prefer {@link AvailableNow})</li>
 *   <li>{@link AvailableNow} — {@code Trigger.AvailableNow()}</li>
 *   <li>{@link Continuous} — {@code Trigger.Continuous(...)}</li>
 * </ul>
 *
 * <p>Use the nested record types to create instances:
 * <pre>{@code
 * TriggerConfig trigger = new TriggerConfig.ProcessingTime(Duration.ofSeconds(30));
 * TriggerConfig once    = new TriggerConfig.AvailableNow();
 * }</pre>
 */
public sealed interface TriggerConfig
        permits TriggerConfig.ProcessingTime,
                TriggerConfig.Once,
                TriggerConfig.AvailableNow,
                TriggerConfig.Continuous {

    /**
     * Micro-batch trigger that fires at a fixed time interval.
     * Maps to {@code Trigger.ProcessingTime(interval.toMillis(), TimeUnit.MILLISECONDS)}.
     *
     * @param interval time between successive micro-batch executions; must be positive
     */
    record ProcessingTime(Duration interval) implements TriggerConfig {}

    /**
     * One-shot trigger that processes all available data and then stops the query.
     * Maps to {@code Trigger.Once()}.
     *
     * <p><strong>Note:</strong> {@code Trigger.Once()} is deprecated in Spark 3.3+ in favour of
     * {@link AvailableNow}, which also runs a single burst but with improved semantics for
     * rate-limited sources. Both are supported by this library for compatibility.
     */
    record Once() implements TriggerConfig {}

    /**
     * Trigger that processes all available data in one or more micro-batches and then stops.
     * Maps to {@code Trigger.AvailableNow()}.
     *
     * <p>Preferred over {@link Once} for Spark 3.3+ deployments.
     */
    record AvailableNow() implements TriggerConfig {}

    /**
     * Continuous processing trigger with a checkpoint commit interval.
     * Maps to {@code Trigger.Continuous(checkpointInterval.toMillis(), TimeUnit.MILLISECONDS)}.
     *
     * @param checkpointInterval interval between asynchronous checkpoint commits
     */
    record Continuous(Duration checkpointInterval) implements TriggerConfig {}
}
