package com.github.castroaj.streaminglib.delta;

/**
 * Output mode for a Delta streaming write.
 *
 * <p>Each constant maps to the corresponding Spark
 * {@link org.apache.spark.sql.streaming.OutputMode} factory method used when configuring
 * a {@code DataStreamWriter}.
 *
 * <ul>
 *   <li>{@link #APPEND} — only new rows appended since the last trigger are written.
 *       The default and most common mode for Delta sink streaming jobs.</li>
 *   <li>{@link #COMPLETE} — the entire result table is rewritten on every trigger.
 *       Only valid for aggregation queries.</li>
 *   <li>{@link #UPDATE} — only rows that were updated since the last trigger are written.
 *       Requires the sink to support upserts.</li>
 * </ul>
 */
public enum OutputMode {

    /**
     * Only new rows added since the last trigger are written to the sink.
     * Maps to {@code org.apache.spark.sql.streaming.OutputMode.Append()}.
     */
    APPEND,

    /**
     * The complete result table is rewritten to the sink on every trigger.
     * Maps to {@code org.apache.spark.sql.streaming.OutputMode.Complete()}.
     */
    COMPLETE,

    /**
     * Only rows that changed since the last trigger are written to the sink.
     * Maps to {@code org.apache.spark.sql.streaming.OutputMode.Update()}.
     */
    UPDATE
}
