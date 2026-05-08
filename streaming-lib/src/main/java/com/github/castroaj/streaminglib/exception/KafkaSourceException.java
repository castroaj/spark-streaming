package com.github.castroaj.streaminglib.exception;

/**
 * Unchecked exception thrown when a {@code KafkaSource} operation fails.
 *
 * <p>Wraps connector-level exceptions (e.g., Spark AnalysisException, runtime failures from
 * the Kafka source connector). The original exception is always preserved as the cause and
 * accessible via {@link #getCause()}.
 *
 * <p>The exception message follows the format:
 * {@code Failed to <operation> on topic '<topic>' at <bootstrapServers> — <cause message>}
 */
public class KafkaSourceException extends StreamingLibException {

    /**
     * Constructs a new exception with the given detail message.
     *
     * @param message human-readable description of the failure including topic and bootstrap servers
     */
    public KafkaSourceException(String message) {
        super(message);
    }

    /**
     * Constructs a new exception wrapping a connector-level cause.
     *
     * @param message human-readable description of the failure including topic and bootstrap servers
     * @param cause   the original connector exception
     */
    public KafkaSourceException(String message, Throwable cause) {
        super(message, cause);
    }
}
