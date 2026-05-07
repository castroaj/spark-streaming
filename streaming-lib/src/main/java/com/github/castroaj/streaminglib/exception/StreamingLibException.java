package com.github.castroaj.streaminglib.exception;

/**
 * Base unchecked exception for all failures originating in streaming-lib.
 *
 * <p>Catch this type for uniform handling across all connectors, or catch a specific
 * subtype ({@link KafkaSourceException}) for targeted recovery. The original connector
 * exception is always available via {@link #getCause()}.
 *
 * <p>Instances are immutable and thread-safe.
 */
public class StreamingLibException extends RuntimeException {

    /**
     * Constructs a new exception with the given detail message.
     *
     * @param message human-readable description of the failure
     */
    public StreamingLibException(String message) {
        super(message);
    }

    /**
     * Constructs a new exception wrapping a connector-level cause.
     *
     * @param message human-readable description of the failure, including the resource identifier
     * @param cause   the original connector exception
     */
    public StreamingLibException(String message, Throwable cause) {
        super(message, cause);
    }
}
