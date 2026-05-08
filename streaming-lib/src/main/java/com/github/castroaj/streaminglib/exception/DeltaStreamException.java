package com.github.castroaj.streaminglib.exception;

/**
 * Unchecked exception thrown when a {@code DeltaStreamWriter} operation fails.
 *
 * <p>Wraps connector-level exceptions (e.g., Spark {@code StreamingQueryException} or other
 * runtime failures from the Delta Lake streaming sink). The original exception is always preserved
 * as the cause and accessible via {@link #getCause()}.
 *
 * <p>The exception message follows the format:
 * {@code Failed to <operation> to '<destinationPath>' — <cause message>}
 */
public class DeltaStreamException extends StreamingLibException {

    /**
     * Constructs a new exception with the given detail message.
     *
     * @param message human-readable description of the failure including the destination path
     */
    public DeltaStreamException(String message) {
        super(message);
    }

    /**
     * Constructs a new exception wrapping a connector-level cause.
     *
     * @param message human-readable description of the failure including the destination path
     * @param cause   the original connector exception
     */
    public DeltaStreamException(String message, Throwable cause) {
        super(message, cause);
    }
}
