package com.github.castroaj.streaminglib.exception;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.junit.jupiter.api.Test;

class DeltaStreamExceptionTest {

    @Test
    void constructor_withMessage_messageIsPreserved() {
        DeltaStreamException ex = new DeltaStreamException("write failed");
        assertEquals("write failed", ex.getMessage());
    }

    @Test
    void constructor_withCause_causeIsPreserved() {
        RuntimeException cause = new RuntimeException("root cause");
        DeltaStreamException ex = new DeltaStreamException("write failed", cause);
        assertSame(cause, ex.getCause());
    }

    @Test
    void constructor_withCause_messageIsPreserved() {
        DeltaStreamException ex = new DeltaStreamException("write failed",
            new RuntimeException("root cause"));
        assertEquals("write failed", ex.getMessage());
    }

    @Test
    void isInstanceOfStreamingLibException() {
        DeltaStreamException ex = new DeltaStreamException("write failed");
        assertInstanceOf(StreamingLibException.class, ex);
    }

    @Test
    void isInstanceOfRuntimeException() {
        DeltaStreamException ex = new DeltaStreamException("write failed");
        assertInstanceOf(RuntimeException.class, ex);
    }
}
