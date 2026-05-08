package com.github.castroaj.streaminglib.exception;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.junit.jupiter.api.Test;

class KafkaSourceExceptionTest {

    @Test
    void constructor_withMessage_messageIsPreserved() {
        KafkaSourceException ex = new KafkaSourceException("test message");
        assertEquals("test message", ex.getMessage());
    }

    @Test
    void constructor_withCause_causeIsPreserved() {
        RuntimeException cause = new RuntimeException("original");
        KafkaSourceException ex = new KafkaSourceException("wrapper message", cause);
        assertSame(cause, ex.getCause());
    }

    @Test
    void constructor_withCause_messageIsPreserved() {
        RuntimeException cause = new RuntimeException("original");
        KafkaSourceException ex = new KafkaSourceException("wrapper message", cause);
        assertEquals("wrapper message", ex.getMessage());
    }

    @Test
    void isInstanceOfStreamingLibException() {
        KafkaSourceException ex = new KafkaSourceException("msg");
        assertInstanceOf(StreamingLibException.class, ex);
    }

    @Test
    void isInstanceOfRuntimeException() {
        KafkaSourceException ex = new KafkaSourceException("msg");
        assertInstanceOf(RuntimeException.class, ex);
    }
}
