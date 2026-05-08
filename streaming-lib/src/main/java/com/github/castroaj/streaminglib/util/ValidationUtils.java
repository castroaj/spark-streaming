package com.github.castroaj.streaminglib.util;

import java.util.Set;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.ConstraintViolationException;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import org.hibernate.validator.messageinterpolation.ParameterMessageInterpolator;

/**
 * Internal helper for Jakarta Bean Validation — not part of the public API.
 *
 * <p>Holds a single shared {@link Validator} instance configured with
 * {@link ParameterMessageInterpolator} so that no Jakarta EL implementation is required
 * on the classpath (important for Spark job environments).
 */
public final class ValidationUtils {

    private static final Validator VALIDATOR = Validation.byDefaultProvider()
        .configure()
        .messageInterpolator(new ParameterMessageInterpolator())
        .buildValidatorFactory()
        .getValidator();

    private ValidationUtils() {
    }

    /**
     * Validates {@code object} against its declared Jakarta constraints.
     *
     * @param <T>    the object type
     * @param object the object to validate
     * @throws ConstraintViolationException if any constraint is violated
     */
    public static <T> void validate(T object) {
        Set<ConstraintViolation<T>> violations = VALIDATOR.validate(object);
        if (!violations.isEmpty()) {
            throw new ConstraintViolationException(violations);
        }
    }
}
