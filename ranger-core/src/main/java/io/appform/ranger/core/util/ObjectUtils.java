package io.appform.ranger.core.util;

import lombok.experimental.UtilityClass;

import java.util.Objects;

/**
 * Utility calss . To be removed when we move to J11
 */
@UtilityClass
public class ObjectUtils {
    public <T> T requireNonNullElse(final T value, final T defaultValue) {
        Objects.requireNonNull(defaultValue, "Default cannot be null");
        return null != value ? value : defaultValue;
    }
}
