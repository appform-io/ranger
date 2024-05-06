package io.appform.ranger.discovery.bundle.id.constraints;


public interface KeyValidationConstraint {
    boolean isValid(int id);

    default boolean failFast() {
        return false;
    }
}
