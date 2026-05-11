package io.appform.ranger.discovery.bundle.id;

import lombok.Getter;
import lombok.Value;

@Getter
@Value
public class GenerationResult {
    int exponent;
    long time;
    InternalId internalId;
    IdValidationState state;
    Domain domain;
}
