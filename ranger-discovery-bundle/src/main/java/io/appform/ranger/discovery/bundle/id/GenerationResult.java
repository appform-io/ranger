package io.appform.ranger.discovery.bundle.id;

import lombok.Getter;
import lombok.Value;

@Getter
@Value
public class GenerationResult {
    int exponent;
    long time;
    IdValidationState state;
    Domain domain;
}
