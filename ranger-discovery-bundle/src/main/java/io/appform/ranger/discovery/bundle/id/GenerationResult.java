package io.appform.ranger.discovery.bundle.id;

import lombok.Value;

@Value
public class GenerationResult {
    IdInfo idInfo;
    IdValidationState state;
    String domain;
}
