package io.appform.ranger.discovery.bundle.id;

import lombok.Getter;
import lombok.Value;

@Getter
@Value
public class GenerationResult {
    IdInfo idInfo;
    IdValidationState state;
    String domain;
}
