package io.appform.ranger.discovery.bundle.id;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class GenerationResult {
    IdInfo idInfo;
    IdValidationState state;
    String domain;
    String namespace;
}
