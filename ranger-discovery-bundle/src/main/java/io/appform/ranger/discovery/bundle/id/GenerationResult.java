package io.appform.ranger.discovery.bundle.id;

import lombok.Getter;
import lombok.Value;

@Getter
@Value
public class GenerationResult {
    NonceInfo nonceInfo;
    IdValidationState state;
    Domain domain;
    String namespace;
}
