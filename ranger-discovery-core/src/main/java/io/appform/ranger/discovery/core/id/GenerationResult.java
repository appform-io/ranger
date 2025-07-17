package io.appform.ranger.discovery.core.id;

import lombok.Getter;
import lombok.Value;

@Getter
@Value
public class GenerationResult {
    NonceInfo nonceInfo;
    IdValidationState state;
    Domain domain;
}
