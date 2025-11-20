package io.appform.ranger.discovery.bundle.id.v2;

import io.appform.ranger.discovery.bundle.id.IdValidationState;
import io.appform.ranger.discovery.bundle.id.NonceInfo;
import lombok.Getter;
import lombok.Value;

@Getter
@Value
public class GenerationResult {
    NonceInfo nonceInfo;
    IdValidationState state;
    Domain domain;
}
