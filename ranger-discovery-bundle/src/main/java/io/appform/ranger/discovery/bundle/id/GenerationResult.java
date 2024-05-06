package io.appform.ranger.discovery.bundle.id;

import lombok.Value;

@Value
class GenerationResult {
    Id id;
    IdValidationState state;
}
