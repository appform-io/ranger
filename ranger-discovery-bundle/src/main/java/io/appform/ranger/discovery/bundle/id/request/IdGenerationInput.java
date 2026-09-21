package io.appform.ranger.discovery.bundle.id.request;

import io.appform.ranger.discovery.bundle.id.Domain;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class IdGenerationInput {
    Domain domain;
}
