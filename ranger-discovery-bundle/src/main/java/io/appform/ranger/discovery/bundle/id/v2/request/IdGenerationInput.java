package io.appform.ranger.discovery.bundle.id.v2.request;

import io.appform.ranger.discovery.bundle.id.v2.Domain;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class IdGenerationInput {
    String prefix;
    String suffix;
    Domain domain;
}
