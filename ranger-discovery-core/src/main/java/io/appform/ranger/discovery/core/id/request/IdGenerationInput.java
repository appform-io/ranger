package io.appform.ranger.discovery.core.id.request;

import io.appform.ranger.discovery.core.id.Domain;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class IdGenerationInput {
    String prefix;
    Domain domain;

}
