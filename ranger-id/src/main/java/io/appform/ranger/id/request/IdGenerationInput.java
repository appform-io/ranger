package io.appform.ranger.id.request;

import io.appform.ranger.id.Domain;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class IdGenerationInput {
    String prefix;
    Domain domain;

}
