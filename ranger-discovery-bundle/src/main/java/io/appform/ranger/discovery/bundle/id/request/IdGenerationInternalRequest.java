package io.appform.ranger.discovery.bundle.id.request;

import io.appform.ranger.discovery.bundle.id.constraints.IdValidationConstraint;
import io.appform.ranger.discovery.bundle.id.decorators.IdDecorator;
import io.appform.ranger.discovery.bundle.id.formatter.IdFormatter;
import lombok.Builder;
import lombok.Value;

import java.util.List;

@Value
@Builder
public class IdGenerationInternalRequest {
    String prefix;
    String suffix;
    boolean skipGlobal;
    String domain;
    List<IdValidationConstraint> constraints;
    IdFormatter idFormatter;
    List<IdDecorator> idDecorators;
    int idGenerationValue;
}
