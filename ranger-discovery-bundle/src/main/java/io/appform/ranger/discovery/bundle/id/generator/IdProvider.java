package io.appform.ranger.discovery.bundle.id.generator;

import io.appform.ranger.discovery.bundle.id.InternalId;
import io.appform.ranger.discovery.bundle.id.request.IdGenerationInternalRequest;

@FunctionalInterface
public interface IdProvider {
    
    InternalId apply(final IdGenerationInternalRequest request);
}
