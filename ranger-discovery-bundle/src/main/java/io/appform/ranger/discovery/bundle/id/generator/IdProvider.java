package io.appform.ranger.discovery.bundle.id.generator;

import io.appform.ranger.discovery.bundle.id.Domain;
import io.appform.ranger.discovery.bundle.id.InternalId;
import io.appform.ranger.discovery.bundle.id.formatter.IdFormatter;

@FunctionalInterface
public interface IdProvider {
    
    InternalId apply(final String prefix, final IdFormatter formatter, final Domain domain);
}
