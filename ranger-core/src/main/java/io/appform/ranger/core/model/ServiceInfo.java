package io.appform.ranger.core.model;

import lombok.Builder;
import lombok.Value;

import java.util.Collection;

@Value
@Builder
public class ServiceInfo<T> {
    Service service;
    Collection<ServiceNode<T>> nodes;
}
