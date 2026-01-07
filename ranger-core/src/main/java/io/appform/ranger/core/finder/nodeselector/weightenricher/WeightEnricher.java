package io.appform.ranger.core.finder.nodeselector.weightenricher;

import io.appform.ranger.core.model.ServiceNode;

public interface WeightEnricher<T> {
    double enrichWeight(ServiceNode<T> node);
}
