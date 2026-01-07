package io.appform.ranger.core.finder.nodeselector.weightenricher;

import io.appform.ranger.core.model.ServiceNode;

public class RoutingWeightEnricher<T> implements WeightEnricher<T> {

    @Override
    public double enrichWeight(ServiceNode<T> node) {
        return node.getRoutingWeight();
    }
}