package io.appform.ranger.core.finder.nodeselector;

import io.appform.ranger.core.finder.nodeselector.weightenricher.WeightEnricher;
import io.appform.ranger.core.model.ServiceNode;
import io.appform.ranger.core.model.ServiceNodeSelector;

import java.util.List;

public abstract class AbstractWeightedSelector<T> implements ServiceNodeSelector<T> {

    private final List<WeightEnricher<T>> weightAdjusters;

    protected AbstractWeightedSelector(final List<WeightEnricher<T>> weightAdjusters) {
        this.weightAdjusters = weightAdjusters;
    }

    protected double adjustWeight(ServiceNode<T> node) {
        double adjustedWeight = 1;
        for (WeightEnricher<T> enricher : weightAdjusters) {
            adjustedWeight *= enricher.enrichWeight(node);
        }
        return adjustedWeight;
    }
}