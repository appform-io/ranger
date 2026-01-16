package io.appform.ranger.core.finder.nodeselector.weightenricher;

import io.appform.ranger.core.model.ServiceNode;

public class TimeBasedWeightEnricher<T> implements WeightEnricher<T> {

    private final long minNodeAgeMs;
    private final double weightBoostMultiplier;

    public TimeBasedWeightEnricher(long minNodeAgeMs, double weightBoostMultiplier) {
        this.minNodeAgeMs = minNodeAgeMs;
        this.weightBoostMultiplier = weightBoostMultiplier;
    }

    @Override
    public double enrichWeight(ServiceNode<T> node) {
        final var currentTime = System.currentTimeMillis();
        if ((currentTime - node.getHealthySinceTimeStamp()) > minNodeAgeMs) {
            return weightBoostMultiplier;
        }
        return 1.0;
    }
}