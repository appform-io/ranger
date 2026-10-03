package io.appform.ranger.core.finder.nodeselector.weightenricher;

import io.appform.ranger.core.model.ServiceNode;

public class TimeBasedWeightEnricher<T> implements WeightEnricher<T> {

    private final long minNodeAgeMs;

    /**
     * Creates a linear warm-up enricher.
     *
     * @param minNodeAgeMs healthy age at full weight; zero disables warm-up
     * @throws IllegalArgumentException if the duration is negative
     */
    public TimeBasedWeightEnricher(final long minNodeAgeMs) {
        if (minNodeAgeMs < 0) {
            throw new IllegalArgumentException("minNodeAgeMs must be non-negative");
        }
        this.minNodeAgeMs = minNodeAgeMs;
    }

    /**
     * Returns the healthy age fraction, clamped to [0, 1].
     * Future timestamps receive zero weight during warm-up. A zero timestamp is
     * interpreted as the Unix epoch, preserving the existing timestamp convention.
     *
     * @param node the node whose healthy timestamp determines its warm-up progress
     * @return the linear time-based weight, or one when warm-up is disabled
     */
    @Override
    public double enrichWeight(final ServiceNode<T> node) {
        if (minNodeAgeMs == 0) {
            return 1.0;
        }
        final var currentTime = System.currentTimeMillis();
        final var healthyAgeMs = Math.max(0.0, (double) currentTime - node.getHealthySinceTimeStamp());
        return Math.min(1.0, healthyAgeMs / minNodeAgeMs);
    }
}