package io.appform.ranger.core.finder.nodeselector;

import io.appform.ranger.core.model.ServiceNode;
import io.appform.ranger.core.model.ServiceNodeSelector;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public final class WarmupRoundRobinServiceNodeSelector<T> implements ServiceNodeSelector<T> {

    private final RoundRobinServiceNodeSelector<T> roundRobinSelector = new RoundRobinServiceNodeSelector<>();
    private final long warmupDurationMs;
    private final int maxAttempts;

    /**
     * Creates a round-robin selector with bounded, linear warm-up acceptance.
     *
     * @param warmupDurationMs the healthy age for full acceptance, or zero to disable warm-up
     * @param maxAttempts the number of candidate acceptance attempts before uniform fallback;
     *                    attempts may revisit nodes after completing a cycle
     * @throws IllegalArgumentException if the duration is negative or attempts are not positive
     */
    public WarmupRoundRobinServiceNodeSelector(final long warmupDurationMs, final int maxAttempts) {
        if (warmupDurationMs < 0) {
            throw new IllegalArgumentException("warmupDurationMs must be non-negative");
        }
        if (maxAttempts <= 0) {
            throw new IllegalArgumentException("maxAttempts must be positive");
        }
        this.warmupDurationMs = warmupDurationMs;
        this.maxAttempts = maxAttempts;
    }

    /**
     * Selects using round-robin candidate visits and healthy age measured at the
     * start of this call. Acceptance probability is the healthy age divided by
     * the warm-up duration, clamped to [0, 1]. Future timestamps receive zero
     * acceptance probability; zero timestamps are interpreted as the Unix epoch.
     * Singleton pools and disabled warm-up delegate directly to round-robin.
     *
     * @param serviceNodes the non-null, non-empty list of eligible, non-null nodes
     * @return an accepted candidate, or a uniformly sampled node after attempt exhaustion
     * @throws IllegalArgumentException if the list is empty
     */
    @Override
    @SuppressWarnings("java:S2245")
    public ServiceNode<T> select(final List<ServiceNode<T>> serviceNodes) {
        final var nodeCount = serviceNodes.size();
        if (nodeCount == 0) {
            throw new IllegalArgumentException("serviceNodes must not be empty");
        }
        if (nodeCount == 1 || warmupDurationMs == 0) {
            return roundRobinSelector.select(serviceNodes);
        }

        final var now = System.currentTimeMillis();
        final var random = ThreadLocalRandom.current();
        for (int attempt = 0; attempt < maxAttempts; attempt++) {
            final var node = roundRobinSelector.select(serviceNodes);
            // Subtract as doubles to avoid overflow for extreme timestamps.
            final var healthyAgeMs = Math.max(0.0, (double) now - node.getHealthySinceTimeStamp());
            if (healthyAgeMs >= warmupDurationMs
                    || random.nextDouble() < healthyAgeMs / warmupDurationMs) {
                return node;
            }
        }

        return serviceNodes.get(random.nextInt(nodeCount));
    }
}
