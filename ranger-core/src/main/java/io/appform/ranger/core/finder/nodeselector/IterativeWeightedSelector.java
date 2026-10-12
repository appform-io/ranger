package io.appform.ranger.core.finder.nodeselector;

import io.appform.ranger.core.finder.nodeselector.weightenricher.WeightEnricher;
import io.appform.ranger.core.model.ServiceNode;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

@Slf4j
public class IterativeWeightedSelector<T> extends AbstractWeightedSelector<T> {

    public IterativeWeightedSelector(final List<WeightEnricher<T>> weightAdjusters) {
        super(weightAdjusters);
    }

    /**
     * Selects proportionally to adjusted weights across the pool. If every weight is
     * zero, selects uniformly so a newly healthy pool can still serve requests.
     *
     * @param serviceNodes a non-empty list of eligible nodes
     * @return the selected node
     * @throws IllegalArgumentException if the list is empty
     */
    @Override
    @SuppressWarnings("java:S2245")
    public ServiceNode<T> select(final List<ServiceNode<T>> serviceNodes) {
        final var nodeCount = serviceNodes.size();
        if (nodeCount == 0) {
            throw new IllegalArgumentException("serviceNodes must not be empty");
        }
        final var cumulativeWeights = new double[nodeCount];
        double totalWeight = 0.0;

        for (int i = 0; i < nodeCount; i++) {
            final var node = serviceNodes.get(i);
            final var adjustedWeight = adjustWeight(node);
            totalWeight += adjustedWeight;
            cumulativeWeights[i] = totalWeight;
        }

        final var random = ThreadLocalRandom.current();
        if (totalWeight == 0.0) {
            // Should never happen: weight enrichers are expected to keep at least one node routable.
            log.warn("All {} eligible nodes have zero adjusted weight; falling back to uniform random selection",
                     nodeCount);
            return serviceNodes.get(random.nextInt(nodeCount));
        }
        final var randomValue = random.nextDouble(totalWeight);
        // Find the first strictly greater cumulative weight, skipping zero-weight intervals.
        int low = 0;
        int high = nodeCount - 1;
        while (low < high) {
            final var middle = low + (high - low) / 2;
            if (cumulativeWeights[middle] <= randomValue) {
                low = middle + 1;
            } else {
                high = middle;
            }
        }

        return serviceNodes.get(low);
    }
}
