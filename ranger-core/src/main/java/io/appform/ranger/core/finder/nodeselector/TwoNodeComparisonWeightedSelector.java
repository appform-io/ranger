package io.appform.ranger.core.finder.nodeselector;

import io.appform.ranger.core.finder.nodeselector.weightenricher.WeightEnricher;
import io.appform.ranger.core.model.ServiceNode;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class TwoNodeComparisonWeightedSelector<T> extends AbstractWeightedSelector<T> {


    /**
     * Creates a selector using the supplied weight enrichers.
     *
     * @param weightAdjusters enrichers producing finite, non-negative adjusted weights
     */
    public TwoNodeComparisonWeightedSelector(final List<WeightEnricher<T>> weightAdjusters) {
        super(weightAdjusters);
    }

    /**
     * Chooses from a uniformly sampled pair using their relative adjusted weights.
     * If both weights are zero, either sampled node is chosen with equal probability.
     *
     * @param serviceNodes a non-empty list of eligible nodes
     * @return the selected node, or the sole node in a singleton pool
     * @throws IllegalArgumentException if the list is empty
     */
    @Override
    @SuppressWarnings("java:S2245")
    public ServiceNode<T> select(final List<ServiceNode<T>> serviceNodes) {
        final var nodeCount = serviceNodes.size();
        if (nodeCount == 0) {
            throw new IllegalArgumentException("serviceNodes must not be empty");
        }
        if (nodeCount == 1) {
            return serviceNodes.get(0);
        }
        final var random = ThreadLocalRandom.current();
        final var firstNodeIndex = random.nextInt(nodeCount);
        final var secondCandidateIndex = random.nextInt(nodeCount - 1);
        final var secondNodeIndex = secondCandidateIndex >= firstNodeIndex
                ? secondCandidateIndex + 1 : secondCandidateIndex;

        return selectFromPair(serviceNodes.get(firstNodeIndex), serviceNodes.get(secondNodeIndex));
    }

    @SuppressWarnings("java:S2245")
    private ServiceNode<T> selectFromPair(final ServiceNode<T> n1, final ServiceNode<T> n2) {
        final var firstWeight = adjustWeight(n1);
        final var secondWeight = adjustWeight(n2);
        final var random = ThreadLocalRandom.current();
        final var maxWeight = Math.max(firstWeight, secondWeight);
        if (maxWeight == 0.0) {
            return random.nextBoolean() ? n1 : n2;
        }

        // Scaling preserves the ratio without overflowing when adding large weights.
        final var scaledFirstWeight = firstWeight / maxWeight;
        final var scaledSecondWeight = secondWeight / maxWeight;
        return random.nextDouble() < scaledFirstWeight / (scaledFirstWeight + scaledSecondWeight) ? n1 : n2;
    }
}
