package io.appform.ranger.core.finder.nodeselector;

import io.appform.ranger.core.model.ServiceNode;
import io.appform.ranger.core.model.ServiceNodeSelector;
import io.appform.ranger.core.model.WeightedNodeSelectorConfig;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class WeightedRandomServiceNodeSelector<T> implements ServiceNodeSelector<T> {
    private final long minNodeAgeMs;
    private final double boostFactor;
    private final int weightedSelectionThreshold;

    public WeightedRandomServiceNodeSelector(final WeightedNodeSelectorConfig weightedNodeSelectorConfig) {
        weightedNodeSelectorConfig.validate();
        this.minNodeAgeMs = weightedNodeSelectorConfig.getMinNodeAgeMs();
        this.boostFactor = weightedNodeSelectorConfig.getBoostFactor();
        this.weightedSelectionThreshold = weightedNodeSelectorConfig.getWeightedSelectionThreshold();
    }

    @Override
    public ServiceNode<T> select(final List<ServiceNode<T>> serviceNodes) {
        if (serviceNodes == null || serviceNodes.isEmpty()) {
            throw new IllegalArgumentException("Service nodes list cannot be empty");
        }

        if (serviceNodes.size() == 1) {
            return serviceNodes.get(0);
        }

        if (serviceNodes.size() <= weightedSelectionThreshold) {
            return selectWeighted(serviceNodes);
        } else {
            return selectTwoRandomChoice(serviceNodes);
        }
    }

    /**
     * Uses a two-random-choice algorithm with recency and weight preference.
     */
    private ServiceNode<T> selectTwoRandomChoice(final List<ServiceNode<T>> serviceNodes) {
        final int firstNodeIndex = ThreadLocalRandom.current().nextInt(serviceNodes.size());
        int secondNodeIndex;
        do {
            secondNodeIndex = ThreadLocalRandom.current().nextInt(serviceNodes.size());
        } while (firstNodeIndex == secondNodeIndex);

        return getBetterNode(serviceNodes.get(firstNodeIndex), serviceNodes.get(secondNodeIndex));
    }

    /**
     * Weighted random selection based on normalized weights.
     */
    private ServiceNode<T> selectWeighted(final List<ServiceNode<T>> serviceNodes) {
        final long currentTime = System.currentTimeMillis();
        final double[] cumulativeWeights = new double[serviceNodes.size()];
        double totalWeight = 0.0;

        for (int i = 0; i < serviceNodes.size(); i++) {
            final ServiceNode<T> node = serviceNodes.get(i);
            double adjustedWeight = node.getRoutingWeight();

            if ((currentTime - node.getHealthySinceTimeStamp()) > minNodeAgeMs) {
                adjustedWeight *= boostFactor;
            }

            totalWeight += adjustedWeight;
            cumulativeWeights[i] = totalWeight;
        }

        final double randomValue = ThreadLocalRandom.current().nextDouble(totalWeight);
        //Returns: index of the search key, if it is contained in the array; otherwise, (-(insertion point) - 1)
        int index = Arrays.binarySearch(cumulativeWeights, randomValue);
        if (index < 0) {
            index = -index - 1;
        }

        return serviceNodes.get(index);
    }

    /**
     * Compares two ServiceNodes based on their startup time and weight.
     *
     * @param n1 First ServiceNode
     * @param n2 Second ServiceNode
     * @return The better ServiceNode based on the defined criteria
     */
    private ServiceNode<T> getBetterNode(final ServiceNode<T> n1, ServiceNode<T> n2) {
        final long currentTime = System.currentTimeMillis();
        final boolean n1IsRecent = ((currentTime - n1.getHealthySinceTimeStamp()) <= minNodeAgeMs);
        final boolean n2IsRecent = ((currentTime - n2.getHealthySinceTimeStamp()) <= minNodeAgeMs);

        if (n1IsRecent && !n2IsRecent) {
            return n2;
        } else if (n2IsRecent && !n1IsRecent) {
            return n1;
        }
        // In this scenario, prioritize by weight (higher weight wins)
        else {
            return (n1.getRoutingWeight() > n2.getRoutingWeight()) ? n1 : n2;
        }
    }
}
