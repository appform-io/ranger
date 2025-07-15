package io.appform.ranger.core.finder.nodeselector;

import io.appform.ranger.core.model.ServiceNode;
import io.appform.ranger.core.model.ServiceNodeSelector;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class WeightedRandomServiceNodeSelector<T> implements ServiceNodeSelector<T> {
    private static final long FIVE_MINUTES_IN_MS = 5 * 60 * 1000; // 5 minutes
    private static final double BOOST_FACTOR = 1.5;

    @Override
    public ServiceNode<T> select(List<ServiceNode<T>> serviceNodes) {
        if (serviceNodes == null || serviceNodes.isEmpty()) {
            throw new IllegalArgumentException("Service nodes list cannot be empty");
        }

        if (serviceNodes.size() == 1) {
            return serviceNodes.get(0);
        }

        if (serviceNodes.size() < 20) {
            return selectWeighted(serviceNodes);
        } else {
            return selectTwoRandomChoice(serviceNodes);
        }
    }

    /**
     * Uses a two-random-choice algorithm with recency and weight preference.
     */
    private ServiceNode<T> selectTwoRandomChoice(List<ServiceNode<T>> serviceNodes) {
        final int index1 = ThreadLocalRandom.current().nextInt(serviceNodes.size());
        int index2;
        do {
            index2 = ThreadLocalRandom.current().nextInt(serviceNodes.size());
        } while (index1 == index2);

        return getBetterNode(serviceNodes.get(index1), serviceNodes.get(index2));
    }

    /**
     * Weighted random selection based on normalized weights.
     */
    private ServiceNode<T> selectWeighted(List<ServiceNode<T>> serviceNodes) {
        long currentTime = System.currentTimeMillis();
        double totalWeight = 0.0;
        double[] cumulativeWeights = new double[serviceNodes.size()];

        for (int i = 0; i < serviceNodes.size(); i++) {
            ServiceNode<T> node = serviceNodes.get(i);
            double adjustedWeight = node.getWeight();

            if ((currentTime - node.getNodeStartupTimeInMs()) > FIVE_MINUTES_IN_MS) {
                adjustedWeight *= BOOST_FACTOR;
            }

            totalWeight += adjustedWeight;
            cumulativeWeights[i] = totalWeight;
        }

        double randomValue = ThreadLocalRandom.current().nextDouble(totalWeight);
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
    private ServiceNode<T> getBetterNode(ServiceNode<T> n1, ServiceNode<T> n2) {
        final long currentTime = System.currentTimeMillis();
        final boolean n1IsRecent = ((currentTime - n1.getNodeStartupTimeInMs()) <= FIVE_MINUTES_IN_MS);
        final boolean n2IsRecent = ((currentTime - n2.getNodeStartupTimeInMs()) <= FIVE_MINUTES_IN_MS);

        if (n1IsRecent && !n2IsRecent) {
            return n2;
        } else if (n2IsRecent && !n1IsRecent) {
            return n1;
        }
        // In this scenario, prioritize by weight (higher weight wins)
        else {
            return (n1.getWeight() > n2.getWeight()) ? n1 : n2;
        }
    }
}
