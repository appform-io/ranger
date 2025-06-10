package io.appform.ranger.core.finder.nodeselector;

import io.appform.ranger.core.model.ServiceNode;
import io.appform.ranger.core.model.ServiceNodeSelector;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class WeightedRandomServiceNodeSelector<T> implements ServiceNodeSelector<T> {
    private static final long FIVE_MINUTES_IN_MS = 5 * 60 * 1000; // 5 minutes in milliseconds

    @Override
    public ServiceNode<T> select(List<ServiceNode<T>> serviceNodes) {
        if (serviceNodes == null || serviceNodes.isEmpty()) {
            throw new IllegalArgumentException("Service nodes list cannot be empty");
        }
        if (serviceNodes.size() == 1) {
            return serviceNodes.get(0);
        }

        // Pick two distinct random distinct indices
        final int index1 = ThreadLocalRandom.current().nextInt(serviceNodes.size());
        int index2;
        do {
            index2 = ThreadLocalRandom.current().nextInt(serviceNodes.size());
        } while (index1 == index2);

        return getBetterNode(serviceNodes.get(index1), serviceNodes.get(index2));
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
        final boolean n1IsRecent = (currentTime - n1.getNodeStartupTimeInMs() <= FIVE_MINUTES_IN_MS);
        final boolean n2IsRecent = (currentTime - n2.getNodeStartupTimeInMs() <= FIVE_MINUTES_IN_MS);

        // Case 1: n1 is recent, n2 is NOT recent
        if (n1IsRecent && !n2IsRecent) {
            return n1;
        }
        // Case 2: n2 is recent, n1 is NOT recent
        else if (n2IsRecent && !n1IsRecent) {
            return n2;
        }
        // Case 3: Both are recent OR Both are not recent
        // In this scenario, prioritize by weight (higher weight wins)
        else {
            return (n1.getWeight() > n2.getWeight()) ? n1 : n2;
        }
    }
}
