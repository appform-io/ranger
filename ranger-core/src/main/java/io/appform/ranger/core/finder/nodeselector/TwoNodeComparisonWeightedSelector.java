package io.appform.ranger.core.finder.nodeselector;

import io.appform.ranger.core.model.ServiceNode;
import io.appform.ranger.core.model.ServiceNodeSelector;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class TwoNodeComparisonWeightedSelector<T> implements ServiceNodeSelector<T> {

    private final long minNodeAgeMs;

    public TwoNodeComparisonWeightedSelector(final long minNodeAgeMs) {
        this.minNodeAgeMs = minNodeAgeMs;
    }

    @Override
    public ServiceNode<T> select(final List<ServiceNode<T>> serviceNodes) {
        if (serviceNodes.size() == 1) {
            return serviceNodes.get(0);
        }
        final int firstNodeIndex = ThreadLocalRandom.current().nextInt(serviceNodes.size());
        int secondNodeIndex;
        do {
            secondNodeIndex = ThreadLocalRandom.current().nextInt(serviceNodes.size());
        } while (firstNodeIndex == secondNodeIndex);

        return getBetterNode(serviceNodes.get(firstNodeIndex), serviceNodes.get(secondNodeIndex));
    }

    private ServiceNode<T> getBetterNode(final ServiceNode<T> n1, ServiceNode<T> n2) {
        final long currentTime = System.currentTimeMillis();
        final boolean n1IsRecent = ((currentTime - n1.getHealthySinceTimeStamp()) <= minNodeAgeMs);
        final boolean n2IsRecent = ((currentTime - n2.getHealthySinceTimeStamp()) <= minNodeAgeMs);

        if (n1IsRecent && !n2IsRecent) {
            return n2;
        } else if (n2IsRecent && !n1IsRecent) {
            return n1;
        } else {
            // In this scenario, prioritize by weight (higher weight wins)
            return (n1.getRoutingWeight() > n2.getRoutingWeight()) ? n1 : n2;
        }
    }
}
