package io.appform.ranger.core.finder.nodeselector;

import io.appform.ranger.core.finder.nodeselector.weightenricher.WeightEnricher;
import io.appform.ranger.core.model.ServiceNode;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class TwoNodeComparisonWeightedSelector<T> extends AbstractWeightedSelector<T> {


    public TwoNodeComparisonWeightedSelector(final List<WeightEnricher<T>> weightAdjusters) {
        super(weightAdjusters);
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
        return adjustWeight(n1) > adjustWeight(n2) ? n1 : n2;
    }
}
