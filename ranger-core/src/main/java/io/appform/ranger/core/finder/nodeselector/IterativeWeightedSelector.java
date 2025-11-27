package io.appform.ranger.core.finder.nodeselector;

import io.appform.ranger.core.finder.nodeselector.weightenricher.WeightEnricher;
import io.appform.ranger.core.model.ServiceNode;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class IterativeWeightedSelector<T> extends AbstractWeightedSelector<T> {

    public IterativeWeightedSelector(final List<WeightEnricher<T>> weightAdjusters) {
        super(weightAdjusters);
    }

    @Override
    public ServiceNode<T> select(final List<ServiceNode<T>> serviceNodes) {
        final double[] cumulativeWeights = new double[serviceNodes.size()];
        double totalWeight = 0.0;

        for (int i = 0; i < serviceNodes.size(); i++) {
            final ServiceNode<T> node = serviceNodes.get(i);
            double adjustedWeight = adjustWeight(node);
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
}
