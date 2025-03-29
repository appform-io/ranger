package io.appform.ranger.core.finder.nodeselector;

import io.appform.ranger.core.model.ServiceNode;
import io.appform.ranger.core.model.ServiceNodeSelector;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class WeightedRandomServiceNodeSelector<T> implements ServiceNodeSelector<T> {

    @Override
    public ServiceNode<T> select(List<ServiceNode<T>> serviceNodes) {
        if (serviceNodes == null || serviceNodes.isEmpty()) {
            throw new IllegalArgumentException("Service nodes list cannot be empty");
        }

        double totalWeight = 0.0;
        double[] cumulativeWeights = new double[serviceNodes.size()];

        for (int i = 0; i < serviceNodes.size(); i++) {
            totalWeight += serviceNodes.get(i).getWeight();
            cumulativeWeights[i] = totalWeight;
        }

        double randomValue = ThreadLocalRandom.current().nextDouble(totalWeight);


        int index = Arrays.binarySearch(cumulativeWeights, randomValue);
        if (index < 0) {
            index = -index - 1;
        }

        return serviceNodes.get(index);
    }
}
