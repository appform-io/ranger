package io.appform.ranger.core.finder.nodeselector;

import io.appform.ranger.core.model.ServiceNode;
import io.appform.ranger.core.model.ServiceNodeSelector;
import io.appform.ranger.core.model.WeightedNodeSelectorConfig;

import java.util.List;
import java.util.function.Predicate;

public class WeightedRandomServiceNodeSelector<T> implements ServiceNodeSelector<T> {

    public static final long DEFAULT_MIN_NODE_AGE = 60000L;
    public static final double DEFAULT_BOOST_FACTOR = 1.0;
    public static final int DEFAULT_WEIGHTED_SELECTION_MIN_NODES_THRESHOLD = 10;
    private final ServiceNodeSelector<T> conditionalSelector;

    public WeightedRandomServiceNodeSelector(final WeightedNodeSelectorConfig weightedNodeSelectorConfig) {
        weightedNodeSelectorConfig.validate();
        final Predicate<List<ServiceNode<T>>> condition = serviceNodes ->
                serviceNodes.size() <= weightedNodeSelectorConfig.getWeightedSelectionThreshold();

        this.conditionalSelector = new ConditionalNodeSelector<>(
                condition,
                new IterativeWeightedSelector<>(weightedNodeSelectorConfig.getMinNodeAgeMs(),
                                                weightedNodeSelectorConfig.getWeightBoostMultiplier()),
                new TwoNodeComparisonWeightedSelector<>(weightedNodeSelectorConfig.getMinNodeAgeMs())
        );
    }

    @Override
    public ServiceNode<T> select(final List<ServiceNode<T>> serviceNodes) {
        return conditionalSelector.select(serviceNodes);
    }
}
