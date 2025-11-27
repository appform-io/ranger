package io.appform.ranger.core.finder.nodeselector;

import io.appform.ranger.core.finder.nodeselector.weightenricher.RoutingWeightEnricher;
import io.appform.ranger.core.finder.nodeselector.weightenricher.TimeBasedWeightEnricher;
import io.appform.ranger.core.finder.nodeselector.weightenricher.WeightEnricher;
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
        final List<WeightEnricher<T>> weightEnrichers = List.of(
                new RoutingWeightEnricher<>(),
                new TimeBasedWeightEnricher<>(weightedNodeSelectorConfig.getMinNodeAgeMs(),
                                              weightedNodeSelectorConfig.getWeightBoostMultiplier())
                                                               );

        this.conditionalSelector = new ConditionalNodeSelector<>(
                condition,
                new IterativeWeightedSelector<>(weightEnrichers),
                new TwoNodeComparisonWeightedSelector<>(weightEnrichers)
        );
    }

    @Override
    public ServiceNode<T> select(final List<ServiceNode<T>> serviceNodes) {
        return conditionalSelector.select(serviceNodes);
    }
}
