package io.appform.ranger.core.finder.nodeselector;

import io.appform.ranger.core.model.ServiceNode;
import io.appform.ranger.core.model.ServiceNodeSelector;

import java.util.List;
import java.util.function.Predicate;

public class ConditionalNodeSelector<T> implements ServiceNodeSelector<T> {

    private final Predicate<List<ServiceNode<T>>> condition;
    private final ServiceNodeSelector<T> primarySelector;
    private final ServiceNodeSelector<T> secondarySelector;

    public ConditionalNodeSelector(final Predicate<List<ServiceNode<T>>> condition,
                                   final ServiceNodeSelector<T> primarySelector,
                                   final ServiceNodeSelector<T> secondarySelector) {
        this.condition = condition;
        this.primarySelector = primarySelector;
        this.secondarySelector = secondarySelector;
    }

    @Override
    public ServiceNode<T> select(final List<ServiceNode<T>> serviceNodes) {
        if (condition.test(serviceNodes)) {
            return primarySelector.select(serviceNodes);
        } else {
            return secondarySelector.select(serviceNodes);
        }
    }
}
