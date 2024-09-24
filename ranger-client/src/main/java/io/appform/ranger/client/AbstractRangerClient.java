/*
 * Copyright 2024 Authors, Flipkart Internet Pvt. Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.appform.ranger.client;

import io.appform.ranger.client.utils.CriteriaUtils;
import io.appform.ranger.core.finder.ServiceFinder;
import io.appform.ranger.core.model.ServiceNode;
import io.appform.ranger.core.model.ServiceNodeSelector;
import io.appform.ranger.core.model.ServiceRegistry;
import io.appform.ranger.core.model.ShardSelector;
import lombok.experimental.SuperBuilder;

import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

@SuperBuilder
public abstract class AbstractRangerClient<T, R extends ServiceRegistry<T>> implements RangerClient<T, R> {

    private final Predicate<T> initialCriteria;
    private final boolean alwaysUseInitialCriteria;

    public abstract ServiceFinder<T, R> getServiceFinder();

    @Override
    public Optional<ServiceNode<T>> getNode() {
        return getNode(initialCriteria);
    }

    @Override
    public Optional<ServiceNode<T>> getNode(Predicate<T> criteria) {
        return getNode(criteria, null);
    }

    @Override
    public Optional<ServiceNode<T>> getNode(
            Predicate<T> criteria, ShardSelector<T, R> shardSelector) {
        return getNode(criteria, shardSelector, null);
    }

    @Override
    public Optional<ServiceNode<T>> getNode(
            Predicate<T> criteria,
            ShardSelector<T, R> shardSelector,
            ServiceNodeSelector<T> nodeSelector) {
        return getServiceFinder().get(CriteriaUtils.getCriteria(alwaysUseInitialCriteria, initialCriteria, criteria),
                                      shardSelector,
                                      nodeSelector);
    }

    @Override
    public List<ServiceNode<T>> getAllNodes() {
        return getAllNodes(initialCriteria);
    }

    @Override
    public List<ServiceNode<T>> getAllNodes(Predicate<T> criteria) {
        return getAllNodes(criteria, null);
    }

    @Override
    public List<ServiceNode<T>> getAllNodes(
            Predicate<T> criteria, ShardSelector<T, R> shardSelector) {
        return getServiceFinder().getAll(CriteriaUtils.getCriteria(alwaysUseInitialCriteria,
                                                                   initialCriteria,
                                                                   criteria),
                                         shardSelector);
    }

}
