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

package io.appform.ranger.core.finder;

import io.appform.ranger.core.model.ServiceNode;
import io.appform.ranger.core.model.ServiceNodeSelector;
import io.appform.ranger.core.model.ServiceRegistry;
import io.appform.ranger.core.model.ShardSelector;
import io.appform.ranger.core.signals.ExternalTriggeredSignal;
import io.appform.ranger.core.util.ObjectUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

@Slf4j
public abstract class ServiceFinder<T, R extends ServiceRegistry<T>> {
    @Getter
    private final R serviceRegistry;
    private final ShardSelector<T, R> shardSelector;
    private final ServiceNodeSelector<T> nodeSelector;
    @Getter
    private final ExternalTriggeredSignal<Void> startSignal = new ExternalTriggeredSignal<>(() -> null,
            Collections.emptyList());
    @Getter
    private final ExternalTriggeredSignal<Void> stopSignal = new ExternalTriggeredSignal<>(() -> null,
            Collections.emptyList());

    protected ServiceFinder(
            R serviceRegistry,
            ShardSelector<T, R> shardSelector,
            ServiceNodeSelector<T> nodeSelector) {
        this.serviceRegistry = serviceRegistry;
        this.shardSelector = shardSelector;
        this.nodeSelector = nodeSelector;
    }

    public Optional<ServiceNode<T>> get(Predicate<T> criteria) {
        return this.get(criteria, null);
    }

    public Optional<ServiceNode<T>> get(
            Predicate<T> criteria,
            final ShardSelector<T, R> shardSelector) {
        return get(criteria, shardSelector, null);
    }

    public Optional<ServiceNode<T>> get(
            Predicate<T> criteria,
            final ShardSelector<T, R> shardSelector,
            final ServiceNodeSelector<T> nodeSelector) {
        val nodes = ObjectUtils.requireNonNullElse(shardSelector, this.shardSelector)
                .nodes(criteria, serviceRegistry);
        return nodes.isEmpty()
                ? Optional.empty()
                : Optional.ofNullable(ObjectUtils.requireNonNullElse(nodeSelector, this.nodeSelector).select(nodes));
    }

    public List<ServiceNode<T>> getAll(Predicate<T> criteria) {
        return getAll(criteria, this.shardSelector);
    }

    public List<ServiceNode<T>> getAll(Predicate<T> criteria, final ShardSelector<T, R> shardSelector) {
        return ObjectUtils.requireNonNullElse(shardSelector, this.shardSelector)
                .nodes(criteria, serviceRegistry);
    }

    public void start() {
        startSignal.trigger();
    }

    public void stop() {
        stopSignal.trigger();
    }
}
