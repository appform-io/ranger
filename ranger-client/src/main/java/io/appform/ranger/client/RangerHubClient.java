/*
 * Copyright 2015 Flipkart Internet Pvt. Ltd.
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

import io.appform.ranger.core.model.Service;
import io.appform.ranger.core.model.ServiceNode;
import io.appform.ranger.core.model.ServiceNodeSelector;
import io.appform.ranger.core.model.ServiceRegistry;
import io.appform.ranger.core.model.ShardSelector;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

public interface RangerHubClient<T, R extends ServiceRegistry<T>> {
    void start();

    void stop();

    Collection<Service> getRegisteredServices();

    CompletableFuture addService(Service service);

    Optional<ServiceNode<T>> getNode(final Service service);

    Optional<ServiceNode<T>> getNode(final Service service, final Predicate<T> criteria);

    Optional<ServiceNode<T>> getNode(
            final Service service,
            final Predicate<T> criteria,
            final ShardSelector<T, R> shardSelector);

    List<ServiceNode<T>> getAllNodes(final Service service);

    Optional<ServiceNode<T>> getNode(
            Service service,
            Predicate<T> criteria,
            ShardSelector<T, R> shardSelector,
            ServiceNodeSelector<T> nodeSelector);

    List<ServiceNode<T>> getAllNodes(final Service service, final Predicate<T> criteria);

    List<ServiceNode<T>> getAllNodes(
            final Service service,
            final Predicate<T> criteria,
            final ShardSelector<T, R> shardSelector);
}
