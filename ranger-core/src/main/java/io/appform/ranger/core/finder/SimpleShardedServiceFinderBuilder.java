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

import io.appform.ranger.core.finder.serviceregistry.MapBasedServiceRegistry;
import io.appform.ranger.core.finder.shardselector.MatchingShardSelector;
import io.appform.ranger.core.model.Deserializer;
import io.appform.ranger.core.model.Service;
import io.appform.ranger.core.model.ServiceNodeSelector;
import io.appform.ranger.core.model.ShardSelector;

public abstract class SimpleShardedServiceFinderBuilder<T, B extends SimpleShardedServiceFinderBuilder<T,B, D>, D extends Deserializer<T>>
        extends BaseServiceFinderBuilder<T, MapBasedServiceRegistry<T>, SimpleShardedServiceFinder<T>, B, D> {

    @Override
    protected SimpleShardedServiceFinder<T> buildFinder(
            Service service,
            ShardSelector<T, MapBasedServiceRegistry<T>> shardSelector,
            ServiceNodeSelector<T> nodeSelector
    ) {
        if (null == shardSelector) {
            shardSelector = new MatchingShardSelector<>();
        }
        return new SimpleShardedServiceFinder<>(new MapBasedServiceRegistry<>(service), shardSelector, nodeSelector);
    }
}
