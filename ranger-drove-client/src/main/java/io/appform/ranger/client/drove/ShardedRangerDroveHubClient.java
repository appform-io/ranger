/*
 * Copyright 2015 Flipkart Internet Pvt. Ltd.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.appform.ranger.client.drove;

import io.appform.ranger.core.finder.serviceregistry.MapBasedServiceRegistry;
import io.appform.ranger.core.finder.shardselector.MatchingShardSelector;
import io.appform.ranger.core.finderhub.ServiceFinderFactory;
import io.appform.ranger.core.model.ShardSelector;
import io.appform.ranger.drove.serde.DroveResponseDataDeserializer;
import io.appform.ranger.drove.servicefinderhub.DroveShardedServiceFinderFactory;
import lombok.Builder;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@SuperBuilder
public class ShardedRangerDroveHubClient<T>
        extends AbstractRangerDroveHubClient<T, MapBasedServiceRegistry<T>, DroveResponseDataDeserializer<T>> {

    @Builder.Default
    private final ShardSelector<T, MapBasedServiceRegistry<T>> shardSelector = new MatchingShardSelector<>();

    @Override
    protected ServiceFinderFactory<T, MapBasedServiceRegistry<T>> getFinderFactory() {
        return DroveShardedServiceFinderFactory.<T>builder()
                .droveConfig(this.getClientConfig())
                .droveClient(this.getDroveClient())
                .nodeRefreshIntervalMs(getNodeRefreshTimeMs())
                .deserializer(getDeserializer())
                .shardSelector(shardSelector)
                .nodeSelector(this.getNodeSelector())
                .mapper(getMapper())
                .build();
    }

}
