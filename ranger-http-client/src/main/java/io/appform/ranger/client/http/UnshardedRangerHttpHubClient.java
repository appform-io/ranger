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
package io.appform.ranger.client.http;

import io.appform.ranger.core.finder.serviceregistry.ListBasedServiceRegistry;
import io.appform.ranger.core.finder.shardselector.ListShardSelector;
import io.appform.ranger.core.finderhub.ServiceFinderFactory;
import io.appform.ranger.core.model.ShardSelector;
import io.appform.ranger.http.serde.HTTPResponseDataDeserializer;
import io.appform.ranger.http.servicefinderhub.HttpUnshardedServiceFinderFactory;
import lombok.Builder;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@SuperBuilder
public class UnshardedRangerHttpHubClient<T>
        extends AbstractRangerHttpHubClient<T, ListBasedServiceRegistry<T>, HTTPResponseDataDeserializer<T>> {

    @Builder.Default
    private final ShardSelector<T, ListBasedServiceRegistry<T>> shardSelector = new ListShardSelector<>();

    @Override
    protected ServiceFinderFactory<T, ListBasedServiceRegistry<T>> getFinderFactory() {
        return HttpUnshardedServiceFinderFactory.<T>builder()
                .httpClientConfig(this.getClientConfig())
                .httpClient(this.getHttpClient())
                .nodeRefreshIntervalMs(getNodeRefreshTimeMs())
                .deserializer(getDeserializer())
                .shardSelector(shardSelector)
                .nodeSelector(this.getNodeSelector())
                .mapper(getMapper())
                .build();
    }

}
