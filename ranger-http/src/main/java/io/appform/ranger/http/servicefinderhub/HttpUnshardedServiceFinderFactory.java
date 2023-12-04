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
package io.appform.ranger.http.servicefinderhub;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.appform.ranger.core.finder.ServiceFinder;
import io.appform.ranger.core.finder.serviceregistry.ListBasedServiceRegistry;
import io.appform.ranger.core.finderhub.ServiceFinderFactory;
import io.appform.ranger.core.model.Service;
import io.appform.ranger.core.model.ServiceNodeSelector;
import io.appform.ranger.core.model.ShardSelector;
import io.appform.ranger.http.config.HttpClientConfig;
import io.appform.ranger.http.serde.HTTPResponseDataDeserializer;
import io.appform.ranger.http.servicefinder.HttpUnshardedServiceFinderBuilider;
import lombok.Builder;
import lombok.val;
import org.apache.hc.client5.http.fluent.Executor;

public class HttpUnshardedServiceFinderFactory<T> implements ServiceFinderFactory<T, ListBasedServiceRegistry<T>> {

    private final HttpClientConfig clientConfig;
    private final ObjectMapper mapper;
    private final HTTPResponseDataDeserializer<T> deserializer;
    private final ShardSelector<T, ListBasedServiceRegistry<T>> shardSelector;
    private final ServiceNodeSelector<T> nodeSelector;
    private final int nodeRefreshIntervalMs;
    private final Executor httpExecutor;

    @Builder
    public HttpUnshardedServiceFinderFactory(
            HttpClientConfig httpClientConfig,
            ObjectMapper mapper,
            HTTPResponseDataDeserializer<T> deserializer,
            ShardSelector<T, ListBasedServiceRegistry<T>> shardSelector,
            ServiceNodeSelector<T> nodeSelector,
            int nodeRefreshIntervalMs,
            Executor httpExecutor)
    {
        this.clientConfig = httpClientConfig;
        this.mapper = mapper;
        this.deserializer = deserializer;
        this.shardSelector = shardSelector;
        this.nodeSelector = nodeSelector;
        this.nodeRefreshIntervalMs = nodeRefreshIntervalMs;
        this.httpExecutor = httpExecutor;
    }

    @Override
    public ServiceFinder<T, ListBasedServiceRegistry<T>> buildFinder(Service service) {
        val serviceFinder = new HttpUnshardedServiceFinderBuilider<T>()
                .withClientConfig(clientConfig)
                .withObjectMapper(mapper)
                .withDeserializer(deserializer)
                .withNamespace(service.getNamespace())
                .withServiceName(service.getServiceName())
                .withNodeRefreshIntervalMs(nodeRefreshIntervalMs)
                .withShardSelector(shardSelector)
                .withNodeSelector(nodeSelector)
                .withHttpExecutor(httpExecutor)
                .build();
        serviceFinder.start();
        return serviceFinder;
    }
}
