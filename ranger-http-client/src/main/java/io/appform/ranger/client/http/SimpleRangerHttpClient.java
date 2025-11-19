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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.appform.ranger.client.AbstractRangerClient;
import io.appform.ranger.core.finder.SimpleUnshardedServiceFinder;
import io.appform.ranger.core.finder.serviceregistry.ListBasedServiceRegistry;
import io.appform.ranger.core.finder.shardselector.ListShardSelector;
import io.appform.ranger.core.model.ShardSelector;
import io.appform.ranger.http.HttpServiceFinderBuilders;
import io.appform.ranger.http.config.HttpClientConfig;
import io.appform.ranger.http.serde.HTTPResponseDataDeserializer;
import io.appform.ranger.http.servicefinder.HttpCommunicator;
import lombok.Builder;
import lombok.Getter;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

import static java.util.Objects.requireNonNull;

@Slf4j
@SuperBuilder
public class SimpleRangerHttpClient<T> extends AbstractRangerClient<T, ListBasedServiceRegistry<T>> {

    private final String serviceName;
    private final String namespace;
    private final ObjectMapper mapper;
    private final int nodeRefreshIntervalMs;
    private final HttpClientConfig clientConfig;
    private final HttpCommunicator<T> httpClient;
    private final HTTPResponseDataDeserializer<T> deserializer;
    @Builder.Default
    private final ShardSelector<T, ListBasedServiceRegistry<T>> shardSelector = new ListShardSelector<>();

    @Getter
    private SimpleUnshardedServiceFinder<T> serviceFinder;

    @Override
    public void start() {
        log.info("Starting the service finder");
        requireNonNull(mapper, "Mapper can't be null");
        requireNonNull(namespace, "namespace can't be null");
        requireNonNull(deserializer, "deserializer can't be null");

        this.serviceFinder = HttpServiceFinderBuilders.<T>httpUnshardedServiceFinderBuilider()
                .withClientConfig(clientConfig)
                .withServiceName(serviceName)
                .withNamespace(namespace)
                .withObjectMapper(mapper)
                .withHttpClient(httpClient)
                .withNodeRefreshIntervalMs(nodeRefreshIntervalMs)
                .withDeserializer(deserializer)
                .withShardSelector(shardSelector)
                .build();
        this.serviceFinder.start();
        log.info("Started the service finder");
    }

    @Override
    public void stop() {
        log.info("Stopping the service finder");
        if(null != serviceFinder){
            this.serviceFinder.stop();
        }
    }

}

