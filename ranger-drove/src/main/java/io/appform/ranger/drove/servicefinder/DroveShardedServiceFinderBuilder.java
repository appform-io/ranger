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
package io.appform.ranger.drove.servicefinder;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.phonepe.drove.client.DroveClient;
import io.appform.ranger.core.finder.SimpleShardedServiceFinder;
import io.appform.ranger.core.finder.SimpleShardedServiceFinderBuilder;
import io.appform.ranger.core.model.NodeDataSource;
import io.appform.ranger.core.model.Service;
import io.appform.ranger.drove.config.DroveUpstreamConfig;
import io.appform.ranger.drove.serde.DroveResponseDataDeserializer;
import io.appform.ranger.drove.utils.RangerDroveUtils;

import java.util.Objects;

/**
 *
 */

public class DroveShardedServiceFinderBuilder<T> extends SimpleShardedServiceFinderBuilder<T,
        DroveShardedServiceFinderBuilder<T>, DroveResponseDataDeserializer<T>> {

    private DroveUpstreamConfig clientConfig;
    private ObjectMapper mapper;
    private DroveClient droveClient = null;

    public DroveShardedServiceFinderBuilder<T> withDroveClient(final DroveClient droveClient) {
        this.droveClient = droveClient;
        return this;
    }

    public DroveShardedServiceFinderBuilder<T> withClientConfig(final DroveUpstreamConfig clientConfig) {
        this.clientConfig = clientConfig;
        return this;
    }

    public DroveShardedServiceFinderBuilder<T> withObjectMapper(final ObjectMapper mapper) {
        this.mapper = mapper;
        return this;
    }

    @Override
    public SimpleShardedServiceFinder<T> build() {
        return buildFinder();
    }

    @Override
    protected NodeDataSource<T, DroveResponseDataDeserializer<T>> dataSource(Service service) {
        return new DroveNodeDataSource<>(service, clientConfig, mapper,
                                         Objects.requireNonNullElseGet(droveClient,
                                                                       () -> RangerDroveUtils.buildDroveClient(
                                                                               clientConfig)));
    }

}
