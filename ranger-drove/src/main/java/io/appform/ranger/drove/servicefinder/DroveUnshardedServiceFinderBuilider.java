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
package io.appform.ranger.drove.servicefinder;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.appform.ranger.core.finder.SimpleUnshardedServiceFinder;
import io.appform.ranger.core.finder.SimpleUnshardedServiceFinderBuilder;
import io.appform.ranger.core.model.NodeDataSource;
import io.appform.ranger.core.model.Service;
import io.appform.ranger.drove.common.DroveCommunicator;
import io.appform.ranger.drove.config.DroveUpstreamConfig;
import io.appform.ranger.drove.serde.DroveResponseDataDeserializer;
import io.appform.ranger.drove.utils.RangerDroveUtils;

import java.util.Objects;

public class DroveUnshardedServiceFinderBuilider<T>
        extends SimpleUnshardedServiceFinderBuilder<T, DroveUnshardedServiceFinderBuilider<T>,
        DroveResponseDataDeserializer<T>> {

    private DroveUpstreamConfig clientConfig;
    private ObjectMapper mapper;
    private DroveCommunicator droveCommunicator;

    public DroveUnshardedServiceFinderBuilider<T> withDroveCommunicator(final DroveCommunicator droveClient) {
        this.droveCommunicator = droveClient;
        return this;
    }

    public DroveUnshardedServiceFinderBuilider<T> withClientConfig(final DroveUpstreamConfig clientConfig) {
        this.clientConfig = clientConfig;
        return this;
    }

    public DroveUnshardedServiceFinderBuilider<T> withObjectMapper(final ObjectMapper mapper) {
        this.mapper = mapper;
        return this;
    }

    @Override
    public SimpleUnshardedServiceFinder<T> build() {
        return buildFinder();
    }

    @Override
    protected NodeDataSource<T, DroveResponseDataDeserializer<T>> dataSource(Service service) {
        return new DroveNodeDataSource<>(
                service,
                clientConfig,
                mapper,
                Objects.requireNonNullElseGet(droveCommunicator,
                                              () -> RangerDroveUtils.buildDroveClient(namespace, clientConfig, mapper)));
    }

}

