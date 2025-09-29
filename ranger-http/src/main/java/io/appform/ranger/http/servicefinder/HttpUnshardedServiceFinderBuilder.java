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
package io.appform.ranger.http.servicefinder;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.appform.ranger.core.finder.SimpleUnshardedServiceFinder;
import io.appform.ranger.core.finder.SimpleUnshardedServiceFinderBuilder;
import io.appform.ranger.core.model.NodeDataSource;
import io.appform.ranger.core.model.Service;
import io.appform.ranger.http.config.HttpClientConfig;
import io.appform.ranger.http.serde.HTTPResponseDataDeserializer;
import io.appform.ranger.http.utils.RangerHttpUtils;

import java.util.Objects;

public class HttpUnshardedServiceFinderBuilder<T>
        extends SimpleUnshardedServiceFinderBuilder<T, HttpUnshardedServiceFinderBuilder<T>, HTTPResponseDataDeserializer<T>> {

    private HttpClientConfig clientConfig;
    private ObjectMapper mapper;
    private HttpCommunicator<T> httpClient;

    public HttpUnshardedServiceFinderBuilder<T> withClientConfig(final HttpClientConfig clientConfig) {
        this.clientConfig = clientConfig;
        return this;
    }

    public HttpUnshardedServiceFinderBuilder<T> withObjectMapper(final ObjectMapper mapper) {
        this.mapper = mapper;
        return this;
    }

    public HttpUnshardedServiceFinderBuilder<T> withHttpClient(final HttpCommunicator<T> httpClient) {
        this.httpClient = httpClient;
        return this;
    }

    @Override
    public SimpleUnshardedServiceFinder<T> build() {
        return buildFinder();
    }

    @Override
    protected NodeDataSource<T, HTTPResponseDataDeserializer<T>> dataSource(Service service) {
        return new HttpNodeDataSource<>(service, clientConfig,
                                        Objects.requireNonNullElseGet(httpClient,
                                                                      () -> RangerHttpUtils.httpClient(clientConfig, mapper)));
    }

}

