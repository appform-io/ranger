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
package io.appform.ranger.http.serviceprovider;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.appform.ranger.core.model.NodeDataSink;
import io.appform.ranger.core.model.Service;
import io.appform.ranger.core.serviceprovider.BaseServiceProviderBuilder;
import io.appform.ranger.core.serviceprovider.ServiceProvider;
import io.appform.ranger.http.config.HttpClientConfig;
import io.appform.ranger.http.serde.HttpRequestDataSerializer;
import io.appform.ranger.http.servicefinder.HttpCommunicator;
import io.appform.ranger.http.utils.RangerHttpUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;

@Slf4j
public class HttpShardedServiceProviderBuilder<T> extends BaseServiceProviderBuilder<T, HttpShardedServiceProviderBuilder<T>, HttpRequestDataSerializer<T>> {

    private HttpClientConfig clientConfig;
    private ObjectMapper mapper;
    private HttpCommunicator<T> httpClient;

    public HttpShardedServiceProviderBuilder<T> withClientConfiguration(final HttpClientConfig clientConfig) {
        this.clientConfig = clientConfig;
        return this;
    }

    public HttpShardedServiceProviderBuilder<T> withObjectMapper(final ObjectMapper mapper){
        this.mapper = mapper;
        return this;
    }

    public HttpShardedServiceProviderBuilder<T> withHttpClient(final HttpCommunicator<T> httpClient) {
        this.httpClient = httpClient;
        return this;
    }

    @Override
    public ServiceProvider<T, HttpRequestDataSerializer<T>> build() {
        return super.buildProvider();
    }

    @Override
    protected NodeDataSink<T, HttpRequestDataSerializer<T>> dataSink(Service service) {
        return new HttpNodeDataSink<>(service, clientConfig, mapper,
                                      Objects.requireNonNullElseGet(httpClient,
                                                                    () -> RangerHttpUtils.httpClient(clientConfig, mapper)));
    }
}
