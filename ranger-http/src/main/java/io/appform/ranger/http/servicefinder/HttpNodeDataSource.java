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


import io.appform.ranger.core.model.DataStoreType;
import io.appform.ranger.core.model.NodeDataSource;
import io.appform.ranger.core.model.Service;
import io.appform.ranger.core.model.ServiceNode;
import io.appform.ranger.core.util.MetricRecorder;
import io.appform.ranger.http.common.HttpNodeDataStoreConnector;
import io.appform.ranger.http.config.HttpClientConfig;
import io.appform.ranger.http.serde.HTTPResponseDataDeserializer;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 */
@Slf4j
public class HttpNodeDataSource<T, D extends HTTPResponseDataDeserializer<T>> extends HttpNodeDataStoreConnector<T> implements NodeDataSource<T, D> {

    private final String upstreamId;
    private final Service service;
    private final AtomicBoolean upstreamAvailable = new AtomicBoolean(true);
    private final ScheduledExecutorService resetter = Executors.newSingleThreadScheduledExecutor();

    public HttpNodeDataSource(
            final String upstreamId,
            final Service service,
            final HttpClientConfig config,
            final HttpCommunicator<T> httpCommunicator) {
        super(config, httpCommunicator);
        Objects.requireNonNull(config, "client config has not been set for node data");
        Objects.requireNonNull(httpCommunicator, "http communicator has not been set for node data");
        this.upstreamId = upstreamId;
        this.service = service;
        resetter.scheduleWithFixedDelay(() -> upstreamAvailable.set(true), 0, 60, TimeUnit.SECONDS);
    }

    public String getUpstreamId() {
        return upstreamId;
    }

    @Override
    public DataStoreType getDataStoreType() {
        return DataStoreType.HTTP;
    }

    @Override
    public Optional<List<ServiceNode<T>>> refresh(D deserializer) {
        return Optional.of(httpCommunicator.listNodes(service, deserializer));
    }

    @Override
    public boolean isActive() {
        var httpUpstreamAvailable = upstreamAvailable.get();
        MetricRecorder.recordNodeDataSourceStatus(DataStoreType.HTTP, upstreamId, httpUpstreamAvailable);
        return httpUpstreamAvailable;
    }
}
