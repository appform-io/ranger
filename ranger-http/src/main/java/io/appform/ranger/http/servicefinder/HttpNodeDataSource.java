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
package io.appform.ranger.http.servicefinder;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import io.appform.ranger.core.model.NodeDataSource;
import io.appform.ranger.core.model.Service;
import io.appform.ranger.core.model.ServiceNode;
import io.appform.ranger.core.util.FinderUtils;
import io.appform.ranger.http.common.HttpNodeDataStoreConnector;
import io.appform.ranger.http.config.HttpClientConfig;
import io.appform.ranger.http.serde.HTTPResponseDataDeserializer;
import io.appform.ranger.http.utils.HttpClientUtils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.hc.client5.http.fluent.Executor;
import org.apache.hc.client5.http.fluent.Request;
import org.apache.hc.core5.net.URIBuilder;
import org.apache.hc.core5.util.Timeout;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 *
 */
@Slf4j
public class HttpNodeDataSource<T, D extends HTTPResponseDataDeserializer<T>> extends HttpNodeDataStoreConnector<T> implements NodeDataSource<T, D> {

    private final Service service;

    public HttpNodeDataSource(
            Service service,
            final HttpClientConfig config,
            ObjectMapper mapper,
            Executor executor) {
        super(config, mapper, executor);
        this.service = service;
    }

    @Override
    @SneakyThrows
    public Optional<List<ServiceNode<T>>> refresh(D deserializer) {
        Preconditions.checkNotNull(config, "client config has not been set for node data");
        Preconditions.checkNotNull(mapper, "mapper has not been set for node data");
        val url = String.format("/ranger/nodes/v1/%s/%s", service.getNamespace(), service.getServiceName());

        log.debug("Refreshing the node list from url {}", url);

        val httpUrl = new URIBuilder()
                .setScheme(config.isSecure()
                        ? "https"
                        : "http")
                .setHost(config.getHost())
                .setPort(config.getPort() == 0
                        ? defaultPort()
                        : config.getPort())
                .setPath(url)
                .build();

        return HttpClientUtils.executeRequest(httpExecutor, Request.get(httpUrl), (Function<byte[], Optional<List<ServiceNode<T>>>>) responseBytes -> {
            val serviceNodesResponse = deserializer.deserialize(responseBytes);
            if (serviceNodesResponse.valid()) {
                return Optional.of(FinderUtils.filterValidNodes(
                        service,
                        serviceNodesResponse.getData(),
                        healthcheckZombieCheckThresholdTime(service)));
            } else {
                log.warn("Http call to {} returned a failure response with response {}", httpUrl, serviceNodesResponse);
                return Optional.empty();
            }
        }, (Function<Exception, Optional<List<ServiceNode<T>>>>) exception -> {
            log.error("HTTP call to {} returned with an exception. Executing the error handler", httpUrl, exception);
            return Optional.empty();
        });
    }
}
