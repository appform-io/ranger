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
package io.appform.ranger.http.serviceprovider;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import io.appform.ranger.core.model.NodeDataSink;
import io.appform.ranger.core.model.Service;
import io.appform.ranger.core.model.ServiceNode;
import io.appform.ranger.core.util.Exceptions;
import io.appform.ranger.http.common.HttpNodeDataStoreConnector;
import io.appform.ranger.http.config.HttpClientConfig;
import io.appform.ranger.http.model.ServiceRegistrationResponse;
import io.appform.ranger.http.serde.HttpRequestDataSerializer;
import io.appform.ranger.http.utils.HttpClientUtils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.hc.client5.http.fluent.Executor;
import org.apache.hc.client5.http.fluent.Request;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.io.entity.ByteArrayEntity;
import org.apache.hc.core5.net.URIBuilder;
import org.apache.hc.core5.util.Timeout;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

@Slf4j
public class HttpNodeDataSink<T, S extends HttpRequestDataSerializer<T>> extends HttpNodeDataStoreConnector<T> implements NodeDataSink<T, S> {

    private final Service service;

    public HttpNodeDataSink(Service service, HttpClientConfig config, ObjectMapper mapper, Executor executor) {
        super(config, mapper, executor);
        this.service = service;
    }

    @Override
    @SneakyThrows
    public void updateState(S serializer, ServiceNode<T> serviceNode) {
        Preconditions.checkNotNull(config, "client config has not been set for node data");
        Preconditions.checkNotNull(mapper, "mapper has not been set for node data");

        val url = String.format("/ranger/nodes/v1/add/%s/%s", service.getNamespace(), service.getServiceName());
        log.debug("Updating state at the url {}", url);

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

        val request = Request.post(httpUrl)
                .body(new ByteArrayEntity(serializer.serialize(serviceNode), ContentType.APPLICATION_JSON));

        HttpClientUtils.executeRequest(httpExecutor, request, (Function<byte[], Optional<ServiceRegistrationResponse<T>>>) responseBytes -> {
            val serviceRegistrationResponse = getServiceRegistrationResponse(responseBytes);

            if (null == serviceRegistrationResponse || !serviceRegistrationResponse.valid()) {
                log.warn("Http call to {} returned a failure response {}", httpUrl, serviceRegistrationResponse);
                Exceptions.illegalState("Error updating state on the server for node data: " + httpUrl);
            }

            return Optional.of(serviceRegistrationResponse);
        }, new Function<Exception, Optional<ServiceRegistrationResponse<T>>>() {
            @Override
            @SneakyThrows
            public Optional<ServiceRegistrationResponse<T>> apply(Exception exception) {
                throw exception;
            }
        });
    }

    @SneakyThrows
    private ServiceRegistrationResponse<T> getServiceRegistrationResponse(final byte[] responseBytes) {
        return mapper.readValue(responseBytes, new TypeReference<>() {});
    }
}
