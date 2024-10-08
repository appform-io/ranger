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
import com.google.common.base.Preconditions;
import io.appform.ranger.core.model.NodeDataSource;
import io.appform.ranger.core.model.Service;
import io.appform.ranger.core.model.ServiceNode;
import io.appform.ranger.core.util.FinderUtils;
import io.appform.ranger.http.common.HttpNodeDataStoreConnector;
import io.appform.ranger.http.config.HttpClientConfig;
import io.appform.ranger.http.serde.HTTPResponseDataDeserializer;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 *
 */
@Slf4j
public class HttpNodeDataSource<T, D extends HTTPResponseDataDeserializer<T>> extends HttpNodeDataStoreConnector<T> implements NodeDataSource<T, D> {

    private final Service service;

    public HttpNodeDataSource(
            final Service service,
            final HttpClientConfig config,
            final ObjectMapper mapper,
            final OkHttpClient httpClient) {
        super(config, mapper, httpClient);
        this.service = service;
    }

    @Override
    public Optional<List<ServiceNode<T>>> refresh(D deserializer) {
        Preconditions.checkNotNull(config, "client config has not been set for node data");
        Preconditions.checkNotNull(mapper, "mapper has not been set for node data");
        val url = String.format("/ranger/nodes/v1/%s/%s", service.getNamespace(), service.getServiceName());

        log.debug("Refreshing the node list from url {}", url);
        val httpUrl = new HttpUrl.Builder()
                .scheme(config.isSecure()
                        ? "https"
                        : "http")
                .host(config.getHost())
                .port(config.getPort() == 0
                        ? defaultPort()
                        : config.getPort())
                .encodedPath(url)
                .build();
        val request = new Request.Builder()
                .url(httpUrl)
                .get()
                .build();

        try (val response = httpClient.newCall(request).execute()) {
            if (response.isSuccessful()) {
                try (val body = response.body()) {
                    if (null == body) {
                        log.warn("HTTP call to {} returned empty body", httpUrl);
                    } else {
                        val bytes = body.bytes();
                        val serviceNodesResponse = deserializer.deserialize(bytes);
                        if(serviceNodesResponse.valid()){
                            return Optional.of(FinderUtils.filterValidNodes(
                                               service,
                                               serviceNodesResponse.getData(),
                                               healthcheckZombieCheckThresholdTime(service)));
                        } else{
                            log.warn("Http call to {} returned a failure response with response {}", httpUrl, serviceNodesResponse);
                        }
                    }
                }
            } else {
                log.warn("HTTP call to {} returned: {}", httpUrl, response.code());
            }
        } catch (IOException e) {
            log.error("Error getting service data from the http endPoint: ", e);
        }
        return Optional.empty();
    }

    @Override
    public boolean isActive() {
        return true;
    }
}
