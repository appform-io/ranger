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
package io.appform.ranger.drove.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.phonepe.drove.client.DroveClient;
import com.phonepe.drove.client.transport.httpcomponent.DroveHttpComponentsTransport;
import io.appform.ranger.core.model.NodeDataStoreConnector;
import io.appform.ranger.drove.config.DroveConfig;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hc.core5.http.HttpHeaders;

import java.util.List;

/**
 *
 */
@Slf4j
public class DroveNodeDataStoreConnector<T> implements NodeDataStoreConnector<T> {

    protected final DroveConfig config;
    protected final ObjectMapper mapper;
    protected final DroveClient droveClient;

    public DroveNodeDataStoreConnector(
            final DroveConfig config,
            final ObjectMapper mapper) {
        this(config,
             mapper,
             new DroveClient(config.getCluster(),
                                      Strings.isNullOrEmpty(config.getAuthHeader())
                                      ? List.of()
                                      : List.of(request -> request.headers().put(HttpHeaders.AUTHORIZATION, List.of(config.getAuthHeader()))),
                                           new DroveHttpComponentsTransport(config.getCluster())));
    }

    public DroveNodeDataStoreConnector(
            final DroveConfig config,
            final ObjectMapper mapper,
            final DroveClient droveClient) {
        this.config = config;
        this.mapper = mapper;
        this.droveClient = droveClient;
    }


    @Override
    public void start() {
        //Nothing to do here
    }

    @Override
    @SneakyThrows
    public void ensureConnected() {
        do {
            Thread.sleep(1_000);

        } while (droveClient.leader().orElse(null) == null);
    }

    @Override
    public void stop() {
        //Nothing to do here
    }

    @Override
    public boolean isActive() {
        return droveClient.leader().isPresent();
    }
}
