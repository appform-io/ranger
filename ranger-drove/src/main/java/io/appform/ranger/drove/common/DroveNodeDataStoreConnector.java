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
import com.phonepe.drove.client.DroveClient;
import io.appform.ranger.core.model.NodeDataStoreConnector;
import io.appform.ranger.drove.config.DroveUpstreamConfig;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/**
 *
 */
@Slf4j
public class DroveNodeDataStoreConnector<T> implements NodeDataStoreConnector<T> {

    protected final DroveUpstreamConfig config;
    protected final ObjectMapper mapper;
    protected final DroveClient droveClient;

    public DroveNodeDataStoreConnector(
            final DroveUpstreamConfig config,
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
