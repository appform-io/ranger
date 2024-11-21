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
package io.appform.ranger.drove.servicefinderhub;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import io.appform.ranger.core.finderhub.ServiceDataSource;
import io.appform.ranger.core.model.Service;
import io.appform.ranger.drove.common.DroveNodeDataStoreConnector;
import io.appform.ranger.drove.config.DroveUpstreamConfig;
import io.appform.ranger.drove.common.DroveCommunicator;
import java.util.Collections;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;

@Slf4j
public class DroveServiceDataSource<T> extends DroveNodeDataStoreConnector<T> implements ServiceDataSource {
    private final String namespace;

    public DroveServiceDataSource(
            final DroveUpstreamConfig config,
            final ObjectMapper mapper,
            final String namespace,
            final DroveCommunicator droveClient) {
        super(config, mapper, droveClient);
        this.namespace = namespace;
    }

    @Override
    public Collection<Service> services() {
        Preconditions.checkNotNull(config, "client config has not been set for node data");
        Preconditions.checkNotNull(mapper, "mapper has not been set for node data");
        return droveClient.services()
                .stream()
                .map(serviceName -> new Service(namespace, serviceName))
                .toList();
    }
}
