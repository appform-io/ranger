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
package io.appform.ranger.drove.servicefinder;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.phonepe.drove.client.DroveClient;
import com.phonepe.drove.models.api.ApiErrorCode;
import com.phonepe.drove.models.api.ApiResponse;
import com.phonepe.drove.models.api.ExposedAppInfo;
import io.appform.ranger.core.model.NodeDataSource;
import io.appform.ranger.core.model.Service;
import io.appform.ranger.core.model.ServiceNode;
import io.appform.ranger.core.util.FinderUtils;
import io.appform.ranger.drove.common.DroveNodeDataStoreConnector;
import io.appform.ranger.drove.config.DroveUpstreamConfig;
import io.appform.ranger.drove.serde.DroveResponseDataDeserializer;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.List;
import java.util.Optional;

/**
 *
 */
@Slf4j
public class DroveNodeDataSource<T, D extends DroveResponseDataDeserializer<T>> extends DroveNodeDataStoreConnector<T> implements NodeDataSource<T, D> {

    private final Service service;

    public DroveNodeDataSource(
            Service service,
            final DroveUpstreamConfig config,
            ObjectMapper mapper,
            DroveClient droveClient) {
        super(config, mapper, droveClient);
        this.service = service;
    }

    @Override
    public Optional<List<ServiceNode<T>>> refresh(D deserializer) {
        Preconditions.checkNotNull(config, "client config has not been set for node data");
        Preconditions.checkNotNull(mapper, "mapper has not been set for node data");
        val url = String.format("/apis/v1/endpoints/app/%s", service.getServiceName());

        log.debug("Refreshing the node list from url {}", url);
        val nodes = droveClient.execute(new DroveClient.Request(DroveClient.Method.GET, url),
                            new DroveClient.ResponseHandler<List<ServiceNode<T>>>() {
                                @Override
                                public List<ServiceNode<T>> defaultValue() {
                                    return List.of();
                                }

                                @Override
                                public List<ServiceNode<T>> handle(DroveClient.Response response) throws Exception {
                                    val apiResponse = mapper.readValue(response.body(),
                                                     new TypeReference<ApiResponse<List<ExposedAppInfo>>>() {});
                                    if (apiResponse.getStatus().equals(ApiErrorCode.FAILED)) {
                                        log.error("Could not read data from drove. Error: {}", apiResponse.getMessage());
                                        return List.of();
                                    }
                                    return deserializer.deserialize(apiResponse.getData());
                                }
                            });
        return Optional.of(FinderUtils.filterValidNodes(
                service,
                nodes,
                healthcheckZombieCheckThresholdTime(service)));
    }

    @Override
    public boolean isActive() {
        return true;
    }
}
