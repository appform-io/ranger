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
package io.appform.ranger.drove.servicefinderhub;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.phonepe.drove.client.DroveClient;
import com.phonepe.drove.models.api.ApiErrorCode;
import com.phonepe.drove.models.api.ApiResponse;
import com.phonepe.drove.models.api.AppSummary;
import com.phonepe.drove.models.application.ApplicationState;
import io.appform.ranger.core.finderhub.ServiceDataSource;
import io.appform.ranger.core.model.Service;
import io.appform.ranger.drove.common.DroveNodeDataStoreConnector;
import io.appform.ranger.drove.config.DroveConfig;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.http.HttpStatus;

import java.util.Collection;
import java.util.List;
import java.util.Map;

@Slf4j
public class DroveServiceDataSource<T> extends DroveNodeDataStoreConnector<T> implements ServiceDataSource {
    private final String namespace;

    public DroveServiceDataSource(
            DroveConfig config,
            ObjectMapper mapper,
            String namespace,
            final DroveClient droveClient) {
        super(config, mapper, droveClient);
        this.namespace = namespace;
    }

    public DroveServiceDataSource(DroveConfig config, ObjectMapper mapper, String namespace) {
        super(config, mapper);
        this.namespace = namespace;
    }

    @Override
    public Collection<Service> services() {
        Preconditions.checkNotNull(config, "client config has not been set for node data");
        Preconditions.checkNotNull(mapper, "mapper has not been set for node data");
        val url = "/apis/v1/applications";
        return droveClient.execute(new DroveClient.Request(DroveClient.Method.GET, url),
                                   new DroveClient.ResponseHandler<>() {
                                       @Override
                                       public Collection<Service> defaultValue() {
                                           return List.of();
                                       }

                                       @Override
                                       public Collection<Service> handle(DroveClient.Response response) throws Exception {
                                           if (response.statusCode() == HttpStatus.SC_OK) {
                                               val apiResponse = mapper.readValue(
                                                       response.body(),
                                                       new TypeReference<ApiResponse<Map<String, AppSummary>>>() {
                                                       });
                                               if (apiResponse.getStatus().equals(ApiErrorCode.SUCCESS)) {
                                                   return apiResponse.getData()
                                                           .values()
                                                           .stream()
                                                           .filter(summary -> summary.getState()
                                                                   .equals(ApplicationState.RUNNING))
                                                           .map(AppSummary::getName)
                                                           .distinct()
                                                           .map(appName -> new Service(namespace, appName))
                                                           .toList();
                                               }
                                               else {
                                                   log.error("Error calling drove: " + apiResponse.getMessage());
                                               }
                                           }
                                           return List.of();
                                       }
                                   });
    }
}
