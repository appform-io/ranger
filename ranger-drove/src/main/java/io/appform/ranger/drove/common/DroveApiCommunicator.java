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

package io.appform.ranger.drove.common;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.phonepe.drove.client.DroveClient;
import com.phonepe.drove.models.api.ApiErrorCode;
import com.phonepe.drove.models.api.ApiResponse;
import com.phonepe.drove.models.api.AppSummary;
import com.phonepe.drove.models.api.ExposedAppInfo;
import com.phonepe.drove.models.application.ApplicationState;
import io.appform.ranger.core.model.Service;
import io.appform.ranger.drove.config.DroveUpstreamConfig;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.http.HttpStatus;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 *
 */
@Slf4j
public class DroveApiCommunicator implements DroveCommunicator {
    private final String namespace;
    private final DroveUpstreamConfig config;
    private final DroveClient droveClient;
    private final ObjectMapper mapper;

    public DroveApiCommunicator(
            String namespace, DroveUpstreamConfig config,
            DroveClient droveClient,
            ObjectMapper mapper) {
        this.namespace = namespace;
        this.config = config;
        this.droveClient = droveClient;
        this.mapper = mapper;
    }

    @Override
    public void close() {
        droveClient.close();
    }

    @Override
    public Optional<String> leader() {
        return droveClient.leader();
    }

    @Override
    public List<String> services() {
        log.debug("Loading services list");
        val skipTagName = Objects.requireNonNullElse(
                config.getSkipTagName(),
                DroveUpstreamConfig.DEFAULT_SKIP_TAG_NAME);
        val url = "/apis/v1/applications";
        return droveClient.execute(
                new DroveClient.Request(DroveClient.Method.GET, url),
                new DroveClient.ResponseHandler<>() {
                    @Override
                    public List<String> defaultValue() {
                        throw new IllegalStateException("Default value should not be used here");
                    }

                    @Override
                    public List<String> handle(DroveClient.Response response) throws Exception {
                        if (response.statusCode() != HttpStatus.SC_OK) {
                            throw new DroveCommunicationException("Error communicating to drove: " + response);
                        }
                        val apiResponse = mapper.readValue(
                                response.body(),
                                new TypeReference<ApiResponse<Map<String, AppSummary>>>() {
                                });
                        if (!apiResponse.getStatus().equals(ApiErrorCode.SUCCESS)) {
                            log.error("Error calling drove: " + apiResponse.getMessage());
                            throwDroveCommError(response);
                        }
                        return apiResponse.getData()
                                .values()
                                .stream()
                                .filter(summary -> summary.getState()
                                        .equals(ApplicationState.RUNNING))
                                .filter(summary -> summary.getTags() == null
                                        || !summary.getTags()
                                        .getOrDefault(skipTagName, "false")
                                        .equals("true"))
                                .map(AppSummary::getName)
                                .distinct()
                                .toList();
                    }
                });
    }

    @Override
    @SuppressWarnings("java:S1168")
    public Map<Service, List<ExposedAppInfo>> listNodes(Iterable<? extends Service> services) {
        log.debug("Loading nodes list for services: {}", Lists.newArrayList(services));
        val url = String.format("/apis/v1/endpoints?%s", Joiner.on("&")
                .join(StreamSupport.stream(services.spliterator(), false)
                              .map(service -> "app=" + service.getServiceName())
                              .toList()));

        logUrl(url);
        return droveClient.execute(new DroveClient.Request(DroveClient.Method.GET, url),
                                   new DroveClient.ResponseHandler<>() {
                                       @Override
                                       public Map<Service, List<ExposedAppInfo>> defaultValue() {
                                           throw new IllegalStateException("Default value should not be used here");
                                       }

                                       @Override
                                       public Map<Service, List<ExposedAppInfo>> handle(DroveClient.Response response) throws Exception {
                                           if (response.statusCode() != HttpStatus.SC_OK) {
                                               throwDroveCommError(response);
                                           }
                                           val apiResponse = mapper.readValue(response.body(),
                                                                              new TypeReference<ApiResponse<List<ExposedAppInfo>>>() {
                                                                              });
                                           if (!apiResponse.getStatus().equals(ApiErrorCode.SUCCESS)) {
                                               throwDroveCommError(response);
                                           }
                                           val data = Objects.requireNonNullElse(apiResponse.getData(),
                                                                                 List.<ExposedAppInfo>of());
                                           return data.stream()
                                                   .filter(appInfo -> !Strings.isNullOrEmpty(appInfo.getAppName()))
                                                   .collect(Collectors.groupingBy(
                                                           appInfo -> new Service(namespace, appInfo.getAppName()),
                                                           Collectors.toList()));
                                       }
                                   });
    }

    private static void throwDroveCommError(DroveClient.Response response) {
        log.error("Error calling drove: [{}] {}", response.statusCode(), response.body());
        throw new DroveCommunicationException("Error status: " + response.statusCode() + " body: " + response.body());
    }

    private static void logUrl(String url) {
        log.debug("Refreshing the node list from url {}", url);
    }

}
