/*
 * Copyright 2015 Flipkart Internet Pvt. Ltd.
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
package io.appform.ranger.http.server.bundle;

import com.codahale.metrics.health.HealthCheck;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableList;
import io.appform.ranger.client.RangerHubClient;
import io.appform.ranger.client.http.UnshardedRangerHttpHubClient;
import io.appform.ranger.common.server.ShardInfo;
import io.appform.ranger.http.model.ServiceNodesResponse;
import io.appform.ranger.http.server.bundle.config.HttpAppConfiguration;
import io.appform.ranger.http.server.bundle.healthcheck.RangerHttpHealthCheck;
import io.appform.ranger.zk.server.bundle.RangerServerBundle;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import javax.inject.Singleton;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Singleton
@NoArgsConstructor
@SuppressWarnings("unused")
public class HttpServerBundle extends RangerServerBundle<ShardInfo, HttpAppConfiguration> {

    @Override
    protected List<RangerHubClient<ShardInfo>> withHubs(HttpAppConfiguration configuration) {
        val rangerConfiguration = configuration.getRangerConfiguration();
        return rangerConfiguration.getHttpClientConfigs().stream().map(clientConfig -> UnshardedRangerHttpHubClient.<ShardInfo>builder()
                .namespace(rangerConfiguration.getNamespace())
                .mapper(getMapper())
                .clientConfig(clientConfig)
                .nodeRefreshTimeMs(rangerConfiguration.getNodeRefreshTimeMs())
                .deserializer(data -> {
                    try {
                        return getMapper().readValue(data, new TypeReference<ServiceNodesResponse<ShardInfo>>() {
                        });
                    } catch (IOException e) {
                        log.warn("Error parsing node data with value {}", new String(data));
                    }
                    return null;
                })
                .build()).collect(Collectors.toList());
    }

    @Override
    protected boolean withInitialRotationStatus(HttpAppConfiguration configuration) {
        return configuration.isInitialRotationStatus();
    }

    @Override
    protected List<HealthCheck> withHealthChecks(HttpAppConfiguration configuration) {
        return ImmutableList.of(new RangerHttpHealthCheck());
    }
}
