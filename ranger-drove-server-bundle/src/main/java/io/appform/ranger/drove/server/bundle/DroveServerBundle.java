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
package io.appform.ranger.drove.server.bundle;

import com.codahale.metrics.health.HealthCheck;
import com.google.common.base.Strings;
import com.phonepe.drove.models.api.ExposedAppInfo;
import io.appform.ranger.client.RangerHubClient;
import io.appform.ranger.client.drove.UnshardedRangerDroveHubClient;
import io.appform.ranger.common.server.ShardInfo;
import io.appform.ranger.core.finder.serviceregistry.ListBasedServiceRegistry;
import io.appform.ranger.drove.serde.DroveResponseDataDeserializer;
import io.appform.ranger.drove.server.bundle.config.RangerDroveConfiguration;
import io.appform.ranger.server.bundle.RangerServerBundle;
import io.dropwizard.Configuration;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import javax.inject.Singleton;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@Slf4j
@Singleton
@NoArgsConstructor
@SuppressWarnings("unused")
public abstract class DroveServerBundle<U extends Configuration> extends RangerServerBundle<ShardInfo,
        ListBasedServiceRegistry<ShardInfo>, U> {

    protected abstract RangerDroveConfiguration getRangerConfiguration(U configuration);

    @Override
    protected List<RangerHubClient<ShardInfo, ListBasedServiceRegistry<ShardInfo>>> withHubs(U configuration) {
        val rangerConfiguration = getRangerConfiguration(configuration);
        val envTagName = Objects.requireNonNullElse(rangerConfiguration.getEnvironmentTagName(),
                                                    RangerDroveConfiguration.DEFAULT_ENVIRONMENT_TAG_NAME);
        val regionTagName = Objects.requireNonNullElse(rangerConfiguration.getRegionTagName(),
                                                       RangerDroveConfiguration.DEFAULT_REGION_TAG_NAME);
        return rangerConfiguration.getDroveConfigs().stream()
                .map(clientConfig -> UnshardedRangerDroveHubClient.<ShardInfo>builder()
                        .namespace(rangerConfiguration.getNamespace())
                        .mapper(getMapper())
                        .clientConfig(clientConfig)
                        .nodeRefreshTimeMs(rangerConfiguration.getNodeRefreshTimeMs())
                        .deserializer(new DroveResponseDataDeserializer<>() {
                            @Override
                            protected ShardInfo translate(ExposedAppInfo appInfo, ExposedAppInfo.ExposedHost host) {
                                val tags = Objects.<Map<String, String>>requireNonNullElse(
                                        appInfo.getTags(), Collections.emptyMap());
                                val env = tags.get(envTagName);
                                val region = tags.get(regionTagName);
                                if (Strings.isNullOrEmpty(env) || Strings.isNullOrEmpty(region)) {
                                    return null;
                                }
                                return ShardInfo.builder()
                                        .environment(env)
                                        .region(region)
                                        .tags(tags.entrySet()
                                                      .stream()
                                                      .map(entry -> entry.getKey() + "|" + entry.getValue())
                                                      .collect(Collectors.toUnmodifiableSet()))
                                        .build();
                            }
                        })
                        .build())
                .collect(Collectors.toList());
    }

    protected List<HealthCheck> withHealthChecks(U configuration) {
        return List.of(new RangerDroveHealthCheck());
    }
}
