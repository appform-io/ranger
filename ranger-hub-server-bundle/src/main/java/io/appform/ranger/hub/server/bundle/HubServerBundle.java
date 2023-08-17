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
package io.appform.ranger.hub.server.bundle;

import com.codahale.metrics.health.HealthCheck;
import com.fasterxml.jackson.core.type.TypeReference;
import io.appform.ranger.client.RangerClientConstants;
import io.appform.ranger.client.RangerHubClient;
import io.appform.ranger.client.http.UnshardedRangerHttpHubClient;
import io.appform.ranger.client.zk.UnshardedRangerZKHubClient;
import io.appform.ranger.common.server.ShardInfo;
import io.appform.ranger.core.finder.serviceregistry.ListBasedServiceRegistry;
import io.appform.ranger.core.model.ServiceNode;
import io.appform.ranger.core.signals.Signal;
import io.appform.ranger.http.config.HttpClientConfig;
import io.appform.ranger.http.model.ServiceNodesResponse;
import io.appform.ranger.hub.server.bundle.configuration.RangerConfiguration;
import io.appform.ranger.hub.server.bundle.configuration.RangerHttpConfiguration;
import io.appform.ranger.hub.server.bundle.configuration.RangerZkConfiguration;
import io.appform.ranger.hub.server.bundle.healthcheck.RangerHealthCheck;
import io.appform.ranger.hub.server.bundle.lifecycle.CuratorLifecycle;
import io.appform.ranger.hub.server.bundle.models.BackendType.BackendTypeVisitor;
import io.appform.ranger.server.bundle.RangerServerBundle;
import io.dropwizard.Configuration;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryForever;

@Slf4j
@SuppressWarnings("unused")
@NoArgsConstructor
public abstract class HubServerBundle<U extends Configuration> extends
    RangerServerBundle<ShardInfo, ListBasedServiceRegistry<ShardInfo>, U> {

  protected abstract List<RangerConfiguration> getRangerConfiguration(U configuration);

  private final List<CuratorFramework> curatorFrameworks = new ArrayList<>();

  @Override
  protected List<RangerHubClient<ShardInfo, ListBasedServiceRegistry<ShardInfo>>> withHubs(
      U configuration) {
    return getRangerConfiguration(configuration).stream()
        .map(rangerConfiguration -> rangerConfiguration.getType().accept(new HubCreatorVisitor(), rangerConfiguration))
        .flatMap(Collection::stream)
        .collect(Collectors.toList());
  }

  @Override
  protected List<Signal<ShardInfo>> withLifecycleSignals(U configuration) {
    return curatorFrameworks.stream()
        .map(curatorFramework -> (Signal<ShardInfo>) new CuratorLifecycle(curatorFramework))
        .collect(Collectors.toList());
  }

  @Override
  protected List<HealthCheck> withHealthChecks(U configuration) {
    return curatorFrameworks.stream()
        .map(curatorFramework -> (HealthCheck) new RangerHealthCheck(curatorFramework))
        .collect(Collectors.toList());
  }

  private class HubCreatorVisitor implements
      BackendTypeVisitor<List<RangerHubClient<ShardInfo, ListBasedServiceRegistry<ShardInfo>>>, RangerConfiguration> {

    @Override
    public List<RangerHubClient<ShardInfo, ListBasedServiceRegistry<ShardInfo>>> visitZK(
        RangerConfiguration configuration) {
      val zkConfiguration = (RangerZkConfiguration) configuration;
      return zkConfiguration.getZookeepers().stream()
          .map(zk -> addCuratorAndGetZkHubClient(zk, zkConfiguration))
          .collect(Collectors.toList());
    }

    @Override
    public List<RangerHubClient<ShardInfo, ListBasedServiceRegistry<ShardInfo>>> visitHTTP(
        RangerConfiguration configuration) {
      val httpConfiguration = (RangerHttpConfiguration) configuration;
      return httpConfiguration.getHttpClientConfigs().stream()
          .map(http -> getHttpHubClient(http, httpConfiguration))
          .collect(Collectors.toList());
    }

    private RangerHubClient<ShardInfo, ListBasedServiceRegistry<ShardInfo>> addCuratorAndGetZkHubClient(
        String zookeeper, RangerZkConfiguration zkConfiguration) {
      val curatorFramework = CuratorFrameworkFactory.builder()
          .connectString(zookeeper)
          .namespace(zkConfiguration.getNamespace())
          .retryPolicy(new RetryForever(RangerClientConstants.CONNECTION_RETRY_TIME))
          .build();
      curatorFrameworks.add(curatorFramework);
      return UnshardedRangerZKHubClient.<ShardInfo>builder()
          .namespace(zkConfiguration.getNamespace())
          .connectionString(zookeeper)
          .curatorFramework(curatorFramework)
          .disablePushUpdaters(zkConfiguration.isDisablePushUpdaters())
          .mapper(getMapper())
          .nodeRefreshTimeMs(zkConfiguration.getNodeRefreshTimeMs())
          .deserializer(data -> {
            try {
              return getMapper().readValue(data, new TypeReference<ServiceNode<ShardInfo>>() {
              });
            } catch (IOException e) {
              log.warn("Error parsing node data with value {}", new String(data));
            }
            return null;
          })
          .build();
    }

    private RangerHubClient<ShardInfo, ListBasedServiceRegistry<ShardInfo>> getHttpHubClient(
        HttpClientConfig httpClientConfig, RangerHttpConfiguration httpConfiguration) {
      return UnshardedRangerHttpHubClient.<ShardInfo>builder()
          .namespace(httpConfiguration.getNamespace())
          .mapper(getMapper())
          .clientConfig(httpClientConfig)
          .nodeRefreshTimeMs(httpConfiguration.getNodeRefreshTimeMs())
          .deserializer(data -> {
            try {
              return getMapper().readValue(data,
                  new TypeReference<ServiceNodesResponse<ShardInfo>>() {
                  });
            } catch (IOException e) {
              log.warn("Error parsing node data with value {}", new String(data));
            }
            return null;
          })
          .build();
    }
  }

}
