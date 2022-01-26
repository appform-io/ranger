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
package io.appform.ranger.server.bundle;

import com.codahale.metrics.health.HealthCheck;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableList;
import io.appform.ranger.client.RangerClientConstants;
import io.appform.ranger.client.RangerHubClient;
import io.appform.ranger.client.zk.UnshardedRangerZKHubClient;
import io.appform.ranger.common.server.ShardInfo;
import io.appform.ranger.core.finder.serviceregistry.ListBasedServiceRegistry;
import io.appform.ranger.core.model.ServiceNode;
import io.appform.ranger.core.signals.Signal;
import io.appform.ranger.server.bundle.config.RangerConfiguration;
import io.appform.ranger.server.bundle.healthcheck.RangerHealthCheck;
import io.appform.ranger.server.bundle.lifecycle.CuratorLifecycle;
import io.dropwizard.Configuration;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryForever;

import javax.inject.Singleton;
import java.io.IOException;
import java.util.List;

@Slf4j
@Singleton
@NoArgsConstructor
public abstract class ZKServerBundle<U extends Configuration> extends RangerServerBundle<ShardInfo, ListBasedServiceRegistry<ShardInfo>, U> {

    private CuratorFramework curatorFramework;

    protected abstract RangerConfiguration getRangerConfiguration(U configuration);

    @Override
    protected void preBundle(U configuration) {
        val rangerConfiguration = getRangerConfiguration(configuration);
        curatorFramework = CuratorFrameworkFactory.builder()
                .connectString(rangerConfiguration.getZookeeper())
                .namespace(rangerConfiguration.getNamespace())
                .retryPolicy(new RetryForever(RangerClientConstants.CONNECTION_RETRY_TIME))
                .build();
    }

    @Override
    protected List<RangerHubClient<ShardInfo, ListBasedServiceRegistry<ShardInfo>>> withHubs(U configuration) {
        val rangerConfiguration = getRangerConfiguration(configuration);
        return ImmutableList.of(UnshardedRangerZKHubClient.<ShardInfo>builder()
                .namespace(rangerConfiguration.getNamespace())
                .connectionString(rangerConfiguration.getZookeeper())
                .curatorFramework(curatorFramework)
                .disablePushUpdaters(rangerConfiguration.isDisablePushUpdaters())
                .mapper(getMapper())
                .nodeRefreshTimeMs(rangerConfiguration.getNodeRefreshTimeMs())
                .deserializer(data -> {
                    try {
                        return getMapper().readValue(data, new TypeReference<ServiceNode<ShardInfo>>() {
                        });
                    } catch (IOException e) {
                        log.warn("Error parsing node data with value {}", new String(data));
                    }
                    return null;
                })
                .build());
    }

    protected List<Signal<ShardInfo>> withLifecycleSignals(U configuration) {
        return ImmutableList.of(
                new CuratorLifecycle(curatorFramework)
        );
    }

    protected List<HealthCheck> withHealthChecks(U configuration) {
        return ImmutableList.of(new RangerHealthCheck(curatorFramework));
    }
}
