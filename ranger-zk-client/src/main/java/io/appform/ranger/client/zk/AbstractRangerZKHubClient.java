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
package io.appform.ranger.client.zk;

import io.appform.ranger.client.AbstractRangerHubClient;
import io.appform.ranger.core.finderhub.ServiceDataSource;
import io.appform.ranger.core.finderhub.ServiceFinderHub;
import io.appform.ranger.core.model.ServiceRegistry;
import io.appform.ranger.zookeeper.serde.ZkNodeDataDeserializer;
import io.appform.ranger.zookeeper.servicefinderhub.ZkServiceDataSource;
import io.appform.ranger.zookeeper.servicefinderhub.ZkServiceFinderHubBuilder;
import lombok.Getter;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;

@Slf4j
@Getter
@SuperBuilder
public abstract class AbstractRangerZKHubClient<T, R extends ServiceRegistry<T>, D extends ZkNodeDataDeserializer<T>>
        extends AbstractRangerHubClient<T, R, D> {

    private final boolean disablePushUpdaters;
    private final String connectionString;
    private final CuratorFramework curatorFramework;

    @Override
    protected ServiceFinderHub<T, R> buildHub() {
       return new ZkServiceFinderHubBuilder<T, R>()
                .withCuratorFramework(curatorFramework)
                .withConnectionString(connectionString)
                .withNamespace(getNamespace())
                .withRefreshFrequencyMs(getNodeRefreshTimeMs())
                .withServiceDataSource(getServiceDataSource())
                .withServiceFinderFactory(getFinderFactory())
                .withHubRefreshDuration(getHubStartTimeoutMs())
                .withServiceRefreshDuration(getServiceRefreshTimeoutMs())
                .withExcludedServices(getExcludedServices())
                .build();
    }

    @Override
    protected ServiceDataSource getDefaultDataSource() {
        return new ZkServiceDataSource(getNamespace(), connectionString, curatorFramework);
    }

}

