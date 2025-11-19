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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.appform.ranger.client.AbstractRangerClient;
import io.appform.ranger.core.finder.SimpleShardedServiceFinder;
import io.appform.ranger.core.finder.serviceregistry.MapBasedServiceRegistry;
import io.appform.ranger.core.finder.shardselector.MatchingShardSelector;
import io.appform.ranger.core.model.HubConstants;
import io.appform.ranger.core.model.ShardSelector;
import io.appform.ranger.zookeeper.ServiceFinderBuilders;
import io.appform.ranger.zookeeper.serde.ZkNodeDataDeserializer;
import lombok.Builder;
import lombok.Getter;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryForever;

import static java.util.Objects.requireNonNull;

@Slf4j
@Getter
@SuperBuilder
public class SimpleRangerZKClient<T> extends AbstractRangerClient<T, MapBasedServiceRegistry<T>> {

    private final String serviceName;
    private final String namespace;
    private final ObjectMapper mapper;
    private final boolean disableWatchers;
    private final String connectionString;
    private final ZkNodeDataDeserializer<T> deserializer;
    private CuratorFramework curatorFramework;
    private int nodeRefreshIntervalMs;
    private SimpleShardedServiceFinder<T> serviceFinder;
    @Builder.Default
    private ShardSelector<T, MapBasedServiceRegistry<T>> shardSelector = new MatchingShardSelector<>();

    @Override
    public void start() {
        log.info("Starting the service finder");

        requireNonNull(mapper, "Mapper can't be null");
        requireNonNull(namespace, "namespace can't be null");
        requireNonNull(deserializer, "deserializer can't be null");

        int effectiveRefreshTime = nodeRefreshIntervalMs;

        if (effectiveRefreshTime < HubConstants.MINIMUM_REFRESH_TIME_MS) {
            effectiveRefreshTime = HubConstants.MINIMUM_REFRESH_TIME_MS;
            log.warn("Node info update interval too low: {} ms. Has been upgraded to {} ms ",
                     nodeRefreshIntervalMs,
                    HubConstants.MINIMUM_REFRESH_TIME_MS);
        }

        if (null == curatorFramework) {
            requireNonNull(connectionString, "Connection string can't be null");
            curatorFramework = CuratorFrameworkFactory.builder()
                    .connectString(connectionString)
                    .namespace(namespace)
                    .retryPolicy(new RetryForever(HubConstants.CONNECTION_RETRY_TIME_MS))
                    .build();
        }

        this.serviceFinder = ServiceFinderBuilders.<T>shardedFinderBuilder()
                .withCuratorFramework(curatorFramework)
                .withNamespace(namespace)
                .withServiceName(serviceName)
                .withDeserializer(deserializer)
                .withNodeRefreshIntervalMs(effectiveRefreshTime)
                .withDisableWatchers(disableWatchers)
                .withShardSelector(shardSelector)
                .build();

        this.serviceFinder.start();
    }

    @Override
    public void stop() {
        log.info("Stopping the service finder");
        if (null != serviceFinder) {
            this.serviceFinder.stop();
        }
    }
}
