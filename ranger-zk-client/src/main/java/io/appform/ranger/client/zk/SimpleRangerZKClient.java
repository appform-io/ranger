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
package io.appform.ranger.client.zk;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import io.appform.ranger.client.AbstractRangerClient;
import io.appform.ranger.client.RangerClientConstants;
import io.appform.ranger.core.finder.SimpleShardedServiceFinder;
import io.appform.ranger.core.finder.serviceregistry.MapBasedServiceRegistry;
import io.appform.ranger.core.finder.shardselector.MatchingShardSelector;
import io.appform.ranger.zookeeper.ServiceFinderBuilders;
import io.appform.ranger.zookeeper.serde.ZkNodeDataDeserializer;
import lombok.Getter;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryForever;

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

    @Override
    public void start() {
        log.info("Starting the service finder");

        Preconditions.checkNotNull(mapper, "Mapper can't be null");
        Preconditions.checkNotNull(namespace, "namespace can't be null");
        Preconditions.checkNotNull(deserializer, "deserializer can't be null");

        int effectiveRefreshTime = nodeRefreshIntervalMs;

        if (effectiveRefreshTime < RangerClientConstants.MINIMUM_REFRESH_TIME) {
            effectiveRefreshTime = RangerClientConstants.MINIMUM_REFRESH_TIME;
            log.warn("Node info update interval too low: {} ms. Has been upgraded to {} ms ",
                    nodeRefreshIntervalMs,
                    RangerClientConstants.MINIMUM_REFRESH_TIME);
        }

        if(null == curatorFramework){
            Preconditions.checkNotNull(connectionString, "Connection string can't be null");
            curatorFramework = CuratorFrameworkFactory.newClient(connectionString, new RetryForever(RangerClientConstants.CONNECTION_RETRY_TIME));
        }

        this.serviceFinder = ServiceFinderBuilders.<T>shardedFinderBuilder()
                .withCuratorFramework(curatorFramework)
                .withNamespace(namespace)
                .withServiceName(serviceName)
                .withDeserializer(deserializer)
                .withNodeRefreshIntervalMs(effectiveRefreshTime)
                .withDisableWatchers(disableWatchers)
                .withShardSelector(new MatchingShardSelector<>())
                .build();

        this.serviceFinder.start();
    }

    @Override
    public void stop() {
        log.info("Stopping the service finder");
        this.serviceFinder.stop();
    }
}
