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
package io.appform.ranger.zookeeper.servicefinder;

import com.google.common.base.Preconditions;
import io.appform.ranger.core.finder.SimpleShardedServiceFinder;
import io.appform.ranger.core.finder.SimpleShardedServiceFinderBuilder;
import io.appform.ranger.core.model.NodeDataSource;
import io.appform.ranger.core.model.Service;
import io.appform.ranger.core.signals.Signal;
import io.appform.ranger.zookeeper.serde.ZkNodeDataDeserializer;
import io.appform.ranger.zookeeper.servicefinder.signals.ZkWatcherRegistryUpdateSignal;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.Collections;
import java.util.List;

/**
 *
 */
@Slf4j
public class ZkSimpleShardedServiceFinderBuilder<T> extends SimpleShardedServiceFinderBuilder<T, ZkSimpleShardedServiceFinderBuilder<T>, ZkNodeDataDeserializer<T>> {
    protected CuratorFramework curatorFramework;
    protected String connectionString;

    public ZkSimpleShardedServiceFinderBuilder<T> withCuratorFramework(CuratorFramework curatorFramework) {
        this.curatorFramework = curatorFramework;
        return this;
    }

    public ZkSimpleShardedServiceFinderBuilder<T> withConnectionString(final String connectionString) {
        this.connectionString = connectionString;
        return this;
    }

    @Override
    public SimpleShardedServiceFinder<T> build() {
        val curatorProvided = curatorFramework != null;
        if (!curatorProvided) {
            Preconditions.checkNotNull(connectionString);
            curatorFramework = CuratorFrameworkFactory.builder()
                    .namespace(namespace)
                    .connectString(connectionString)
                    .retryPolicy(new ExponentialBackoffRetry(1000, 100)).build();
            super.withStartSignalHandler(x -> curatorFramework.start());
            super.withStopSignalHandler(x -> curatorFramework.close());
        }
        return buildFinder();
    }

    @Override
    protected NodeDataSource<T, ZkNodeDataDeserializer<T>> dataSource(Service service) {
        return new ZkNodeDataSource<>(service, curatorFramework);
    }

    @Override
    protected List<Signal<T>> implementationSpecificRefreshSignals(final Service service, final NodeDataSource<T, ZkNodeDataDeserializer<T>> nodeDataSource) {
        if (!disablePushUpdaters) {
            return Collections.singletonList(
                    new ZkWatcherRegistryUpdateSignal<>(service, nodeDataSource, curatorFramework));
        }
        else {
            log.info("Push based signal updater not registered for service: {}", service.getServiceName());
        }
        return Collections.emptyList();
    }
}
