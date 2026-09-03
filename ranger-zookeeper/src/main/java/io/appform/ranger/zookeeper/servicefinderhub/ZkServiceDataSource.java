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
package io.appform.ranger.zookeeper.servicefinderhub;


import io.appform.ranger.core.finderhub.ServiceDataSource;
import io.appform.ranger.core.model.Service;
import io.appform.ranger.core.model.DataStoreType;
import io.appform.ranger.core.util.MetricRecorder;
import io.appform.ranger.zookeeper.util.PathBuilder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static io.appform.ranger.core.util.MetricRecorder.*;
import static java.util.Objects.requireNonNull;

/**
 *
 */
@Slf4j
public class ZkServiceDataSource implements ServiceDataSource {

    private final String upstreamId;
    private final String namespace;
    private final String connectionString;
    private CuratorFramework curatorFramework;
    private boolean curatorProvided;

    public ZkServiceDataSource(String upstreamId,
                               String namespace,
                               String connectionString,
                               CuratorFramework curatorFramework){
        this.upstreamId = upstreamId;
        this.namespace = namespace;
        this.connectionString = connectionString;
        this.curatorFramework = curatorFramework;
    }

    @Override
    @SneakyThrows
    public Collection<Service> services() {
        try {
            val children = curatorFramework.getChildren()
                    .forPath(PathBuilder.REGISTERED_SERVICES_PATH);
            val result = getServices(children);
            MetricRecorder.recordServicesFetchStatus(DataStoreType.ZK, upstreamId, SUCCESS);
            return result;
        }
        catch (Exception e) {
            MetricRecorder.recordZookeeperReadUnknownFailure(DataStoreType.ZK, upstreamId, SERVICES_LIST, e.getClass().getSimpleName());
            MetricRecorder.recordServicesFetchStatus(DataStoreType.ZK, upstreamId, FAILURE);
            throw e;
        }
    }

    private Set<Service> getServices(List<String> children) {
        if(children == null || children.isEmpty()) {
            MetricRecorder.recordNullOrEmptyServicesListResponse(DataStoreType.ZK, upstreamId);
            log.warn("No services found for namespace: {} in zk data source with metric id: {}", namespace, upstreamId);
            return Collections.<Service>emptySet();
        } else {
            return children.stream()
                    .map(child -> Service.builder().namespace(namespace).serviceName(child).build())
                    .collect(Collectors.toSet());
        }
    }

    @Override
    public void start() {
        if(null == curatorFramework){
            requireNonNull(connectionString);
            log.info("Building custom curator framework");
            curatorFramework = CuratorFrameworkFactory.builder()
                    .namespace(namespace)
                    .connectString(connectionString)
                    .retryPolicy(new ExponentialBackoffRetry(1000, 100))
                    .build();
            curatorFramework.start();
            curatorProvided = false;
        }
        try {
            curatorFramework.blockUntilConnected();
        }
        catch (InterruptedException e) {
            log.error("Curator block interrupted", e);
            Thread.currentThread().interrupt();
        }
        log.info("Service data source started. Curator state is: {}", curatorFramework.getState().name());
    }

    @Override
    public void stop() {
        log.info("Service data stopped");
        if(!curatorProvided) curatorFramework.close();
        log.info("Service data source stopped");
    }
}
