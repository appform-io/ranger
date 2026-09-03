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

import io.appform.ranger.core.model.DataStoreType;
import io.appform.ranger.core.model.NodeDataSource;
import io.appform.ranger.core.model.Service;
import io.appform.ranger.core.model.ServiceNode;
import io.appform.ranger.core.util.MetricRecorder;
import io.appform.ranger.zookeeper.common.ZkNodeDataStoreConnector;
import io.appform.ranger.zookeeper.common.ZkStoreType;
import io.appform.ranger.zookeeper.serde.ZkNodeDataDeserializer;
import io.appform.ranger.zookeeper.util.PathBuilder;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static io.appform.ranger.core.util.MetricRecorder.LIST_NODES;
import static java.util.Objects.requireNonNull;

/**
 *
 */
@Slf4j
public class ZkNodeDataSource<T, D extends ZkNodeDataDeserializer<T>> extends ZkNodeDataStoreConnector<T> implements NodeDataSource<T, D> {

    public ZkNodeDataSource(
            String upstreamId, Service service,
            CuratorFramework curatorFramework) {
        super(upstreamId, service, curatorFramework, ZkStoreType.SOURCE);
    }

    @Override
    public String getUpstreamId() {
        return upstreamId;
    }

    @Override
    public DataStoreType getDataStoreType() {
        return DataStoreType.ZK;
    }

    @Override
    public Optional<List<ServiceNode<T>>> refresh(D deserializer) {
        return checkForUpdateOnZookeeper(deserializer);
    }

    private Optional<List<ServiceNode<T>>> checkForUpdateOnZookeeper(D deserializer) {
        if (!isStarted()) {
            log.warn("Data source is not yet started for service: {}. No nodes will be returned.",
                     service.getServiceName());
            return Optional.empty();
        }
        if (isStopped()) {
            log.warn("Data source is  stopped already for service: {}. No nodes will be returned.",
                     service.getServiceName());
            return Optional.empty();
        }
        requireNonNull(deserializer, "Deserializer has not been set for node data");
        val serviceName = service.getServiceName();
        try {
            if (!isActive()) {
                log.warn("ZK connection is not active. Ignoring refresh request for service: {}",
                         service.getServiceName());
                return Optional.empty();
            }
            val parentPath = PathBuilder.servicePath(service);
            log.debug("Looking for node list of [{}]", serviceName);
            val children = curatorFramework.getChildren().forPath(parentPath);
            List<ServiceNode<T>> nodes = new ArrayList<>(children.size());
            log.debug("Found {} nodes for [{}]", children.size(), serviceName);
            if(children.isEmpty()){
                recordNullOrEmptyResponse(serviceName);
            }
            for (val child : children) {
                byte[] data = readChild(serviceName, parentPath, child).orElse(null);
                if (data == null || data.length == 0) {
                    continue;
                }
                final var node = parseServiceNodeData(serviceName, deserializer, data);
                nodes.add(node);
            }
            return Optional.of(nodes);
        }
        catch (NoNodeException e) {
            recordNullOrEmptyResponse(serviceName);
            log.error(
                    "No ZK container node found for service: {}. Will return empty list for now. Please doublecheck service name",
                    service.getServiceName());
            return Optional.of(Collections.emptyList());
        }
        catch (Exception e) {
            MetricRecorder.recordZookeeperReadUnknownFailure(DataStoreType.ZK, upstreamId, LIST_NODES, e.getClass().getSimpleName());
            log.error("Error getting node data from zookeeper: ", e);
            throw new ZkCommunicationException("Error getting node data from zookeeper: exception %s , message: %s"
                    .formatted(e.getClass().getSimpleName(), e.getMessage()));
        }
    }

    private <T, D extends ZkNodeDataDeserializer<T>> ServiceNode<T> parseServiceNodeData(String serviceName, D deserializer, byte[] data) {
        try {
            return deserializer.deserialize(data);
        } catch (Exception e) {
            MetricRecorder.recordListNodesParseFailure(DataStoreType.ZK, upstreamId);
            MetricRecorder.recordListNodesParseFailure(DataStoreType.ZK, upstreamId, serviceName);
            log.error("Error deserializing node data : {} for service name: {} ", new String(data), serviceName, e);
            throw e;
        }
    }

    private Optional<byte[]> readChild(String serviceName, String parentPath, String child) throws Exception {
        final String path = String.format("%s/%s", parentPath, child);
        try {
            return Optional.ofNullable(curatorFramework.getData().forPath(path));
        }
        catch (KeeperException.NoNodeException e) {
            recordNullOrEmptyResponse(serviceName);
            log.warn("Node not found for path {}", path);
            return Optional.empty();
        }
        catch (KeeperException e) {
            recordNullOrEmptyResponse(serviceName);
            log.error("Could not get data for node: {}", path, e);
            return Optional.empty();
        } catch (Exception e){
            MetricRecorder.recordZookeeperReadUnknownFailure(DataStoreType.ZK, upstreamId, LIST_NODES, e.getClass().getSimpleName());
            log.error("Could not read child for node: {}", path, e);
            throw e;
        }
    }

    private void recordNullOrEmptyResponse(String serviceName) {
        MetricRecorder.recordNullOrEmptyListNodeResponse(DataStoreType.ZK, upstreamId);
        MetricRecorder.recordNullOrEmptyListNodeResponse(DataStoreType.ZK, upstreamId, serviceName);
    }

}
