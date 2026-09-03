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
package io.appform.ranger.zookeeper.serviceprovider;


import io.appform.ranger.core.model.DataStoreType;
import io.appform.ranger.core.model.NodeDataSink;
import io.appform.ranger.core.model.Service;
import io.appform.ranger.core.model.ServiceNode;
import io.appform.ranger.core.util.Exceptions;
import io.appform.ranger.core.util.MetricRecorder;
import io.appform.ranger.zookeeper.common.ZkNodeDataStoreConnector;
import io.appform.ranger.zookeeper.common.ZkStoreType;
import io.appform.ranger.zookeeper.serde.ZkNodeDataSerializer;
import io.appform.ranger.zookeeper.util.PathBuilder;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import static io.appform.ranger.core.util.MetricRecorder.FAILURE;
import static io.appform.ranger.core.util.MetricRecorder.SUCCESS;
import static java.util.Objects.requireNonNull;

/**
 *
 */
@Slf4j
public class ZkNodeDataSink<T, S extends ZkNodeDataSerializer<T>> extends ZkNodeDataStoreConnector<T> implements NodeDataSink<T,S> {
    public ZkNodeDataSink(
            String upstreamId, Service service,
            CuratorFramework curatorFramework) {
        super(upstreamId, service, curatorFramework, ZkStoreType.SINK);
    }

    @Override
    public DataStoreType getDataStoreType() {
        return DataStoreType.ZK;
    }

    @Override
    public String getUpstreamId() {
        return upstreamId;
    }

    @Override
    public void updateState(S serializer, ServiceNode<T> serviceNode) {
        if (isStopped()) {
            log.warn("Node has been stopped already for service: {}. No update will be possible.",
                     service.getServiceName());
            return;
        }
        requireNonNull(serializer, "Serializer has not been set for node data");
        val path = PathBuilder.instancePath(service, serviceNode);
        try {
            if (null == curatorFramework.checkExists().forPath(path)) {
                log.info("No node exists for path: {}. Will create now.", path);
                createPath(service.getServiceName(), serviceNode, serializer);
            }
            else {
                val serviceData = getSerializedData(service.getServiceName(), serializer, serviceNode);
                curatorFramework.setData().forPath(path, serviceData);
            }
            MetricRecorder.recordNodeDataSinkUpdateStatus(getDataStoreType(), upstreamId, SUCCESS);
        }
        catch (Exception e) {
            log.error("Error updating node data at path " + path, e);
            MetricRecorder.recordNodeDataSinkUpdateStatus(getDataStoreType(), upstreamId, FAILURE);
            Exceptions.illegalState(e);
        }
    }

    private <T, S extends ZkNodeDataSerializer<T>> byte[] getSerializedData(String serviceName, S serializer, ServiceNode<T> serviceNode) {
        try {
            return serializer.serialize(serviceNode);
        } catch (Exception e) {
            MetricRecorder.recordNodeDataSinkSerDeFailure(getDataStoreType(), upstreamId, MetricRecorder.SERIALIZATION, serviceName, e.getClass().getSimpleName());
            throw e;
        }
    }

    private synchronized void createPath(
            String serviceName, ServiceNode<T> serviceNode,
            S serializer) {
        val instancePath = PathBuilder.instancePath(service, serviceNode);
        try {
            if (null == curatorFramework.checkExists().forPath(instancePath)) {
                curatorFramework.create()
                        .creatingParentContainersIfNeeded()
                        .withMode(CreateMode.EPHEMERAL)
                        .forPath(instancePath, getSerializedData(serviceName, serializer, serviceNode));
                log.info("Created instance path: {}", instancePath);
            }
        }
        catch (KeeperException.NodeExistsException e) {
            log.warn("Node already exists.. Race condition?", e);
        }
        catch (Exception e) {
            MetricRecorder.recordNodeDataSinkUnknownFailure(getDataStoreType(), upstreamId, service.getServiceName(), e.getClass().getSimpleName());
            val message = String.format(
                    "Could not create node for %s after 60 retries (1 min). " +
                            "This service will not be discoverable. Retry after some time.", service.getServiceName());
            log.error(message, e);
            Exceptions.illegalState(message, e);
        }
    }


}
