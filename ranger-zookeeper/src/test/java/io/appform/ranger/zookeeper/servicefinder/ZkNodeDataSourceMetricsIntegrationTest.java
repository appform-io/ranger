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

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.appform.ranger.core.model.Service;
import io.appform.ranger.core.model.ServiceNode;
import io.appform.ranger.core.units.TestNodeData;
import io.appform.ranger.core.util.MetricRecorder;
import io.appform.ranger.zookeeper.serde.ZkNodeDataDeserializer;
import io.appform.ranger.zookeeper.util.PathBuilder;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingCluster;
import org.apache.zookeeper.CreateMode;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for ZkNodeDataSource metrics recording.
 * Tests verify that metrics are pushed through actual ZK operations.
 */
class ZkNodeDataSourceMetricsIntegrationTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String METRIC_PREFIX = "io.appform.ranger";
    private static final String NAMESPACE = "test";
    private static final String SERVICE_NAME = "node-source-svc";

    private TestingCluster testingCluster;
    private CuratorFramework curatorFramework;
    private MetricRegistry metricRegistry;

    @BeforeEach
    void setUp() throws Exception {
        metricRegistry = new MetricRegistry();
        MetricRecorder.initialize(metricRegistry);

        testingCluster = new TestingCluster(3);
        testingCluster.start();

        curatorFramework = CuratorFrameworkFactory.builder()
                .namespace(NAMESPACE)
                .connectString(testingCluster.getConnectString())
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .build();
        curatorFramework.start();
        curatorFramework.blockUntilConnected();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (curatorFramework != null) {
            curatorFramework.close();
        }
        if (testingCluster != null) {
            testingCluster.close();
        }
    }

    // ==================== isActive() - Active connection ====================

    @Test
    void testIsActive_connectedZk_recordsActiveStatus() {
        val service = new Service(NAMESPACE, SERVICE_NAME);
        val dataSource = new ZkNodeDataSource<TestNodeData, ZkNodeDataDeserializer<TestNodeData>>(
                "zk-node-src-1", service, curatorFramework);
        dataSource.start();

        val active = dataSource.isActive();

        assertTrue(active);

        // Verify active status metric
        val activeMeter = metricRegistry.getMeters().get(
                METRIC_PREFIX + ".dataStoreType.ZK.dataSource.zk-node-src-1.active");
        assertNotNull(activeMeter, "Active status meter should be recorded");
        assertEquals(1, activeMeter.getCount());

        dataSource.stop();
    }

    // ==================== isActive() - Disconnected ====================

    @Test
    void testIsActive_disconnectedZk_recordsInactiveStatus() throws Exception {
        val service = new Service(NAMESPACE, SERVICE_NAME);
        val disconnectedCurator = CuratorFrameworkFactory.builder()
                .namespace(NAMESPACE)
                .connectString("127.0.0.1:19999") // non-existent ZK
                .retryPolicy(new ExponentialBackoffRetry(100, 1))
                .sessionTimeoutMs(500)
                .connectionTimeoutMs(500)
                .build();
        disconnectedCurator.start();
        // Don't wait for connection — it should fail

        val dataSource = new ZkNodeDataSource<TestNodeData, ZkNodeDataDeserializer<TestNodeData>>(
                "zk-node-src-2", service, disconnectedCurator);
        // Don't call start() — it would block waiting for connection

        val active = dataSource.isActive();

        assertFalse(active);

        // Verify inactive status metric
        val inactiveMeter = metricRegistry.getMeters().get(
                METRIC_PREFIX + ".dataStoreType.ZK.dataSource.zk-node-src-2.inactive");
        assertNotNull(inactiveMeter, "Inactive status meter should be recorded");
        assertEquals(1, inactiveMeter.getCount());

        disconnectedCurator.close();
    }

    // ==================== refresh() - Success with nodes ====================

    @Test
    void testRefresh_withNodes_noEmptyMetric() throws Exception {
        val service = new Service(NAMESPACE, SERVICE_NAME);
        val dataSource = new ZkNodeDataSource<TestNodeData, ZkNodeDataDeserializer<TestNodeData>>(
                "zk-node-src-3", service, curatorFramework);
        dataSource.start();

        // Create service path and child nodes in ZK
        val servicePath = PathBuilder.servicePath(service);
        curatorFramework.create().creatingParentContainersIfNeeded().forPath(servicePath);

        val node1 = ServiceNode.<TestNodeData>builder()
                .host("host1").port(8080)
                .nodeData(TestNodeData.builder().shardId(1).build())
                .build();
        val nodeData1 = MAPPER.writeValueAsBytes(node1);
        curatorFramework.create().withMode(CreateMode.EPHEMERAL)
                .forPath(servicePath + "/" + node1.representation(), nodeData1);

        val result = dataSource.refresh(validDeserializer());

        assertTrue(result.isPresent());
        assertEquals(1, result.get().size());
        assertEquals("host1", result.get().get(0).getHost());

        // No null/empty list node response metric should be recorded
        val emptyMeter = metricRegistry.getMeters().get(
                METRIC_PREFIX + ".dataStoreType.ZK.dataSource.zk-node-src-3.httpCall.listNodes.serviceName."
                        + SERVICE_NAME + ".nullOrEmptyResponse");
        assertNull(emptyMeter, "No empty list node metric should be recorded when nodes exist");

        dataSource.stop();
    }

    // ==================== refresh() - Empty children ====================

    @Test
    void testRefresh_emptyChildren_recordsNullOrEmptyListNodeResponse() throws Exception {
        val service = new Service(NAMESPACE, SERVICE_NAME);
        val dataSource = new ZkNodeDataSource<TestNodeData, ZkNodeDataDeserializer<TestNodeData>>(
                "zk-node-src-4", service, curatorFramework);
        dataSource.start();

        // Create service path but no child nodes
        val servicePath = PathBuilder.servicePath(service);
        curatorFramework.create().creatingParentContainersIfNeeded().forPath(servicePath);

        val result = dataSource.refresh(validDeserializer());

        assertTrue(result.isPresent());
        assertTrue(result.get().isEmpty());

        // Verify null/empty list node response metric
        val aggregateMeter = metricRegistry.getMeters().get(
                METRIC_PREFIX + ".dataStoreType.ZK.dataSource.zk-node-src-4.httpCall.listNodes.nullOrEmptyResponse");
        assertNotNull(aggregateMeter, "Aggregate empty list node meter should be recorded");
        assertEquals(1, aggregateMeter.getCount());

        val emptyMeter = metricRegistry.getMeters().get(
                METRIC_PREFIX + ".dataStoreType.ZK.dataSource.zk-node-src-4.httpCall.listNodes.serviceName."
                        + SERVICE_NAME + ".nullOrEmptyResponse");
        assertNotNull(emptyMeter, "Empty list node meter should be recorded");
        assertEquals(1, emptyMeter.getCount());

        dataSource.stop();
    }

    // ==================== refresh() - NoNodeException (path doesn't exist) ====================

    @Test
    void testRefresh_noServicePath_recordsNullOrEmptyListNodeResponse() throws Exception {
        val service = new Service(NAMESPACE, "nonexistent-svc");
        val dataSource = new ZkNodeDataSource<TestNodeData, ZkNodeDataDeserializer<TestNodeData>>(
                "zk-node-src-5", service, curatorFramework);
        dataSource.start();

        // Don't create the service path — triggers NoNodeException

        val result = dataSource.refresh(validDeserializer());

        assertTrue(result.isPresent());
        assertTrue(result.get().isEmpty());

        // Verify null/empty list node response metric (NoNodeException path)
        val aggregateMeter = metricRegistry.getMeters().get(
                METRIC_PREFIX + ".dataStoreType.ZK.dataSource.zk-node-src-5.httpCall.listNodes.nullOrEmptyResponse");
        assertNotNull(aggregateMeter, "Aggregate empty list node meter should be recorded for NoNodeException");
        assertTrue(aggregateMeter.getCount() >= 1);

        val emptyMeter = metricRegistry.getMeters().get(
                METRIC_PREFIX + ".dataStoreType.ZK.dataSource.zk-node-src-5.httpCall.listNodes.serviceName.nonexistent-svc.nullOrEmptyResponse");
        assertNotNull(emptyMeter, "Empty list node meter should be recorded for NoNodeException");
        assertTrue(emptyMeter.getCount() >= 1);

        dataSource.stop();
    }

    // ==================== refresh() - Deserializer failure (parse failure) ====================

    @Test
    void testRefresh_deserializerThrows_recordsListNodesParseFailure() throws Exception {
        val service = new Service(NAMESPACE, SERVICE_NAME);
        val dataSource = new ZkNodeDataSource<TestNodeData, ZkNodeDataDeserializer<TestNodeData>>(
                "zk-node-src-6", service, curatorFramework);
        dataSource.start();

        // Create service path with a child node containing bad data
        val servicePath = PathBuilder.servicePath(service);
        curatorFramework.create().creatingParentContainersIfNeeded().forPath(servicePath);
        curatorFramework.create().withMode(CreateMode.EPHEMERAL)
                .forPath(servicePath + "/bad-node", "invalid-json{{{".getBytes());

        // Deserializer that always throws
        ZkNodeDataDeserializer<TestNodeData> badDeserializer = data -> {
            throw new RuntimeException("Parse failure simulation");
        };

        assertThrows(RuntimeException.class, () -> dataSource.refresh(badDeserializer));

        // Verify list nodes parse failure metric
        val aggregateParseMeter = metricRegistry.getMeters().get(
                METRIC_PREFIX + ".dataStoreType.ZK.dataSource.zk-node-src-6.httpCall.listNodes.responseParseFailure");
        assertNotNull(aggregateParseMeter, "Aggregate list nodes parse failure meter should be recorded");
        assertEquals(1, aggregateParseMeter.getCount());

        val parseMeter = metricRegistry.getMeters().get(
                METRIC_PREFIX + ".dataStoreType.ZK.dataSource.zk-node-src-6.httpCall.listNodes.serviceName."
                        + SERVICE_NAME + ".responseParseFailure");
        assertNotNull(parseMeter, "List nodes parse failure meter should be recorded");
        assertEquals(1, parseMeter.getCount());

        dataSource.stop();
    }

    // ==================== refresh() - Not started ====================

    @Test
    void testRefresh_notStarted_returnsEmpty() {
        val service = new Service(NAMESPACE, SERVICE_NAME);
        val dataSource = new ZkNodeDataSource<TestNodeData, ZkNodeDataDeserializer<TestNodeData>>(
                "zk-node-src-7", service, curatorFramework);
        // Intentionally do NOT call start()

        val result = dataSource.refresh(validDeserializer());

        assertFalse(result.isPresent());
    }

    // ==================== refresh() - Stopped ====================

    @Test
    void testRefresh_stopped_returnsEmpty() {
        val service = new Service(NAMESPACE, SERVICE_NAME);
        val dataSource = new ZkNodeDataSource<TestNodeData, ZkNodeDataDeserializer<TestNodeData>>(
                "zk-node-src-8", service, curatorFramework);
        dataSource.start();
        dataSource.stop();

        val result = dataSource.refresh(validDeserializer());

        assertFalse(result.isPresent());
    }

    // ==================== Helper ====================

    @SneakyThrows
    private static ServiceNode<TestNodeData> deserializeNode(byte[] data) {
        return MAPPER.readValue(data, new TypeReference<ServiceNode<TestNodeData>>() {});
    }

    private ZkNodeDataDeserializer<TestNodeData> validDeserializer() {
        return ZkNodeDataSourceMetricsIntegrationTest::deserializeNode;
    }
}
