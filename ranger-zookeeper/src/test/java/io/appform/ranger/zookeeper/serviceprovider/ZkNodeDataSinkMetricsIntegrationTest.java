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

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.appform.ranger.core.model.Service;
import io.appform.ranger.core.model.ServiceNode;
import io.appform.ranger.core.units.TestNodeData;
import io.appform.ranger.core.util.MetricRecorder;
import io.appform.ranger.zookeeper.serde.ZkNodeDataSerializer;
import lombok.val;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingCluster;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for ZkNodeDataSink metrics recording.
 * Tests verify that metrics are pushed through actual ZK write operations.
 */
class ZkNodeDataSinkMetricsIntegrationTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String METRIC_PREFIX = "io.appform.ranger";
    private static final String NAMESPACE = "test";
    private static final String SERVICE_NAME = "sink-test-svc";

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

    // ==================== updateState() - Success (create new node) ====================

    @Test
    void testUpdateState_createNewNode_recordsSuccess() {
        val service = new Service(NAMESPACE, SERVICE_NAME);
        val sink = new ZkNodeDataSink<TestNodeData, ZkNodeDataSerializer<TestNodeData>>(
                "zk-sink-1", service, curatorFramework);
        sink.start();

        val serviceNode = ServiceNode.<TestNodeData>builder()
                .host("host1").port(8080)
                .nodeData(TestNodeData.builder().shardId(1).build())
                .build();

        ZkNodeDataSerializer<TestNodeData> serializer = node -> {
            try {
                return MAPPER.writeValueAsBytes(node);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };

        sink.updateState(serializer, serviceNode);

        // Verify success meter
        val successMeter = metricRegistry.getMeters().get(
                METRIC_PREFIX + ".dataStoreType.ZK.dataSource.zk-sink-1.nodeDataSink.update.success");
        assertNotNull(successMeter, "Node data sink update success meter should be recorded");
        assertEquals(1, successMeter.getCount());

        // No failure
        val failureMeter = metricRegistry.getMeters().get(
                METRIC_PREFIX + ".dataStoreType.ZK.dataSource.zk-sink-1.nodeDataSink.update.failure");
        assertNull(failureMeter, "No failure meter should be recorded on success");

        sink.stop();
    }

    // ==================== updateState() - Success (update existing node) ====================

    @Test
    void testUpdateState_updateExistingNode_recordsSuccess() {
        val service = new Service(NAMESPACE, SERVICE_NAME);
        val sink = new ZkNodeDataSink<TestNodeData, ZkNodeDataSerializer<TestNodeData>>(
                "zk-sink-2", service, curatorFramework);
        sink.start();

        val serviceNode = ServiceNode.<TestNodeData>builder()
                .host("host2").port(8081)
                .nodeData(TestNodeData.builder().shardId(2).build())
                .build();

        ZkNodeDataSerializer<TestNodeData> serializer = node -> {
            try {
                return MAPPER.writeValueAsBytes(node);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };

        // First call creates
        sink.updateState(serializer, serviceNode);
        // Second call updates
        sink.updateState(serializer, serviceNode);

        // Verify 2 success markers
        val successMeter = metricRegistry.getMeters().get(
                METRIC_PREFIX + ".dataStoreType.ZK.dataSource.zk-sink-2.nodeDataSink.update.success");
        assertNotNull(successMeter, "Success meter should exist");
        assertEquals(2, successMeter.getCount());

        sink.stop();
    }

    // ==================== updateState() - Serialization failure ====================

    @Test
    void testUpdateState_serializerThrows_recordsSerDeFailure() {
        val service = new Service(NAMESPACE, SERVICE_NAME);
        val sink = new ZkNodeDataSink<TestNodeData, ZkNodeDataSerializer<TestNodeData>>(
                "zk-sink-3", service, curatorFramework);
        sink.start();

        val serviceNode = ServiceNode.<TestNodeData>builder()
                .host("host3").port(8082)
                .nodeData(TestNodeData.builder().shardId(3).build())
                .build();

        // Serializer that throws
        ZkNodeDataSerializer<TestNodeData> badSerializer = node -> {
            throw new RuntimeException("Serialization failed");
        };

        // updateState should propagate the exception (wrapped in IllegalStateException)
        assertThrows(Exception.class, () -> sink.updateState(badSerializer, serviceNode));

        // Verify serialization failure metric
        val serdeMeter = metricRegistry.getMeters().get(
                METRIC_PREFIX + ".dataStoreType.ZK.dataSource.zk-sink-3.nodeDataSink.serialization.failure");
        assertNotNull(serdeMeter, "Serialization failure meter should be recorded");
        assertTrue(serdeMeter.getCount() >= 1);

        // Verify service-specific serialization failure
        val svcSerdeMeter = metricRegistry.getMeters().get(
                METRIC_PREFIX + ".dataStoreType.ZK.dataSource.zk-sink-3.nodeDataSink.serialization.serviceName."
                        + SERVICE_NAME + ".failure");
        assertNotNull(svcSerdeMeter, "Service-specific serialization failure meter should be recorded");
        assertTrue(svcSerdeMeter.getCount() >= 1);

        sink.stop();
    }

    // ==================== updateState() - Stopped state (no-op) ====================

    @Test
    void testUpdateState_stoppedSink_noMetricsRecorded() {
        val service = new Service(NAMESPACE, SERVICE_NAME);
        val sink = new ZkNodeDataSink<TestNodeData, ZkNodeDataSerializer<TestNodeData>>(
                "zk-sink-4", service, curatorFramework);
        sink.start();
        sink.stop();

        val serviceNode = ServiceNode.<TestNodeData>builder()
                .host("host4").port(8083)
                .nodeData(TestNodeData.builder().shardId(4).build())
                .build();

        ZkNodeDataSerializer<TestNodeData> serializer = node -> {
            try {
                return MAPPER.writeValueAsBytes(node);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };

        // Should not throw, just return early
        sink.updateState(serializer, serviceNode);

        // No success or failure metrics
        assertNull(metricRegistry.getMeters().get(
                METRIC_PREFIX + ".dataStoreType.ZK.dataSource.zk-sink-4.nodeDataSink.update.success"));
        assertNull(metricRegistry.getMeters().get(
                METRIC_PREFIX + ".dataStoreType.ZK.dataSource.zk-sink-4.nodeDataSink.update.failure"));
    }

    // ==================== isActive() via ZkNodeDataStoreConnector base class ====================

    @Test
    void testIsActive_connectedCurator_recordsActiveStatus() {
        val service = new Service(NAMESPACE, SERVICE_NAME);
        val sink = new ZkNodeDataSink<TestNodeData, ZkNodeDataSerializer<TestNodeData>>(
                "zk-sink-5", service, curatorFramework);
        sink.start();

        val active = sink.isActive();

        assertTrue(active);
        val activeMeter = metricRegistry.getMeters().get(
                METRIC_PREFIX + ".dataStoreType.ZK.dataSource.zk-sink-5.active");
        assertNotNull(activeMeter, "Active meter should be recorded");
        assertEquals(1, activeMeter.getCount());

        sink.stop();
    }
}
