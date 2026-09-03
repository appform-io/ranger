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

import com.codahale.metrics.MetricRegistry;
import io.appform.ranger.core.util.MetricRecorder;
import lombok.val;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingCluster;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for ZkServiceDataSource metrics recording.
 * Tests verify that metrics are pushed through actual ZK operations.
 */
class ZkServiceDataSourceMetricsIntegrationTest {

    private static final String METRIC_PREFIX = "io.appform.ranger";
    private static final String NAMESPACE = "test";

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

    // ==================== services() - Success with children ====================

    @Test
    void testServices_withChildren_recordsSuccessStatus() throws Exception {
        // Create some child nodes under "/" (the registered services path)
        curatorFramework.create().creatingParentContainersIfNeeded().forPath("/service-a");
        curatorFramework.create().creatingParentContainersIfNeeded().forPath("/service-b");

        val dataSource = new ZkServiceDataSource(
                "zk-svc-src-1", NAMESPACE, testingCluster.getConnectString(), curatorFramework);
        dataSource.start();

        val services = dataSource.services();

        assertNotNull(services);
        assertEquals(2, services.size());

        // Verify success fetch status
        val successMeter = metricRegistry.getMeters().get(
                METRIC_PREFIX + ".dataStoreType.ZK.dataSource.zk-svc-src-1.services.fetch.success");
        assertNotNull(successMeter, "Services fetch success meter should be recorded");
        assertEquals(1, successMeter.getCount());

        // No empty response metric
        val emptyMeter = metricRegistry.getMeters().get(
                METRIC_PREFIX + ".dataStoreType.ZK.dataSource.zk-svc-src-1.httpCall.services.nullOrEmptyResponse");
        assertNull(emptyMeter, "No null/empty services response meter should be recorded when children exist");

        dataSource.stop();
    }

    // ==================== services() - Empty children ====================

    @Test
    void testServices_noChildren_recordsNullOrEmptyServicesAndSuccess() throws Exception {
        // Use a fresh curator with a different namespace that has no children
        val emptyCurator = CuratorFrameworkFactory.builder()
                .namespace("empty-ns")
                .connectString(testingCluster.getConnectString())
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .build();
        emptyCurator.start();
        emptyCurator.blockUntilConnected();

        // Ensure root path exists but is empty
        if (emptyCurator.checkExists().forPath("/") == null) {
            emptyCurator.create().forPath("/");
        }

        val dataSource = new ZkServiceDataSource(
                "zk-svc-src-2", "empty-ns", testingCluster.getConnectString(), emptyCurator);
        dataSource.start();

        val services = dataSource.services();

        assertNotNull(services);
        assertTrue(services.isEmpty());

        // Verify null/empty services response metric
        val emptyMeter = metricRegistry.getMeters().get(
                METRIC_PREFIX + ".dataStoreType.ZK.dataSource.zk-svc-src-2.httpCall.services.nullOrEmptyResponse");
        assertNotNull(emptyMeter, "Null/empty services response meter should be recorded");
        assertEquals(1, emptyMeter.getCount());

        // Verify success fetch status (still marked success even if empty)
        val successMeter = metricRegistry.getMeters().get(
                METRIC_PREFIX + ".dataStoreType.ZK.dataSource.zk-svc-src-2.services.fetch.success");
        assertNotNull(successMeter, "Services fetch success meter should still be recorded");
        assertEquals(1, successMeter.getCount());

        emptyCurator.close();
    }

    // ==================== services() - ZK failure ====================

    @Test
    void testServices_zkFailure_recordsUnknownFailureAndFailureStatus() throws Exception {
        val dataSource = new ZkServiceDataSource(
                "zk-svc-src-3", NAMESPACE, testingCluster.getConnectString(), curatorFramework);
        dataSource.start();

        // Close the curator to simulate connection failure
        curatorFramework.close();

        assertThrows(Exception.class, dataSource::services);

        // Verify failure fetch status
        val failureMeter = metricRegistry.getMeters().get(
                METRIC_PREFIX + ".dataStoreType.ZK.dataSource.zk-svc-src-3.services.fetch.failure");
        assertNotNull(failureMeter, "Services fetch failure meter should be recorded");
        assertEquals(1, failureMeter.getCount());

        // Verify ZK read unknown failure
        val unknownFailureMeter = metricRegistry.getMeters().get(
                METRIC_PREFIX + ".dataStoreType.ZK.dataSource.zk-svc-src-3.zkRead.services.unknownFailure");
        assertNotNull(unknownFailureMeter, "ZK read unknown failure meter should be recorded");
        assertEquals(1, unknownFailureMeter.getCount());

        // Null out curatorFramework to prevent double-close in tearDown
        curatorFramework = null;
    }

    // ==================== services() - Success with provided curator ====================

    @Test
    void testServices_providedCurator_recordsSuccess() throws Exception {
        // Create a service node
        curatorFramework.create().creatingParentContainersIfNeeded().forPath("/my-service");

        val dataSource = new ZkServiceDataSource(
                "zk-svc-src-4", NAMESPACE, testingCluster.getConnectString(), curatorFramework);
        dataSource.start();

        val services = dataSource.services();

        assertNotNull(services);
        assertTrue(services.stream().anyMatch(s -> "my-service".equals(s.getServiceName())));

        // Verify success
        val successMeter = metricRegistry.getMeters().get(
                METRIC_PREFIX + ".dataStoreType.ZK.dataSource.zk-svc-src-4.services.fetch.success");
        assertNotNull(successMeter, "Success meter should be recorded");
        assertEquals(1, successMeter.getCount());

        dataSource.stop();
    }
}
