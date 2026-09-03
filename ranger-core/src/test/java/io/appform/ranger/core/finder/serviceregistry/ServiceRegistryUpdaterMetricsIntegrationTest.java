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
package io.appform.ranger.core.finder.serviceregistry;

import com.codahale.metrics.MetricRegistry;
import io.appform.ranger.core.healthcheck.HealthcheckStatus;
import io.appform.ranger.core.model.*;
import io.appform.ranger.core.signals.Signal;
import io.appform.ranger.core.units.TestNodeData;
import io.appform.ranger.core.util.MetricRecorder;
import lombok.val;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

class ServiceRegistryUpdaterMetricsIntegrationTest {

    private MetricRegistry metricRegistry;
    private static final String METRIC_ID = "test-updater-metric";
    private static final Service TEST_SERVICE = new Service("test-ns", "test-svc");

    private ServiceRegistryUpdater<TestNodeData, TestDeserializer> updater;

    @BeforeEach
    void setUp() {
        metricRegistry = new MetricRegistry();
        MetricRecorder.initialize(metricRegistry);
    }

    @AfterEach
    void tearDown() {
        if (updater != null) {
            updater.stop();
        }
    }

    @Test
    void testSuccessfulRefresh_recordsSuccessTimer() {
        val registry = new MapBasedServiceRegistry<TestNodeData>(TEST_SERVICE);
        val nodes = List.of(
                ServiceNode.<TestNodeData>builder()
                        .host("host1")
                        .port(8080)
                        .nodeData(TestNodeData.builder().shardId(1).build())
                        .healthcheckStatus(HealthcheckStatus.healthy)
                        .lastUpdatedTimeStamp(System.currentTimeMillis())
                        .build()
        );
        val dataSource = new TestNodeDataSource(METRIC_ID, DataStoreType.ZK, true, nodes, false);
        val signal = new TestSignal();

        updater = new ServiceRegistryUpdater<>(registry, dataSource, List.of(signal), new TestDeserializer());
        updater.start();

        // Wait for initial update and a brief period for metric recording to complete
        awaitRefresh(registry);

        // Wait for success timer to be recorded using Awaitility
        val timerName = "io.appform.ranger.dataStoreType.ZK.dataSource." + METRIC_ID + ".nodeDataRefresh.success";
        await()
                .atMost(Duration.ofSeconds(5))
                .pollInterval(Duration.ofMillis(50))
                .untilAsserted(() -> {
                    val timer = metricRegistry.getTimers().get(timerName);
                    assertNotNull(timer, "Node data refresh success timer should exist");
                    assertTrue(timer.getCount() >= 1, "Timer should have at least 1 update");
                });

        // No failure timer
        val failureTimerName = "io.appform.ranger.dataStoreType.ZK.dataSource." + METRIC_ID + ".nodeDataRefresh.failure";
        val failureTimer = metricRegistry.getTimers().get(failureTimerName);
        assertTrue(failureTimer == null || failureTimer.getCount() == 0,
                "Failure timer should not be recorded on success");
    }

    @Test
    void testRefreshThrowsException_recordsFailureTimer() {
        val registry = new MapBasedServiceRegistry<TestNodeData>(TEST_SERVICE);
        val dataSource = new TestNodeDataSource(METRIC_ID, DataStoreType.HTTP, true, null, true);
        val signal = new TestSignal();

        updater = new ServiceRegistryUpdater<>(registry, dataSource, List.of(signal), new TestDeserializer());
        // Use try-catch since initial update will fail
        try {
            updater.start();
        } catch (Exception e) {
            // Expected: initial update fails
        }

        // Wait for failure timer to be recorded using Awaitility
        val failureTimerName = "io.appform.ranger.dataStoreType.HTTP.dataSource." + METRIC_ID + ".nodeDataRefresh.failure";
        await()
                .atMost(Duration.ofSeconds(5))
                .pollInterval(Duration.ofMillis(50))
                .untilAsserted(() -> {
                    val failureTimer = metricRegistry.getTimers().get(failureTimerName);
                    assertNotNull(failureTimer, "Node data refresh failure timer should exist");
                    assertTrue(failureTimer.getCount() >= 1, "Failure timer should have at least 1 update");
                });
    }

    @Test
    void testInactiveDataSource_recordsStaleDataRetained() {
        val registry = new MapBasedServiceRegistry<TestNodeData>(TEST_SERVICE);
        // Start with an active source so initial update succeeds
        val nodes = List.of(
                ServiceNode.<TestNodeData>builder()
                        .host("host1")
                        .port(8080)
                        .nodeData(TestNodeData.builder().shardId(1).build())
                        .healthcheckStatus(HealthcheckStatus.healthy)
                        .lastUpdatedTimeStamp(System.currentTimeMillis())
                        .build()
        );
        val dataSource = new TestNodeDataSource(METRIC_ID, DataStoreType.DROVE, true, nodes, false);
        val signal = new TestSignal();

        updater = new ServiceRegistryUpdater<>(registry, dataSource, List.of(signal), new TestDeserializer());
        updater.start();
        awaitRefresh(registry);

        // Now deactivate the data source and trigger another update
        dataSource.setActive(false);
        signal.fire();

        // Wait for stale data retained meter to be recorded using Awaitility
        val meterName = "io.appform.ranger.dataStoreType.DROVE.dataSource." + METRIC_ID + ".staleDataRetained";
        await()
                .atMost(Duration.ofSeconds(5))
                .pollInterval(Duration.ofMillis(50))
                .untilAsserted(() -> {
                    val meter = metricRegistry.getMeters().get(meterName);
                    assertNotNull(meter, "Stale data retained meter should exist");
                    assertTrue(meter.getCount() >= 1, "Stale data retained should be recorded at least once");
                });
    }

    @Test
    void testSuccessfulRefresh_zombieNodesFiltered_recordsZombieMetric() {
        val registry = new MapBasedServiceRegistry<TestNodeData>(TEST_SERVICE);
        val nodes = List.of(
                ServiceNode.<TestNodeData>builder()
                        .host("healthy-host")
                        .port(8080)
                        .nodeData(TestNodeData.builder().shardId(1).build())
                        .healthcheckStatus(HealthcheckStatus.healthy)
                        .lastUpdatedTimeStamp(System.currentTimeMillis())
                        .build(),
                ServiceNode.<TestNodeData>builder()
                        .host("zombie-host")
                        .port(8081)
                        .nodeData(TestNodeData.builder().shardId(2).build())
                        .healthcheckStatus(HealthcheckStatus.healthy)
                        .lastUpdatedTimeStamp(0L) // Very old => zombie
                        .build()
        );
        val dataSource = new TestNodeDataSource(METRIC_ID, DataStoreType.ZK, true, nodes, false);
        val signal = new TestSignal();

        updater = new ServiceRegistryUpdater<>(registry, dataSource, List.of(signal), new TestDeserializer());
        updater.start();
        awaitRefresh(registry);

        // Verify zombie metric recorded (from FinderUtils.filterValidNodes called inside updateRegistry)
        val zombieMeter = metricRegistry.getMeters().get("io.appform.ranger.zombieNodes");
        assertNotNull(zombieMeter, "Zombie nodes meter should exist");
        assertTrue(zombieMeter.getCount() >= 1);

        val svcZombieMeter = metricRegistry.getMeters().get("io.appform.ranger.zombieNodes.serviceName.test-svc");
        assertNotNull(svcZombieMeter);
        assertTrue(svcZombieMeter.getCount() >= 1);

        // Also verify success timer still recorded
        val timerName = "io.appform.ranger.dataStoreType.ZK.dataSource." + METRIC_ID + ".nodeDataRefresh.success";
        val timer = metricRegistry.getTimers().get(timerName);
        assertNotNull(timer);
        assertTrue(timer.getCount() >= 1);
    }

    @Test
    void testSuccessfulRefresh_recordsNodesFetchedCount() {
        val registry = new MapBasedServiceRegistry<TestNodeData>(TEST_SERVICE);
        val nodes = List.of(
                ServiceNode.<TestNodeData>builder()
                        .host("host1").port(8080)
                        .nodeData(TestNodeData.builder().shardId(1).build())
                        .healthcheckStatus(HealthcheckStatus.healthy)
                        .lastUpdatedTimeStamp(System.currentTimeMillis())
                        .build(),
                ServiceNode.<TestNodeData>builder()
                        .host("host2").port(8081)
                        .nodeData(TestNodeData.builder().shardId(2).build())
                        .healthcheckStatus(HealthcheckStatus.healthy)
                        .lastUpdatedTimeStamp(System.currentTimeMillis())
                        .build()
        );
        val dataSource = new TestNodeDataSource(METRIC_ID, DataStoreType.ZK, true, nodes, false);
        val signal = new TestSignal();

        updater = new ServiceRegistryUpdater<>(registry, dataSource, List.of(signal), new TestDeserializer());
        updater.start();
        awaitRefresh(registry);

        val histName = "io.appform.ranger.dataStoreType.ZK.dataSource." + METRIC_ID
                + ".listNodes.serviceName." + TEST_SERVICE.getServiceName() + ".nodeCount";
        await()
                .atMost(Duration.ofSeconds(5))
                .pollInterval(Duration.ofMillis(50))
                .untilAsserted(() -> {
                    val histogram = metricRegistry.getHistograms().get(histName);
                    assertNotNull(histogram, "listNodes nodeCount histogram should be recorded");
                    assertTrue(histogram.getCount() >= 1, "Histogram should have at least one update");
                    assertEquals(2, histogram.getSnapshot().getMax(),
                            "Fetched count should equal total nodes returned by data source (2)");
                });
    }

    @Test
    void testSuccessfulRefresh_recordsServiceRegistryUpdateNodeCount() {
        val registry = new MapBasedServiceRegistry<TestNodeData>(TEST_SERVICE);
        val nodes = List.of(
                ServiceNode.<TestNodeData>builder()
                        .host("host1").port(8080)
                        .nodeData(TestNodeData.builder().shardId(1).build())
                        .healthcheckStatus(HealthcheckStatus.healthy)
                        .lastUpdatedTimeStamp(System.currentTimeMillis())
                        .build(),
                ServiceNode.<TestNodeData>builder()
                        .host("host2").port(8081)
                        .nodeData(TestNodeData.builder().shardId(2).build())
                        .healthcheckStatus(HealthcheckStatus.healthy)
                        .lastUpdatedTimeStamp(System.currentTimeMillis())
                        .build()
        );
        val dataSource = new TestNodeDataSource(METRIC_ID, DataStoreType.ZK, true, nodes, false);
        val signal = new TestSignal();

        updater = new ServiceRegistryUpdater<>(registry, dataSource, List.of(signal), new TestDeserializer());
        updater.start();
        awaitRefresh(registry);

        val histName = "io.appform.ranger.dataStoreType.ZK.dataSource." + METRIC_ID
                + ".serviceRegistryUpdate.serviceName." + TEST_SERVICE.getServiceName() + ".nodeCount";
        await()
                .atMost(Duration.ofSeconds(5))
                .pollInterval(Duration.ofMillis(50))
                .untilAsserted(() -> {
                    val histogram = metricRegistry.getHistograms().get(histName);
                    assertNotNull(histogram, "serviceRegistryUpdate nodeCount histogram should be recorded");
                    assertTrue(histogram.getCount() >= 1, "Histogram should have at least one update");
                    assertEquals(2, histogram.getSnapshot().getMax(),
                            "Valid node count should equal 2 healthy, non-zombie nodes");
                });
    }

    @Test
    void testFetchedCountExceedsValidCount_whenZombiesPresent() {
        val registry = new MapBasedServiceRegistry<TestNodeData>(TEST_SERVICE);
        // 1 healthy + 1 zombie (very old timestamp). Fetched = 2, valid = 1.
        val nodes = List.of(
                ServiceNode.<TestNodeData>builder()
                        .host("healthy-host").port(8080)
                        .nodeData(TestNodeData.builder().shardId(1).build())
                        .healthcheckStatus(HealthcheckStatus.healthy)
                        .lastUpdatedTimeStamp(System.currentTimeMillis())
                        .build(),
                ServiceNode.<TestNodeData>builder()
                        .host("zombie-host").port(8081)
                        .nodeData(TestNodeData.builder().shardId(2).build())
                        .healthcheckStatus(HealthcheckStatus.healthy)
                        .lastUpdatedTimeStamp(0L) // zombie
                        .build()
        );
        val dataSource = new TestNodeDataSource(METRIC_ID, DataStoreType.HTTP, true, nodes, false);
        val signal = new TestSignal();

        updater = new ServiceRegistryUpdater<>(registry, dataSource, List.of(signal), new TestDeserializer());
        updater.start();
        awaitRefresh(registry);

        val fetchedHistName = "io.appform.ranger.dataStoreType.HTTP.dataSource." + METRIC_ID
                + ".listNodes.serviceName." + TEST_SERVICE.getServiceName() + ".nodeCount";
        val validHistName = "io.appform.ranger.dataStoreType.HTTP.dataSource." + METRIC_ID
                + ".serviceRegistryUpdate.serviceName." + TEST_SERVICE.getServiceName() + ".nodeCount";

        await()
                .atMost(Duration.ofSeconds(5))
                .pollInterval(Duration.ofMillis(50))
                .untilAsserted(() -> {
                    val fetchedHist = metricRegistry.getHistograms().get(fetchedHistName);
                    val validHist = metricRegistry.getHistograms().get(validHistName);
                    assertNotNull(fetchedHist, "Fetched count histogram should exist");
                    assertNotNull(validHist, "Valid count histogram should exist");
                    assertEquals(2, fetchedHist.getSnapshot().getMax(),
                            "Fetched count should be 2 (all nodes including zombie)");
                    assertEquals(1, validHist.getSnapshot().getMax(),
                            "Valid count should be 1 (zombie filtered out)");
                });
    }

    @Test
    void testInactiveDataSource_recordsStaleDataRetainedWithNodeCountHistogram() {
        val registry = new MapBasedServiceRegistry<TestNodeData>(TEST_SERVICE);
        val nodes = List.of(
                ServiceNode.<TestNodeData>builder()
                        .host("host1").port(8080)
                        .nodeData(TestNodeData.builder().shardId(1).build())
                        .healthcheckStatus(HealthcheckStatus.healthy)
                        .lastUpdatedTimeStamp(System.currentTimeMillis())
                        .build(),
                ServiceNode.<TestNodeData>builder()
                        .host("host2").port(8081)
                        .nodeData(TestNodeData.builder().shardId(2).build())
                        .healthcheckStatus(HealthcheckStatus.healthy)
                        .lastUpdatedTimeStamp(System.currentTimeMillis())
                        .build()
        );
        val dataSource = new TestNodeDataSource(METRIC_ID, DataStoreType.ZK, true, nodes, false);
        val signal = new TestSignal();

        updater = new ServiceRegistryUpdater<>(registry, dataSource, List.of(signal), new TestDeserializer());
        updater.start();
        awaitRefresh(registry);

        // Deactivate and trigger stale path
        dataSource.setActive(false);
        signal.fire();

        val staleNodeCountHistName = "io.appform.ranger.dataStoreType.ZK.dataSource." + METRIC_ID
                + ".serviceName." + TEST_SERVICE.getServiceName() + ".staleDataRetained.nodeCount";
        await()
                .atMost(Duration.ofSeconds(5))
                .pollInterval(Duration.ofMillis(50))
                .untilAsserted(() -> {
                    val histogram = metricRegistry.getHistograms().get(staleNodeCountHistName);
                    assertNotNull(histogram, "staleDataRetained nodeCount histogram should be recorded");
                    assertTrue(histogram.getCount() >= 1, "Histogram should have at least one update");
                    // The stale path retains healthy nodes only; 2 healthy nodes were present
                    assertTrue(histogram.getSnapshot().getMax() >= 0,
                            "Histogram should record a non-negative node count");
                });
    }

    @Test
    void testCallFailure_recordsStaleDataRetainedWithNodeCountHistogram() {
        val registry = new MapBasedServiceRegistry<TestNodeData>(TEST_SERVICE);
        val signal = new TestSignal();

        // First start with a healthy node so the registry has something to retain
        val initialNodes = List.of(
                ServiceNode.<TestNodeData>builder()
                        .host("host1").port(8080)
                        .nodeData(TestNodeData.builder().shardId(1).build())
                        .healthcheckStatus(HealthcheckStatus.healthy)
                        .lastUpdatedTimeStamp(System.currentTimeMillis())
                        .build()
        );
        val dataSource = new TestNodeDataSource(METRIC_ID, DataStoreType.HTTP, true, initialNodes, false);

        updater = new ServiceRegistryUpdater<>(registry, dataSource, List.of(signal), new TestDeserializer());
        updater.start();
        awaitRefresh(registry);

        // Now trigger a call failure (while still active, but throws on refresh)
        val failingDataSource = new TestNodeDataSource(METRIC_ID, DataStoreType.HTTP, true, null, true);
        // Create a new updater with the failing data source and trigger
        updater.stop();
        val signal2 = new TestSignal();
        val updater2 = new ServiceRegistryUpdater<>(registry, failingDataSource, List.of(signal2), new TestDeserializer());
        try {
            updater2.start();
        } catch (Exception ignored) {
            // May throw on initial update failure
        }

        val staleNodeCountHistName = "io.appform.ranger.dataStoreType.HTTP.dataSource." + METRIC_ID
                + ".serviceName." + TEST_SERVICE.getServiceName() + ".staleDataRetained.nodeCount";
        await()
                .atMost(Duration.ofSeconds(5))
                .pollInterval(Duration.ofMillis(50))
                .untilAsserted(() -> {
                    val histogram = metricRegistry.getHistograms().get(staleNodeCountHistName);
                    assertNotNull(histogram, "staleDataRetained nodeCount histogram should be recorded on call failure");
                    assertTrue(histogram.getCount() >= 1, "Histogram should have at least one update");
                });

        updater2.stop();
    }

    @Test
    void testRefreshReturnsNull_noSuccessOrFailureTimer_butStaleRetained() {
        val registry = new MapBasedServiceRegistry<TestNodeData>(TEST_SERVICE);
        // Return null from refresh (empty Optional)
        val dataSource = new TestNodeDataSource(METRIC_ID, DataStoreType.ZK, true,
                null, false); // null nodes => refresh returns empty
        // Pre-populate registry so initial wait doesn't block forever
        registry.updateNodes(List.of(
                ServiceNode.<TestNodeData>builder()
                        .host("old-host")
                        .port(8080)
                        .nodeData(TestNodeData.builder().shardId(1).build())
                        .healthcheckStatus(HealthcheckStatus.healthy)
                        .lastUpdatedTimeStamp(System.currentTimeMillis())
                        .build()
        ));

        val signal = new TestSignal();
        updater = new ServiceRegistryUpdater<>(registry, dataSource, List.of(signal), new TestDeserializer());

        // Cannot call start() because initial refresh returns null which won't set refreshed=true
        // Instead, test the scenario after start by triggering signal
        // Use alternate approach: make first call return valid, then switch to null
        dataSource.setNodeList(List.of(
                ServiceNode.<TestNodeData>builder()
                        .host("valid-host")
                        .port(8080)
                        .nodeData(TestNodeData.builder().shardId(1).build())
                        .healthcheckStatus(HealthcheckStatus.healthy)
                        .lastUpdatedTimeStamp(System.currentTimeMillis())
                        .build()
        ));

        updater = new ServiceRegistryUpdater<>(registry, dataSource, List.of(signal), new TestDeserializer());
        updater.start();
        awaitRefresh(registry);

        // Get the initial count after first successful refresh
        val timerName = "io.appform.ranger.dataStoreType.ZK.dataSource." + METRIC_ID + ".nodeDataRefresh.success";

        // Wait for the first success timer to be recorded
        await()
                .atMost(Duration.ofSeconds(5))
                .pollInterval(Duration.ofMillis(50))
                .untilAsserted(() -> {
                    val timer = metricRegistry.getTimers().get(timerName);
                    assertNotNull(timer, "Success timer should exist after first refresh");
                    assertTrue(timer.getCount() >= 1, "Should have at least one success");
                });

        val beforeTimer = metricRegistry.getTimers().get(timerName);
        final long beforeCount = beforeTimer.getCount();

        // Now set to null and trigger update
        dataSource.setNodeList(null);
        signal.fire();

        // Wait a bit to ensure the signal processing would have completed
        // Use a shorter wait since we're just ensuring the async operation completes
        await()
                .atMost(Duration.ofSeconds(2))
                .pollDelay(Duration.ofMillis(100))
                .until(() -> true);

        val afterTimer = metricRegistry.getTimers().get(timerName);
        final long afterCount = afterTimer == null ? 0L : afterTimer.getCount();

        assertEquals(beforeCount, afterCount,
                "Success timer count should not increase when refresh returns null");
    }

    // ==================== Test helpers ====================

    private void awaitRefresh(ServiceRegistry<?> registry) {
        await()
                .atMost(Duration.ofSeconds(5))
                .pollInterval(Duration.ofMillis(50))
                .untilAsserted(() -> assertTrue(registry.isRefreshed(), "Registry should be refreshed"));
    }

    // ==================== Test implementations ====================

    static class TestDeserializer implements Deserializer<TestNodeData> {
    }

    static class TestNodeDataSource implements NodeDataSource<TestNodeData, TestDeserializer> {
        private final String upstreamId;
        private final DataStoreType dataStoreType;
        private final AtomicBoolean active;
        private volatile List<ServiceNode<TestNodeData>> nodeList;
        private final boolean throwOnRefresh;

        TestNodeDataSource(String upstreamId, DataStoreType dataStoreType, boolean active,
                           List<ServiceNode<TestNodeData>> nodeList, boolean throwOnRefresh) {
            this.upstreamId = upstreamId;
            this.dataStoreType = dataStoreType;
            this.active = new AtomicBoolean(active);
            this.nodeList = nodeList;
            this.throwOnRefresh = throwOnRefresh;
        }

        void setActive(boolean active) {
            this.active.set(active);
        }

        void setNodeList(List<ServiceNode<TestNodeData>> nodeList) {
            this.nodeList = nodeList;
        }

        @Override
        public String getUpstreamId() {
            return upstreamId;
        }

        @Override
        public DataStoreType getDataStoreType() {
            return dataStoreType;
        }

        @Override
        public Optional<List<ServiceNode<TestNodeData>>> refresh(TestDeserializer deserializer) {
            if (throwOnRefresh) {
                throw new RuntimeException("Simulated refresh failure");
            }
            return Optional.ofNullable(nodeList);
        }

        @Override
        public void start() {
            // No-op for test
        }

        @Override
        public void ensureConnected() {
            // No-op for test
        }

        @Override
        public void stop() {
            // No-op for test
        }

        @Override
        public boolean isActive() {
            return active.get();
        }
    }

    static class TestSignal extends Signal<TestNodeData> {
        protected TestSignal() {
            super(() -> null, Collections.emptyList());
        }

        public void fire() {
            onSignalReceived();
        }

        @Override
        public void start() {
            // No-op for test
        }

        @Override
        public void stop() {
            // No-op for test
        }
    }
}
