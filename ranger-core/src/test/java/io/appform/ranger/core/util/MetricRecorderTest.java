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
package io.appform.ranger.core.util;

import com.codahale.metrics.MetricRegistry;
import io.appform.ranger.core.model.DataStoreType;
import lombok.val;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the new and modified methods introduced in MetricRecorder:
 * <ul>
 *   <li>{@link MetricRecorder#recordStaleDataRetained} – now also records a nodeCount histogram</li>
 *   <li>{@link MetricRecorder#recordServiceRegistryUpdateNodeCount} – new method</li>
 *   <li>{@link MetricRecorder#recordNodesFetchedCount} – new method</li>
 * </ul>
 */
class MetricRecorderTest {

    private static final String SERVICE_NAME = "test-service";
    private static final String UPSTREAM_ID = "test-upstream";
    private static final String PACKAGE_PREFIX = "io.appform.ranger";

    private MetricRegistry metricRegistry;

    @Test
    void metricRegistryHolder_isThreadSafe() throws NoSuchFieldException {
        val registryHolder = MetricRecorder.class.getDeclaredField("metricRegistry");

        assertEquals(AtomicReference.class, registryHolder.getType());
    }

    @BeforeEach
    void setUp() {
        metricRegistry = new MetricRegistry();
        MetricRecorder.initialize(metricRegistry);
    }

    @AfterEach
    void tearDown() {
        // Reset the static registry so other test classes are not affected
        MetricRecorder.initialize(null);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // recordStaleDataRetained (modified: size parameter + histogram)
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    void recordStaleDataRetained_marksGlobalMeter() {
        MetricRecorder.recordStaleDataRetained(SERVICE_NAME, DataStoreType.ZK, UPSTREAM_ID, 3);

        val meterName = PACKAGE_PREFIX + ".dataStoreType.ZK.dataSource." + UPSTREAM_ID + ".staleDataRetained";
        val meter = metricRegistry.getMeters().get(meterName);
        assertNotNull(meter, "Global staleDataRetained meter should be created");
        assertEquals(1, meter.getCount(), "Global meter should be marked once");
    }

    @Test
    void recordStaleDataRetained_marksServiceNameMeter() {
        MetricRecorder.recordStaleDataRetained(SERVICE_NAME, DataStoreType.HTTP, UPSTREAM_ID, 5);

        val meterName = PACKAGE_PREFIX + ".dataStoreType.HTTP.dataSource." + UPSTREAM_ID
                + ".serviceName." + SERVICE_NAME + ".staleDataRetained";
        val meter = metricRegistry.getMeters().get(meterName);
        assertNotNull(meter, "Per-service staleDataRetained meter should be created");
        assertEquals(1, meter.getCount(), "Per-service meter should be marked once");
    }

    @Test
    void recordStaleDataRetained_updatesNodeCountHistogram() {
        MetricRecorder.recordStaleDataRetained(SERVICE_NAME, DataStoreType.DROVE, UPSTREAM_ID, 7);

        val histName = PACKAGE_PREFIX + ".dataStoreType.DROVE.dataSource." + UPSTREAM_ID
                + ".serviceName." + SERVICE_NAME + ".staleDataRetained.nodeCount";
        val histogram = metricRegistry.getHistograms().get(histName);
        assertNotNull(histogram, "staleDataRetained nodeCount histogram should be created");
        assertEquals(1, histogram.getCount(), "Histogram should have one update");
        assertEquals(7, histogram.getSnapshot().getMax(), "Histogram max should equal size passed (7)");
    }

    @Test
    void recordStaleDataRetained_zeroSize_histogramUpdated() {
        MetricRecorder.recordStaleDataRetained(SERVICE_NAME, DataStoreType.ZK, UPSTREAM_ID, 0);

        val histName = PACKAGE_PREFIX + ".dataStoreType.ZK.dataSource." + UPSTREAM_ID
                + ".serviceName." + SERVICE_NAME + ".staleDataRetained.nodeCount";
        val histogram = metricRegistry.getHistograms().get(histName);
        assertNotNull(histogram);
        assertEquals(0, histogram.getSnapshot().getMax(), "Histogram max should be 0 when size is 0");
    }

    @Test
    void recordStaleDataRetained_noRegistry_doesNotThrow() {
        MetricRecorder.initialize(null);
        // Should be a safe no-op
        assertDoesNotThrow(
                () -> MetricRecorder.recordStaleDataRetained(SERVICE_NAME, DataStoreType.ZK, UPSTREAM_ID, 3),
                "recordStaleDataRetained should not throw when registry is not initialized"
        );
    }

    @Test
    void recordStaleDataRetained_multipleCalls_metersAccumulate() {
        MetricRecorder.recordStaleDataRetained(SERVICE_NAME, DataStoreType.ZK, UPSTREAM_ID, 2);
        MetricRecorder.recordStaleDataRetained(SERVICE_NAME, DataStoreType.ZK, UPSTREAM_ID, 4);

        val globalMeterName = PACKAGE_PREFIX + ".dataStoreType.ZK.dataSource." + UPSTREAM_ID + ".staleDataRetained";
        val globalMeter = metricRegistry.getMeters().get(globalMeterName);
        assertEquals(2, globalMeter.getCount(), "Global meter should accumulate 2 marks");

        val histName = PACKAGE_PREFIX + ".dataStoreType.ZK.dataSource." + UPSTREAM_ID
                + ".serviceName." + SERVICE_NAME + ".staleDataRetained.nodeCount";
        val histogram = metricRegistry.getHistograms().get(histName);
        assertEquals(2, histogram.getCount(), "Histogram should have 2 updates");
        assertEquals(4, histogram.getSnapshot().getMax(), "Histogram max should be 4 (latest max)");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // recordServiceRegistryUpdateNodeCount (new method)
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    void recordServiceRegistryUpdateNodeCount_createsHistogramWithCorrectName() {
        MetricRecorder.recordServiceRegistryUpdateNodeCount(SERVICE_NAME, DataStoreType.ZK, UPSTREAM_ID, 10);

        val histName = PACKAGE_PREFIX + ".dataStoreType.ZK.dataSource." + UPSTREAM_ID
                + ".serviceRegistryUpdate.serviceName." + SERVICE_NAME + ".nodeCount";
        val histogram = metricRegistry.getHistograms().get(histName);
        assertNotNull(histogram, "serviceRegistryUpdate nodeCount histogram should be created");
    }

    @Test
    void recordServiceRegistryUpdateNodeCount_updatesHistogramWithSize() {
        MetricRecorder.recordServiceRegistryUpdateNodeCount(SERVICE_NAME, DataStoreType.HTTP, UPSTREAM_ID, 15);

        val histName = PACKAGE_PREFIX + ".dataStoreType.HTTP.dataSource." + UPSTREAM_ID
                + ".serviceRegistryUpdate.serviceName." + SERVICE_NAME + ".nodeCount";
        val histogram = metricRegistry.getHistograms().get(histName);
        assertNotNull(histogram);
        assertEquals(1, histogram.getCount(), "Histogram should have one update");
        assertEquals(15, histogram.getSnapshot().getMax(), "Histogram max should equal size passed (15)");
    }

    @Test
    void recordServiceRegistryUpdateNodeCount_zeroNodes_histogramUpdated() {
        MetricRecorder.recordServiceRegistryUpdateNodeCount(SERVICE_NAME, DataStoreType.ZK, UPSTREAM_ID, 0);

        val histName = PACKAGE_PREFIX + ".dataStoreType.ZK.dataSource." + UPSTREAM_ID
                + ".serviceRegistryUpdate.serviceName." + SERVICE_NAME + ".nodeCount";
        val histogram = metricRegistry.getHistograms().get(histName);
        assertNotNull(histogram);
        assertEquals(0, histogram.getSnapshot().getMax(), "Histogram max should be 0 when no valid nodes");
    }

    @Test
    void recordServiceRegistryUpdateNodeCount_noRegistry_doesNotThrow() {
        MetricRecorder.initialize(null);
        assertDoesNotThrow(
                () -> MetricRecorder.recordServiceRegistryUpdateNodeCount(SERVICE_NAME, DataStoreType.ZK, UPSTREAM_ID, 5),
                "recordServiceRegistryUpdateNodeCount should not throw when registry is not initialized"
        );
    }

    @Test
    void recordServiceRegistryUpdateNodeCount_multipleCalls_histogramAccumulates() {
        MetricRecorder.recordServiceRegistryUpdateNodeCount(SERVICE_NAME, DataStoreType.DROVE, UPSTREAM_ID, 3);
        MetricRecorder.recordServiceRegistryUpdateNodeCount(SERVICE_NAME, DataStoreType.DROVE, UPSTREAM_ID, 8);

        val histName = PACKAGE_PREFIX + ".dataStoreType.DROVE.dataSource." + UPSTREAM_ID
                + ".serviceRegistryUpdate.serviceName." + SERVICE_NAME + ".nodeCount";
        val histogram = metricRegistry.getHistograms().get(histName);
        assertEquals(2, histogram.getCount(), "Histogram should have 2 updates");
        assertEquals(8, histogram.getSnapshot().getMax(), "Histogram max should be 8");
    }

    @Test
    void recordServiceRegistryUpdateNodeCount_differentDataStoreTypes_separateHistograms() {
        MetricRecorder.recordServiceRegistryUpdateNodeCount(SERVICE_NAME, DataStoreType.ZK, UPSTREAM_ID, 5);
        MetricRecorder.recordServiceRegistryUpdateNodeCount(SERVICE_NAME, DataStoreType.HTTP, UPSTREAM_ID, 10);

        val zkHistName = PACKAGE_PREFIX + ".dataStoreType.ZK.dataSource." + UPSTREAM_ID
                + ".serviceRegistryUpdate.serviceName." + SERVICE_NAME + ".nodeCount";
        val httpHistName = PACKAGE_PREFIX + ".dataStoreType.HTTP.dataSource." + UPSTREAM_ID
                + ".serviceRegistryUpdate.serviceName." + SERVICE_NAME + ".nodeCount";

        assertNotNull(metricRegistry.getHistograms().get(zkHistName), "ZK histogram should exist");
        assertNotNull(metricRegistry.getHistograms().get(httpHistName), "HTTP histogram should exist");
        assertEquals(5, metricRegistry.getHistograms().get(zkHistName).getSnapshot().getMax());
        assertEquals(10, metricRegistry.getHistograms().get(httpHistName).getSnapshot().getMax());
    }

    // ─────────────────────────────────────────────────────────────────────────
    // recordNodesFetchedCount (new method)
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    void recordNodesFetchedCount_createsHistogramWithCorrectName() {
        MetricRecorder.recordNodesFetchedCount(SERVICE_NAME, DataStoreType.ZK, UPSTREAM_ID, 6);

        val histName = PACKAGE_PREFIX + ".dataStoreType.ZK.dataSource." + UPSTREAM_ID
                + ".listNodes.serviceName." + SERVICE_NAME + ".nodeCount";
        val histogram = metricRegistry.getHistograms().get(histName);
        assertNotNull(histogram, "listNodes nodeCount histogram should be created");
    }

    @Test
    void recordNodesFetchedCount_updatesHistogramWithSize() {
        MetricRecorder.recordNodesFetchedCount(SERVICE_NAME, DataStoreType.HTTP, UPSTREAM_ID, 20);

        val histName = PACKAGE_PREFIX + ".dataStoreType.HTTP.dataSource." + UPSTREAM_ID
                + ".listNodes.serviceName." + SERVICE_NAME + ".nodeCount";
        val histogram = metricRegistry.getHistograms().get(histName);
        assertNotNull(histogram);
        assertEquals(1, histogram.getCount(), "Histogram should have one update");
        assertEquals(20, histogram.getSnapshot().getMax(), "Histogram max should equal size passed (20)");
    }

    @Test
    void recordNodesFetchedCount_zeroNodes_histogramUpdated() {
        MetricRecorder.recordNodesFetchedCount(SERVICE_NAME, DataStoreType.ZK, UPSTREAM_ID, 0);

        val histName = PACKAGE_PREFIX + ".dataStoreType.ZK.dataSource." + UPSTREAM_ID
                + ".listNodes.serviceName." + SERVICE_NAME + ".nodeCount";
        val histogram = metricRegistry.getHistograms().get(histName);
        assertNotNull(histogram);
        assertEquals(0, histogram.getSnapshot().getMax(), "Histogram max should be 0 when no nodes fetched");
    }

    @Test
    void recordNodesFetchedCount_noRegistry_doesNotThrow() {
        MetricRecorder.initialize(null);
        assertDoesNotThrow(
                () -> MetricRecorder.recordNodesFetchedCount(SERVICE_NAME, DataStoreType.ZK, UPSTREAM_ID, 5),
                "recordNodesFetchedCount should not throw when registry is not initialized"
        );
    }

    @Test
    void recordNodesFetchedCount_multipleCalls_histogramAccumulates() {
        MetricRecorder.recordNodesFetchedCount(SERVICE_NAME, DataStoreType.ZK, UPSTREAM_ID, 4);
        MetricRecorder.recordNodesFetchedCount(SERVICE_NAME, DataStoreType.ZK, UPSTREAM_ID, 9);

        val histName = PACKAGE_PREFIX + ".dataStoreType.ZK.dataSource." + UPSTREAM_ID
                + ".listNodes.serviceName." + SERVICE_NAME + ".nodeCount";
        val histogram = metricRegistry.getHistograms().get(histName);
        assertEquals(2, histogram.getCount(), "Histogram should have 2 updates");
        assertEquals(9, histogram.getSnapshot().getMax(), "Histogram max should be 9");
    }

    @Test
    void recordNodesFetchedCount_differentUpstreamIds_separateHistograms() {
        MetricRecorder.recordNodesFetchedCount(SERVICE_NAME, DataStoreType.ZK, "upstream-a", 3);
        MetricRecorder.recordNodesFetchedCount(SERVICE_NAME, DataStoreType.ZK, "upstream-b", 7);

        val histNameA = PACKAGE_PREFIX + ".dataStoreType.ZK.dataSource.upstream-a"
                + ".listNodes.serviceName." + SERVICE_NAME + ".nodeCount";
        val histNameB = PACKAGE_PREFIX + ".dataStoreType.ZK.dataSource.upstream-b"
                + ".listNodes.serviceName." + SERVICE_NAME + ".nodeCount";

        assertNotNull(metricRegistry.getHistograms().get(histNameA), "Histogram for upstream-a should exist");
        assertNotNull(metricRegistry.getHistograms().get(histNameB), "Histogram for upstream-b should exist");
        assertEquals(3, metricRegistry.getHistograms().get(histNameA).getSnapshot().getMax());
        assertEquals(7, metricRegistry.getHistograms().get(histNameB).getSnapshot().getMax());
    }
}
