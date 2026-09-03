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
import io.appform.ranger.core.healthcheck.HealthcheckStatus;
import io.appform.ranger.core.model.Service;
import io.appform.ranger.core.model.ServiceNode;
import lombok.val;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class FinderUtilsMetricsIntegrationTest {

    private MetricRegistry metricRegistry;

    @BeforeEach
    void setUp() {
        metricRegistry = new MetricRegistry();
        MetricRecorder.initialize(metricRegistry);
    }

    @Test
    void testZombieNodeDetection_recordsMetric() {
        val service = new Service("test-ns", "test-svc");
        val zombieNode = ServiceNode.<Integer>builder()
                .host("localhost")
                .port(8080)
                .nodeData(1)
                .healthcheckStatus(HealthcheckStatus.healthy)
                .lastUpdatedTimeStamp(0L) // Very old timestamp => zombie
                .build();

        val thresholdTime = System.currentTimeMillis() - 60000; // 1 minute ago

        val result = FinderUtils.isValidNode(service, thresholdTime, zombieNode);

        assertFalse(result, "Zombie node should be invalid");

        // Verify global zombie nodes meter
        val globalMeter = metricRegistry.getMeters().get("io.appform.ranger.zombieNodes");
        assertNotNull(globalMeter, "Global zombie nodes meter should exist");
        assertEquals(1, globalMeter.getCount());

        // Verify per-service zombie nodes meter
        val svcMeter = metricRegistry.getMeters().get("io.appform.ranger.zombieNodes.serviceName.test-svc");
        assertNotNull(svcMeter, "Per-service zombie nodes meter should exist");
        assertEquals(1, svcMeter.getCount());
    }

    @Test
    void testZombieNodeDetection_multipleZombies() {
        val service = new Service("ns", "my-service");
        val thresholdTime = System.currentTimeMillis() - 60000;

        // Create 3 zombie nodes
        for (int i = 0; i < 3; i++) {
            val zombieNode = ServiceNode.<Integer>builder()
                    .host("host-" + i)
                    .port(8080 + i)
                    .nodeData(i)
                    .healthcheckStatus(HealthcheckStatus.healthy)
                    .lastUpdatedTimeStamp(0L)
                    .build();
            FinderUtils.isValidNode(service, thresholdTime, zombieNode);
        }

        val globalMeter = metricRegistry.getMeters().get("io.appform.ranger.zombieNodes");
        assertEquals(3, globalMeter.getCount());

        val svcMeter = metricRegistry.getMeters().get("io.appform.ranger.zombieNodes.serviceName.my-service");
        assertEquals(3, svcMeter.getCount());
    }

    @Test
    void testHealthyNode_noMetricRecorded() {
        val service = new Service("ns", "svc");
        val healthyNode = ServiceNode.<Integer>builder()
                .host("localhost")
                .port(8080)
                .nodeData(1)
                .healthcheckStatus(HealthcheckStatus.healthy)
                .lastUpdatedTimeStamp(System.currentTimeMillis()) // Fresh timestamp
                .build();

        val thresholdTime = System.currentTimeMillis() - 60000;
        val result = FinderUtils.isValidNode(service, thresholdTime, healthyNode);

        assertTrue(result, "Healthy node should be valid");
        assertTrue(metricRegistry.getMeters().isEmpty(), "No meters should be recorded for healthy nodes");
    }

    @Test
    void testUnhealthyNode_noZombieMetricRecorded() {
        val service = new Service("ns", "svc");
        val unhealthyNode = ServiceNode.<Integer>builder()
                .host("localhost")
                .port(8080)
                .nodeData(1)
                .healthcheckStatus(HealthcheckStatus.unhealthy)
                .lastUpdatedTimeStamp(System.currentTimeMillis())
                .build();

        val thresholdTime = System.currentTimeMillis() - 60000;
        val result = FinderUtils.isValidNode(service, thresholdTime, unhealthyNode);

        assertFalse(result, "Unhealthy node should be invalid");
        // Zombie metric should NOT be recorded for unhealthy nodes (only for stale healthy nodes)
        assertTrue(metricRegistry.getMeters().isEmpty(),
                "No zombie meters should be recorded for unhealthy nodes");
    }

    @Test
    void testFilterValidNodes_zombiesFiltered_metricsRecorded() {
        val service = new Service("ns", "filter-svc");
        val thresholdTime = System.currentTimeMillis() - 60000;

        val nodes = List.of(
                ServiceNode.<Integer>builder()
                        .host("good-host")
                        .port(8080)
                        .nodeData(1)
                        .healthcheckStatus(HealthcheckStatus.healthy)
                        .lastUpdatedTimeStamp(System.currentTimeMillis())
                        .build(),
                ServiceNode.<Integer>builder()
                        .host("zombie-host")
                        .port(8081)
                        .nodeData(2)
                        .healthcheckStatus(HealthcheckStatus.healthy)
                        .lastUpdatedTimeStamp(0L) // zombie
                        .build(),
                ServiceNode.<Integer>builder()
                        .host("zombie-host-2")
                        .port(8082)
                        .nodeData(3)
                        .healthcheckStatus(HealthcheckStatus.healthy)
                        .lastUpdatedTimeStamp(0L) // zombie
                        .build()
        );

        val filtered = FinderUtils.filterValidNodes(service, nodes, thresholdTime);

        assertEquals(1, filtered.size(), "Only 1 valid node should remain");
        assertEquals("good-host", filtered.get(0).getHost());

        val globalMeter = metricRegistry.getMeters().get("io.appform.ranger.zombieNodes");
        assertEquals(2, globalMeter.getCount(), "2 zombie detections should be recorded");

        val svcMeter = metricRegistry.getMeters().get("io.appform.ranger.zombieNodes.serviceName.filter-svc");
        assertEquals(2, svcMeter.getCount());
    }

    @Test
    void testNullNode_noMetricRecorded() {
        val service = new Service("ns", "svc");
        val result = FinderUtils.isValidNode(service, System.currentTimeMillis() - 60000, null);

        assertFalse(result, "Null node should be invalid");
        assertTrue(metricRegistry.getMeters().isEmpty(), "No meters should be recorded for null nodes");
    }
}
