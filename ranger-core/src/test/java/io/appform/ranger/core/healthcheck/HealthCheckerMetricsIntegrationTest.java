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
package io.appform.ranger.core.healthcheck;

import com.codahale.metrics.MetricRegistry;
import io.appform.ranger.core.util.MetricRecorder;
import lombok.val;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class HealthCheckerMetricsIntegrationTest {

    private MetricRegistry metricRegistry;

    // Metric keys produced by MetricRecorder (no DataStoreType/upstreamId context):
    //   recordHealthcheckStatus  -> io.appform.ranger.healthChecker.status.<healthy|unhealthy>
    //   recordHealthcheckFailure -> io.appform.ranger.healthChecker.failure
    private static final String HC_PREFIX       = "io.appform.ranger.healthChecker";
    private static final String HEALTHY_KEY     = HC_PREFIX + ".status.healthy";
    private static final String UNHEALTHY_KEY   = HC_PREFIX + ".status.unhealthy";
    private static final String FAILURE_KEY     = HC_PREFIX + ".failure";

    @BeforeEach
    void setUp() {
        metricRegistry = new MetricRegistry();
        MetricRecorder.initialize(metricRegistry);
    }

    @Test
    void testHealthyCheck_recordsHealthyMetric() {
        val healthChecker = new HealthChecker(
                List.of(() -> HealthcheckStatus.healthy), 10000);

        val result = healthChecker.get();

        assertNotNull(result, "First call should always return a result");
        assertEquals(HealthcheckStatus.healthy, result.getStatus());

        // Verify healthy metric recorded
        val healthyMeter = metricRegistry.getMeters().get(HEALTHY_KEY);
        assertNotNull(healthyMeter, "Healthy meter should exist");
        assertEquals(1, healthyMeter.getCount());

        // Verify no failure metric
        assertNull(metricRegistry.getMeters().get(FAILURE_KEY));
        // Verify no unhealthy metric
        assertNull(metricRegistry.getMeters().get(UNHEALTHY_KEY));
    }

    @Test
    void testUnhealthyCheck_recordsUnhealthyMetric() {
        val healthChecker = new HealthChecker(
                List.of(() -> HealthcheckStatus.unhealthy), 10000);

        val result = healthChecker.get();

        assertNotNull(result);
        assertEquals(HealthcheckStatus.unhealthy, result.getStatus());

        // Verify unhealthy metric recorded
        val unhealthyMeter = metricRegistry.getMeters().get(UNHEALTHY_KEY);
        assertNotNull(unhealthyMeter, "Unhealthy meter should exist");
        assertEquals(1, unhealthyMeter.getCount());

        // No healthy metric
        assertNull(metricRegistry.getMeters().get(HEALTHY_KEY));
    }

    @Test
    void testExceptionInHealthcheck_recordsFailureAndUnhealthyMetrics() {
        val healthChecker = new HealthChecker(List.of(() -> {
            throw new RuntimeException("Healthcheck error");
        }), 10000);

        val result = healthChecker.get();

        assertNotNull(result);
        assertEquals(HealthcheckStatus.unhealthy, result.getStatus());

        // Verify failure metric recorded (exception path)
        val failureMeter = metricRegistry.getMeters().get(FAILURE_KEY);
        assertNotNull(failureMeter, "Failure meter should exist on exception");
        assertEquals(1, failureMeter.getCount());

        // Verify unhealthy status metric also recorded
        val unhealthyMeter = metricRegistry.getMeters().get(UNHEALTHY_KEY);
        assertNotNull(unhealthyMeter, "Unhealthy meter should be recorded after exception");
        assertEquals(1, unhealthyMeter.getCount());
    }

    @Test
    void testMultipleHealthchecks_firstUnhealthy_shortCircuits() {
        val healthChecker = new HealthChecker(List.of(
                () -> HealthcheckStatus.unhealthy,
                () -> HealthcheckStatus.healthy // Should not be reached
        ), 10000);

        val result = healthChecker.get();

        assertNotNull(result);
        assertEquals(HealthcheckStatus.unhealthy, result.getStatus());

        val unhealthyMeter = metricRegistry.getMeters().get(UNHEALTHY_KEY);
        assertNotNull(unhealthyMeter);
        assertEquals(1, unhealthyMeter.getCount());
    }

    @Test
    void testMultipleHealthchecks_allHealthy() {
        val healthChecker = new HealthChecker(List.of(
                () -> HealthcheckStatus.healthy,
                () -> HealthcheckStatus.healthy,
                () -> HealthcheckStatus.healthy
        ), 10000);

        val result = healthChecker.get();

        assertNotNull(result);
        assertEquals(HealthcheckStatus.healthy, result.getStatus());

        val healthyMeter = metricRegistry.getMeters().get(HEALTHY_KEY);
        assertNotNull(healthyMeter);
        assertEquals(1, healthyMeter.getCount());
    }

    @Test
    void testRepeatedCalls_metricsAccumulate() {
        val healthChecker = new HealthChecker(
                List.of(() -> HealthcheckStatus.healthy), 0); // staleUpdateThreshold=0 => always returns result

        healthChecker.get();
        healthChecker.get();
        healthChecker.get();

        val healthyMeter = metricRegistry.getMeters().get(HEALTHY_KEY);
        assertNotNull(healthyMeter);
        assertEquals(3, healthyMeter.getCount(), "Healthy metric count should accumulate");
    }

    @Test
    void testHealthStatusTransition_bothMetricsRecorded() {
        // Start healthy, then transition to unhealthy
        val statusHolder = new HealthcheckStatus[]{HealthcheckStatus.healthy};
        val healthChecker = new HealthChecker(List.of(() -> statusHolder[0]), 0);

        // First call: healthy
        val result1 = healthChecker.get();
        assertNotNull(result1);
        assertEquals(HealthcheckStatus.healthy, result1.getStatus());

        // Switch to unhealthy
        statusHolder[0] = HealthcheckStatus.unhealthy;
        val result2 = healthChecker.get();
        assertNotNull(result2);
        assertEquals(HealthcheckStatus.unhealthy, result2.getStatus());

        val healthyMeter = metricRegistry.getMeters().get(HEALTHY_KEY);
        assertNotNull(healthyMeter);
        assertEquals(1, healthyMeter.getCount());

        val unhealthyMeter = metricRegistry.getMeters().get(UNHEALTHY_KEY);
        assertNotNull(unhealthyMeter);
        assertEquals(1, unhealthyMeter.getCount());
    }

    @Test
    void testExceptionInSecondHealthcheck_recordsFailure() {
        val healthChecker = new HealthChecker(List.of(
                () -> HealthcheckStatus.healthy, // First passes
                () -> { throw new RuntimeException("Second fails"); } // Second throws
        ), 10000);

        val result = healthChecker.get();

        assertNotNull(result);
        assertEquals(HealthcheckStatus.unhealthy, result.getStatus());

        // Failure meter for the exception
        val failureMeter = metricRegistry.getMeters().get(FAILURE_KEY);
        assertNotNull(failureMeter);
        assertEquals(1, failureMeter.getCount());

        // Unhealthy status because the second check failed
        val unhealthyMeter = metricRegistry.getMeters().get(UNHEALTHY_KEY);
        assertNotNull(unhealthyMeter);
        assertEquals(1, unhealthyMeter.getCount());
    }
}
