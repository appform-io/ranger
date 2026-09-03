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
package io.appform.ranger.drove.servicefinderhub;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import com.phonepe.drove.models.api.ApiResponse;
import com.phonepe.drove.models.api.AppSummary;
import com.phonepe.drove.models.application.ApplicationState;
import io.appform.ranger.core.util.MetricRecorder;
import io.appform.ranger.drove.common.DroveCommunicationException;
import io.appform.ranger.drove.common.DroveCommunicator;
import io.appform.ranger.drove.config.DroveUpstreamConfig;
import io.appform.ranger.drove.utils.RangerDroveUtils;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Date;
import java.util.List;
import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for DroveServiceDataSource metrics recording.
 * Tests verify that metrics are pushed through actual Drove code flows.
 */
@WireMockTest
class DroveServiceDataSourceMetricsIntegrationTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String METRIC_PREFIX = "io.appform.ranger";
    private MetricRegistry metricRegistry;

    @BeforeEach
    void setUp() {
        metricRegistry = new MetricRegistry();
        MetricRecorder.initialize(metricRegistry);
    }

    // ==================== services() - Success ====================

    @Test
    @SneakyThrows
    void testServices_success_recordsFetchSuccess(WireMockRuntimeInfo wm) {
        val response = ApiResponse.success(Map.of(
                "SVC_A", new AppSummary("SVC_A", "SVC_A", 4, 4, 4, 1024, Map.of(),
                        ApplicationState.RUNNING, new Date(), new Date()),
                "SVC_B", new AppSummary("SVC_B", "SVC_B", 4, 4, 4, 1024, Map.of(),
                        ApplicationState.RUNNING, new Date(), new Date())));

        stubFor(get("/apis/v1/applications")
                .withBasicAuth("guest", "guest")
                .willReturn(okJson(MAPPER.writeValueAsString(response))));

        val config = buildConfig(wm, "drove-svc-src-1");
        try (val droveClient = buildClient(config)) {
            val dataSource = new DroveServiceDataSource<>(
                    config, MAPPER, "testns", droveClient);

            val services = dataSource.services();

            assertNotNull(services);
            assertEquals(2, services.size());

            // Verify services fetch success
            val successMeter = metricRegistry.getMeters().get(
                    METRIC_PREFIX + ".dataStoreType.DROVE.dataSource.drove-svc-src-1.services.fetch.success");
            assertNotNull(successMeter, "Services fetch success meter should be recorded");
            assertEquals(1, successMeter.getCount());

            // No failure
            assertNull(metricRegistry.getMeters().get(
                    METRIC_PREFIX + ".dataStoreType.DROVE.dataSource.drove-svc-src-1.services.fetch.failure"));
        }
    }

    // ==================== services() - Failure ====================

    @Test
    @SneakyThrows
    void testServices_failure_recordsFetchFailure(WireMockRuntimeInfo wm) {
        stubFor(get("/apis/v1/applications")
                .withBasicAuth("guest", "guest")
                .willReturn(aResponse().withStatus(500).withBody("Error")));

        val config = buildConfig(wm, "drove-svc-src-2");
        try (val droveClient = buildClient(config)) {
            val dataSource = new DroveServiceDataSource<>(
                    config, MAPPER, "testns", droveClient);

            assertThrows(DroveCommunicationException.class, dataSource::services);

            // Verify services fetch failure
            val failureMeter = metricRegistry.getMeters().get(
                    METRIC_PREFIX + ".dataStoreType.DROVE.dataSource.drove-svc-src-2.services.fetch.failure");
            assertNotNull(failureMeter, "Services fetch failure meter should be recorded");
            assertEquals(1, failureMeter.getCount());
        }
    }

    // ==================== isActive() - Healthy ====================

    @Test
    @SneakyThrows
    void testIsActive_healthy_recordsActiveStatus(WireMockRuntimeInfo wm) {
        // Stub services endpoint to make client healthy (upstreamAvailable defaults true)
        val response = ApiResponse.success(Map.<String, AppSummary>of());
        stubFor(get("/apis/v1/applications")
                .withBasicAuth("guest", "guest")
                .willReturn(okJson(MAPPER.writeValueAsString(response))));

        val config = buildConfig(wm, "drove-svc-src-3");
        try (val droveClient = buildClient(config)) {
            val dataSource = new DroveServiceDataSource<>(
                    config, MAPPER, "testns", droveClient);

            val active = dataSource.isActive();

            assertTrue(active);

            // Verify active status
            val activeMeter = metricRegistry.getMeters().get(
                    METRIC_PREFIX + ".dataStoreType.DROVE.dataSource.drove-svc-src-3.active");
            assertNotNull(activeMeter, "Active status meter should be recorded");
            assertEquals(1, activeMeter.getCount());
        }
    }

    // ==================== isActive() - Unhealthy (after failed call) ====================

    @Test
    @SneakyThrows
    void testIsActive_afterFailedCall_recordsInactiveStatus(WireMockRuntimeInfo wm) {
        stubFor(get("/apis/v1/applications")
                .withBasicAuth("guest", "guest")
                .willReturn(aResponse().withStatus(500).withBody("Error")));

        val config = buildConfig(wm, "drove-svc-src-4");
        try (val droveClient = buildClient(config)) {
            val dataSource = new DroveServiceDataSource<>(
                    config, MAPPER, "testns", droveClient);

            // First call fails and sets upstreamAvailable to false
            assertThrows(DroveCommunicationException.class, dataSource::services);

            val active = dataSource.isActive();

            assertFalse(active);

            // Verify inactive status
            val inactiveMeter = metricRegistry.getMeters().get(
                    METRIC_PREFIX + ".dataStoreType.DROVE.dataSource.drove-svc-src-4.inactive");
            assertNotNull(inactiveMeter, "Inactive status meter should be recorded");
            assertEquals(1, inactiveMeter.getCount());
        }
    }

    // ==================== Helpers ====================

    private DroveUpstreamConfig buildConfig(WireMockRuntimeInfo wm, String upstreamId) {
        return DroveUpstreamConfig.builder()
                .id(upstreamId)
                .endpoints(List.of("http://localhost:" + wm.getHttpPort()))
                .username("guest")
                .password("guest")
                .skipCaching(true)
                .build();
    }

    private DroveCommunicator buildClient(DroveUpstreamConfig config) {
        return RangerDroveUtils.buildDroveClient("testns", config, MAPPER);
    }
}
