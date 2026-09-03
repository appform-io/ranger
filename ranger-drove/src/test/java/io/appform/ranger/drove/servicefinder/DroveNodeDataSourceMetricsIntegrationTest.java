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
package io.appform.ranger.drove.servicefinder;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import com.phonepe.drove.models.api.ApiResponse;
import com.phonepe.drove.models.api.ExposedAppInfo;
import com.phonepe.drove.models.application.PortType;
import io.appform.ranger.core.model.Service;
import io.appform.ranger.core.util.MetricRecorder;
import io.appform.ranger.drove.common.DroveCommunicator;
import io.appform.ranger.drove.config.DroveUpstreamConfig;
import io.appform.ranger.drove.serde.DroveResponseDataDeserializer;
import io.appform.ranger.drove.utils.RangerDroveUtils;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for DroveNodeDataSource metrics recording.
 * Tests verify metrics from actual Drove data source flows.
 */
@WireMockTest
class DroveNodeDataSourceMetricsIntegrationTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String METRIC_PREFIX = "io.appform.ranger";
    private MetricRegistry metricRegistry;

    private static final DroveResponseDataDeserializer<String> STRING_DESERIALIZER =
            new DroveResponseDataDeserializer<>() {
                @Override
                protected String translate(ExposedAppInfo appInfo, ExposedAppInfo.ExposedHost host) {
                    return host.getHost() + ":" + host.getPort();
                }
            };

    @BeforeEach
    void setUp() {
        metricRegistry = new MetricRegistry();
        MetricRecorder.initialize(metricRegistry);
    }

    // ==================== isActive() - Healthy upstream ====================

    @Test
    @SneakyThrows
    void testIsActive_healthyUpstream_recordsActiveStatus(WireMockRuntimeInfo wm) {
        // Drove client starts with upstreamAvailable = true
        val config = buildConfig(wm, "drove-node-src-1");
        try (val droveClient = buildClient(config)) {
            val service = new Service("testns", "TEST_APP");
            val dataSource = new DroveNodeDataSource<String, DroveResponseDataDeserializer<String>>(
                    service, config, MAPPER, droveClient);

            val active = dataSource.isActive();

            assertTrue(active);

            val activeMeter = metricRegistry.getMeters().get(
                    METRIC_PREFIX + ".dataStoreType.DROVE.dataSource.drove-node-src-1.active");
            assertNotNull(activeMeter, "Active status meter should be recorded");
            assertEquals(1, activeMeter.getCount());
        }
    }

    // ==================== isActive() - Unhealthy upstream ====================

    @Test
    @SneakyThrows
    void testIsActive_unhealthyUpstream_recordsInactiveStatus(WireMockRuntimeInfo wm) {
        stubFor(get("/apis/v1/applications")
                .withBasicAuth("guest", "guest")
                .willReturn(aResponse().withStatus(500).withBody("Error")));

        val config = buildConfig(wm, "drove-node-src-2");
        try (val droveClient = buildClient(config)) {
            val service = new Service("testns", "TEST_APP");
            val dataSource = new DroveNodeDataSource<String, DroveResponseDataDeserializer<String>>(
                    service, config, MAPPER, droveClient);

            // First call to services fails, setting upstreamAvailable to false
            try {
                droveClient.services();
            } catch (Exception ignored) {
                // Ignored
            }

            val active = dataSource.isActive();

            assertFalse(active);

            val inactiveMeter = metricRegistry.getMeters().get(
                    METRIC_PREFIX + ".dataStoreType.DROVE.dataSource.drove-node-src-2.inactive");
            assertNotNull(inactiveMeter, "Inactive status meter should be recorded");
            assertEquals(1, inactiveMeter.getCount());
        }
    }

    // ==================== refresh() - Success ====================

    @Test
    @SneakyThrows
    void testRefresh_success_returnsNodes(WireMockRuntimeInfo wm) {
        val nodeResponse = ApiResponse.success(List.of(
                new ExposedAppInfo("TEST_APP", "v1", "host.internal", Map.of(),
                        List.of(new ExposedAppInfo.ExposedHost("host1.internal", 32000, PortType.HTTP),
                                new ExposedAppInfo.ExposedHost("host2.internal", 32001, PortType.HTTP)))));

        stubFor(get(urlPathEqualTo("/apis/v1/endpoints"))
                .withBasicAuth("guest", "guest")
                .willReturn(aResponse()
                        .withBody(MAPPER.writeValueAsBytes(nodeResponse))
                        .withStatus(200)));

        val config = buildConfig(wm, "drove-node-src-3");
        try (val droveClient = buildClient(config)) {
            val service = new Service("testns", "TEST_APP");
            val dataSource = new DroveNodeDataSource<String, DroveResponseDataDeserializer<String>>(
                    service, config, MAPPER, droveClient);

            val result = dataSource.refresh(STRING_DESERIALIZER);

            assertTrue(result.isPresent());
            assertEquals(2, result.get().size());

            // Verify 200 status code was recorded (from DroveApiCommunicator)
            val statusMeter = metricRegistry.getMeters().get(
                    METRIC_PREFIX + ".dataStoreType.DROVE.dataSource.drove-node-src-3.httpCall.listNodes.responseStatus.200");
            assertNotNull(statusMeter, "200 status meter should be recorded for listNodes");
            assertEquals(1, statusMeter.getCount());
        }
    }

    // ==================== refresh() - Communication failure ====================

    @Test
    @SneakyThrows
    void testRefresh_communicationFailure_returnsEmpty(WireMockRuntimeInfo wm) {
        stubFor(get(urlPathEqualTo("/apis/v1/endpoints"))
                .withBasicAuth("guest", "guest")
                .willReturn(aResponse().withStatus(500).withBody("Error")));

        val config = buildConfig(wm, "drove-node-src-4");
        try (val droveClient = buildClient(config)) {
            val service = new Service("testns", "FAIL_APP");
            val dataSource = new DroveNodeDataSource<String, DroveResponseDataDeserializer<String>>(
                    service, config, MAPPER, droveClient);

            val result = dataSource.refresh(STRING_DESERIALIZER);

            // On DroveCommunicationException, returns Optional.empty() to maintain old list
            assertFalse(result.isPresent());

            // Verify 500 status code was still recorded
            val statusMeter = metricRegistry.getMeters().get(
                    METRIC_PREFIX + ".dataStoreType.DROVE.dataSource.drove-node-src-4.httpCall.listNodes.responseStatus.500");
            assertNotNull(statusMeter, "500 status meter should be recorded");
            assertEquals(1, statusMeter.getCount());
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
