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
package io.appform.ranger.drove.common;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.http.Fault;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import com.phonepe.drove.models.api.ApiResponse;
import com.phonepe.drove.models.api.AppSummary;
import com.phonepe.drove.models.api.ExposedAppInfo;
import com.phonepe.drove.models.application.ApplicationState;
import com.phonepe.drove.models.application.PortType;
import io.appform.ranger.core.model.Service;
import io.appform.ranger.core.util.MetricRecorder;
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
 * Integration tests for DroveApiCommunicator metrics recording.
 * Tests verify that metrics are pushed through actual Drove API calls via WireMock.
 */
@WireMockTest
class DroveApiCommunicatorMetricsIntegrationTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String METRIC_PREFIX = "io.appform.ranger";
    private MetricRegistry metricRegistry;

    @BeforeEach
    void setUp() {
        metricRegistry = new MetricRegistry();
        MetricRecorder.initialize(metricRegistry);
    }

    // ==================== services() - Success with 200 ====================

    @Test
    @SneakyThrows
    void testServices_success_recordsStatusCodeAndNoFailure(WireMockRuntimeInfo wm) {
        val response = ApiResponse.success(Map.of(
                "APP_1", new AppSummary("APP_1", "APP_1", 4, 4, 4, 1024, Map.of(),
                        ApplicationState.RUNNING, new Date(), new Date()),
                "APP_2", new AppSummary("APP_2", "APP_2", 4, 4, 4, 1024, Map.of(),
                        ApplicationState.RUNNING, new Date(), new Date())));

        stubFor(get("/apis/v1/applications")
                .withBasicAuth("guest", "guest")
                .willReturn(okJson(MAPPER.writeValueAsString(response))));

        try (val client = buildClient(wm, "drove-api-1")) {
            val services = client.services();
            assertNotNull(services);
            assertEquals(2, services.size());

            // Verify 200 status code meter
            val statusMeter = metricRegistry.getMeters().get(
                    METRIC_PREFIX + ".dataStoreType.DROVE.dataSource.drove-api-1.httpCall.services.responseStatus.200");
            assertNotNull(statusMeter, "200 status meter should be recorded for services");
            assertEquals(1, statusMeter.getCount());
        }
    }

    // ==================== services() - Empty data (null/empty services response) ====================

    @Test
    @SneakyThrows
    void testServices_emptyData_recordsNullOrEmptyServicesResponse(WireMockRuntimeInfo wm) {
        val response = ApiResponse.success(Map.<String, AppSummary>of()); // empty map

        stubFor(get("/apis/v1/applications")
                .withBasicAuth("guest", "guest")
                .willReturn(okJson(MAPPER.writeValueAsString(response))));

        try (val client = buildClient(wm, "drove-api-2")) {
            val services = client.services();
            assertNotNull(services);
            assertTrue(services.isEmpty());

            // Verify null/empty services meter
            val emptyMeter = metricRegistry.getMeters().get(
                    METRIC_PREFIX + ".dataStoreType.DROVE.dataSource.drove-api-2.httpCall.services.nullOrEmptyResponse");
            assertNotNull(emptyMeter, "Null/empty services response meter should be recorded");
            assertEquals(1, emptyMeter.getCount());
        }
    }

    // ==================== services() - Non-200 (error) ====================

    @Test
    @SneakyThrows
    void testServices_serverError_recordsStatusCode(WireMockRuntimeInfo wm) {
        stubFor(get("/apis/v1/applications")
                .withBasicAuth("guest", "guest")
                .willReturn(aResponse().withStatus(500).withBody("Internal Server Error")));

        try (val client = buildClient(wm, "drove-api-3")) {
            assertThrows(DroveCommunicationException.class, client::services);

            // Verify 500 status code meter
            val statusMeter = metricRegistry.getMeters().get(
                    METRIC_PREFIX + ".dataStoreType.DROVE.dataSource.drove-api-3.httpCall.services.responseStatus.500");
            assertNotNull(statusMeter, "500 status meter should be recorded");
            assertEquals(1, statusMeter.getCount());
        }
    }

    // ==================== services() - Invalid JSON (parse failure) ====================

    @Test
    @SneakyThrows
    void testServices_invalidJson_recordsParseFailure(WireMockRuntimeInfo wm) {
        stubFor(get("/apis/v1/applications")
                .withBasicAuth("guest", "guest")
                .willReturn(aResponse().withStatus(200).withBody("not-valid-json{{{")));

        try (val client = buildClient(wm, "drove-api-4")) {
            assertThrows(DroveCommunicationException.class, client::services);

            // Verify parse failure meter
            val parseMeter = metricRegistry.getMeters().get(
                    METRIC_PREFIX + ".dataStoreType.DROVE.dataSource.drove-api-4.httpCall.services.responseParseFailure");
            assertNotNull(parseMeter, "Services parse failure meter should be recorded");
            assertEquals(1, parseMeter.getCount());
        }
    }

    // ==================== services() - Network error (unknown failure from OkHttp transport) ====================

    @Test
    @SneakyThrows
    void testServices_networkError_recordsRemoteCallUnknownFailure(WireMockRuntimeInfo wm) {
        stubFor(get("/apis/v1/applications")
                .withBasicAuth("guest", "guest")
                .willReturn(aResponse().withFault(Fault.MALFORMED_RESPONSE_CHUNK)));

        try (val client = buildClient(wm, "drove-api-5")) {
            assertThrows(DroveCommunicationException.class, client::services);

            // Verify remote call unknown failure meter (from DroveOkHttpTransport.get())
            val failureMeter = metricRegistry.getMeters().get(
                    METRIC_PREFIX + ".dataStoreType.DROVE.dataSource.drove-api-5.httpCall.GET.unknownFailure");
            assertNotNull(failureMeter, "Remote call unknown failure meter should be recorded");
            assertTrue(failureMeter.getCount() >= 1);
        }
    }

    // ==================== listNodes() - Success with 200 ====================

    @Test
    @SneakyThrows
    void testListNodes_success_recordsStatusCode(WireMockRuntimeInfo wm) {
        val nodeResponse = ApiResponse.success(List.of(
                new ExposedAppInfo("TEST_APP", "v1", "host.internal", Map.of(),
                        List.of(new ExposedAppInfo.ExposedHost("host1.internal", 32000, PortType.HTTP)))));

        stubFor(get(urlPathEqualTo("/apis/v1/endpoints"))
                .withBasicAuth("guest", "guest")
                .willReturn(aResponse()
                        .withBody(MAPPER.writeValueAsBytes(nodeResponse))
                        .withStatus(200)));

        try (val client = buildClient(wm, "drove-api-6")) {
            val service = new Service("testns", "TEST_APP");
            val nodes = client.listNodes(service);

            assertNotNull(nodes);
            assertFalse(nodes.isEmpty());

            // Verify 200 status code for listNodes
            val statusMeter = metricRegistry.getMeters().get(
                    METRIC_PREFIX + ".dataStoreType.DROVE.dataSource.drove-api-6.httpCall.listNodes.responseStatus.200");
            assertNotNull(statusMeter, "200 status meter for listNodes should exist");
            assertEquals(1, statusMeter.getCount());
        }
    }

    // ==================== listNodes() - Empty response ====================

    @Test
    @SneakyThrows
    void testListNodes_emptyData_recordsNullOrEmptyListNodeResponse(WireMockRuntimeInfo wm) {
        val nodeResponse = ApiResponse.success(List.<ExposedAppInfo>of()); // empty list

        stubFor(get(urlPathEqualTo("/apis/v1/endpoints"))
                .withBasicAuth("guest", "guest")
                .willReturn(aResponse()
                        .withBody(MAPPER.writeValueAsBytes(nodeResponse))
                        .withStatus(200)));

        try (val client = buildClient(wm, "drove-api-7")) {
            val service = new Service("testns", "TEST_APP");
            val nodes = client.listNodes(service);

            assertNotNull(nodes);
            assertTrue(nodes.isEmpty());

            // Verify null/empty list node response
            val aggregateMeter = metricRegistry.getMeters().get(
                    METRIC_PREFIX + ".dataStoreType.DROVE.dataSource.drove-api-7.httpCall.listNodes.nullOrEmptyResponse");
            assertNotNull(aggregateMeter, "Aggregate null/empty list node response meter should be recorded");
            assertEquals(1, aggregateMeter.getCount());

            val emptyMeter = metricRegistry.getMeters().get(
                    METRIC_PREFIX + ".dataStoreType.DROVE.dataSource.drove-api-7.httpCall.listNodes.serviceName.TEST_APP.nullOrEmptyResponse");
            assertNotNull(emptyMeter, "Null/empty list node response meter should be recorded");
            assertEquals(1, emptyMeter.getCount());
        }
    }

    // ==================== listNodes() - Invalid JSON (parse failure) ====================

    @Test
    @SneakyThrows
    void testListNodes_invalidJson_recordsParseFailure(WireMockRuntimeInfo wm) {
        stubFor(get(urlPathEqualTo("/apis/v1/endpoints"))
                .withBasicAuth("guest", "guest")
                .willReturn(aResponse().withStatus(200).withBody("invalid-json{{{{")));

        try (val client = buildClient(wm, "drove-api-8")) {
            val service = new Service("testns", "PARSE_FAIL_APP");
            assertThrows(DroveCommunicationException.class, () -> client.listNodes(service));

            // Verify list nodes parse failure
            val aggregateParseMeter = metricRegistry.getMeters().get(
                    METRIC_PREFIX + ".dataStoreType.DROVE.dataSource.drove-api-8.httpCall.listNodes.responseParseFailure");
            assertNotNull(aggregateParseMeter, "Aggregate list nodes parse failure meter should be recorded");
            assertEquals(1, aggregateParseMeter.getCount());

            val parseMeter = metricRegistry.getMeters().get(
                    METRIC_PREFIX + ".dataStoreType.DROVE.dataSource.drove-api-8.httpCall.listNodes.serviceName.PARSE_FAIL_APP.responseParseFailure");
            assertNotNull(parseMeter, "List nodes parse failure meter should be recorded");
            assertEquals(1, parseMeter.getCount());
        }
    }

    // ==================== listNodes() - Non-200 (error) ====================

    @Test
    @SneakyThrows
    void testListNodes_serverError_recordsStatusCode(WireMockRuntimeInfo wm) {
        stubFor(get(urlPathEqualTo("/apis/v1/endpoints"))
                .withBasicAuth("guest", "guest")
                .willReturn(aResponse().withStatus(503).withBody("Service Unavailable")));

        try (val client = buildClient(wm, "drove-api-9")) {
            val service = new Service("testns", "ERROR_APP");
            assertThrows(DroveCommunicationException.class, () -> client.listNodes(service));

            // Verify 503 status code
            val statusMeter = metricRegistry.getMeters().get(
                    METRIC_PREFIX + ".dataStoreType.DROVE.dataSource.drove-api-9.httpCall.listNodes.responseStatus.503");
            assertNotNull(statusMeter, "503 status meter for listNodes should be recorded");
            assertEquals(1, statusMeter.getCount());
        }
    }

    // ==================== listNodes() - Multi-service: aggregate pushed only once ====================

    @Test
    @SneakyThrows
    void testListNodes_multiServiceEmptyResponse_aggregateMetricPushedOnce(WireMockRuntimeInfo wm) {
        val nodeResponse = ApiResponse.success(List.<ExposedAppInfo>of()); // empty list

        stubFor(get(urlPathEqualTo("/apis/v1/endpoints"))
                .withBasicAuth("guest", "guest")
                .willReturn(aResponse()
                        .withBody(MAPPER.writeValueAsBytes(nodeResponse))
                        .withStatus(200)));

        try (val client = buildClient(wm, "drove-api-10")) {
            val service1 = new Service("testns", "APP_ONE");
            val service2 = new Service("testns", "APP_TWO");
            val result = client.listNodes(List.of(service1, service2));

            assertNotNull(result);
            assertTrue(result.isEmpty());

            // Aggregate metric must be pushed exactly once regardless of how many services were in the batch
            val aggregateMeter = metricRegistry.getMeters().get(
                    METRIC_PREFIX + ".dataStoreType.DROVE.dataSource.drove-api-10.httpCall.listNodes.nullOrEmptyResponse");
            assertNotNull(aggregateMeter, "Aggregate null/empty metric should be recorded");
            assertEquals(1, aggregateMeter.getCount(),
                    "Aggregate metric must be pushed exactly once even for multi-service batches");

            // Service-level metrics pushed once per service
            val svc1Meter = metricRegistry.getMeters().get(
                    METRIC_PREFIX + ".dataStoreType.DROVE.dataSource.drove-api-10.httpCall.listNodes.serviceName.APP_ONE.nullOrEmptyResponse");
            assertNotNull(svc1Meter, "Service-level metric for APP_ONE should be recorded");
            assertEquals(1, svc1Meter.getCount());

            val svc2Meter = metricRegistry.getMeters().get(
                    METRIC_PREFIX + ".dataStoreType.DROVE.dataSource.drove-api-10.httpCall.listNodes.serviceName.APP_TWO.nullOrEmptyResponse");
            assertNotNull(svc2Meter, "Service-level metric for APP_TWO should be recorded");
            assertEquals(1, svc2Meter.getCount());
        }
    }

    @Test
    @SneakyThrows
    void testListNodes_multiServiceParseFailure_aggregateMetricPushedOnce(WireMockRuntimeInfo wm) {
        stubFor(get(urlPathEqualTo("/apis/v1/endpoints"))
                .withBasicAuth("guest", "guest")
                .willReturn(aResponse().withStatus(200).withBody("invalid-json{{{{")));

        try (val client = buildClient(wm, "drove-api-11")) {
            val service1 = new Service("testns", "APP_ONE");
            val service2 = new Service("testns", "APP_TWO");
            assertThrows(DroveCommunicationException.class, () -> client.listNodes(List.of(service1, service2)));

            // Aggregate metric must be pushed exactly once
            val aggregateParseMeter = metricRegistry.getMeters().get(
                    METRIC_PREFIX + ".dataStoreType.DROVE.dataSource.drove-api-11.httpCall.listNodes.responseParseFailure");
            assertNotNull(aggregateParseMeter, "Aggregate parse failure metric should be recorded");
            assertEquals(1, aggregateParseMeter.getCount(),
                    "Aggregate metric must be pushed exactly once even for multi-service batches");

            // Service-level metrics pushed once per service
            val svc1Meter = metricRegistry.getMeters().get(
                    METRIC_PREFIX + ".dataStoreType.DROVE.dataSource.drove-api-11.httpCall.listNodes.serviceName.APP_ONE.responseParseFailure");
            assertNotNull(svc1Meter, "Service-level parse failure metric for APP_ONE should be recorded");
            assertEquals(1, svc1Meter.getCount());

            val svc2Meter = metricRegistry.getMeters().get(
                    METRIC_PREFIX + ".dataStoreType.DROVE.dataSource.drove-api-11.httpCall.listNodes.serviceName.APP_TWO.responseParseFailure");
            assertNotNull(svc2Meter, "Service-level parse failure metric for APP_TWO should be recorded");
            assertEquals(1, svc2Meter.getCount());
        }
    }

    // ==================== Helper ====================

    private DroveCommunicator buildClient(WireMockRuntimeInfo wm, String upstreamId) {
        return RangerDroveUtils.buildDroveClient(
                "testns",
                DroveUpstreamConfig.builder()
                        .id(upstreamId)
                        .endpoints(List.of("http://localhost:" + wm.getHttpPort()))
                        .username("guest")
                        .password("guest")
                        .skipCaching(true)
                        .build(),
                MAPPER);
    }
}
