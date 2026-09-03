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
package io.appform.ranger.http.servicefinder;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import io.appform.ranger.core.model.Service;
import io.appform.ranger.core.model.ServiceNode;
import io.appform.ranger.core.util.MetricRecorder;
import io.appform.ranger.http.config.HttpClientConfig;
import io.appform.ranger.http.model.ServiceDataSourceResponse;
import io.appform.ranger.http.model.ServiceNodesResponse;
import io.appform.ranger.http.serde.HTTPResponseDataDeserializer;
import io.appform.ranger.http.utils.RangerHttpUtils;
import lombok.val;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Set;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.junit.jupiter.api.Assertions.*;

@WireMockTest
class HttpApiCommunicatorMetricsIntegrationTest {

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
    void testServices_success_recordsStatusCodeAndNoFailure(WireMockRuntimeInfo wmInfo) throws Exception {
        val responseObj = ServiceDataSourceResponse.builder()
                .data(Set.of(new Service("ns", "svc1"), new Service("ns", "svc2")))
                .build();
        stubFor(get(urlPathEqualTo("/ranger/services/v1"))
                .willReturn(aResponse()
                        .withBody(MAPPER.writeValueAsBytes(responseObj))
                        .withStatus(200)));

        val config = buildConfig(wmInfo, "http-metric-1");
        val communicator = RangerHttpUtils.<String>httpClient(config, MAPPER);
        val services = communicator.services();

        assertNotNull(services);
        assertEquals(2, services.size());

        // Verify status code 200 meter
        val statusMeter = metricRegistry.getMeters().get(
                METRIC_PREFIX + ".dataStoreType.HTTP.dataSource.http-metric-1.httpCall.services.responseStatus.200");
        assertNotNull(statusMeter, "200 status meter should be recorded");
        assertEquals(1, statusMeter.getCount());

        // No unknown failure
        assertNull(metricRegistry.getMeters().get(
                METRIC_PREFIX + ".dataStoreType.HTTP.dataSource.http-metric-1.httpCall.services.unknownFailure"));

        // No parse failure
        assertNull(metricRegistry.getMeters().get(
                METRIC_PREFIX + ".dataStoreType.HTTP.dataSource.http-metric-1.httpCall.services.responseParseFailure"));
    }

    // ==================== services() - Empty response ====================

    @Test
    void testServices_emptyData_recordsNullOrEmptyServicesResponse(WireMockRuntimeInfo wmInfo) throws Exception {
        val responseObj = ServiceDataSourceResponse.builder()
                .data(Set.of()) // empty set
                .build();
        stubFor(get(urlPathEqualTo("/ranger/services/v1"))
                .willReturn(aResponse()
                        .withBody(MAPPER.writeValueAsBytes(responseObj))
                        .withStatus(200)));

        val config = buildConfig(wmInfo, "http-metric-2");
        val communicator = RangerHttpUtils.<String>httpClient(config, MAPPER);

        // Empty set is valid (data != null), so services() returns successfully with empty set
        val services = communicator.services();
        assertNotNull(services);
        assertTrue(services.isEmpty());

        // Verify null/empty services meter is still recorded
        val emptyMeter = metricRegistry.getMeters().get(
                METRIC_PREFIX + ".dataStoreType.HTTP.dataSource.http-metric-2.httpCall.services.nullOrEmptyResponse");
        assertNotNull(emptyMeter, "Null/empty services meter should be recorded");
        assertEquals(1, emptyMeter.getCount());
    }

    // ==================== services() - Non-200 response ====================

    @Test
    void testServices_serverError_recordsStatusCodeAndUnknownFailure(WireMockRuntimeInfo wmInfo) {
        stubFor(get(urlPathEqualTo("/ranger/services/v1"))
                .willReturn(aResponse()
                        .withStatus(500)
                        .withBody("Internal Server Error")));

        val config = buildConfig(wmInfo, "http-metric-3");
        val communicator = RangerHttpUtils.<String>httpClient(config, MAPPER);

        assertThrows(Exception.class, communicator::services);

        // Verify 500 status code meter
        val statusMeter = metricRegistry.getMeters().get(
                METRIC_PREFIX + ".dataStoreType.HTTP.dataSource.http-metric-3.httpCall.services.responseStatus.500");
        assertNotNull(statusMeter, "500 status meter should be recorded");
        assertEquals(1, statusMeter.getCount());

        // Verify unknown failure meter (HttpCommunicationException re-thrown)
        val failureMeter = metricRegistry.getMeters().get(
                METRIC_PREFIX + ".dataStoreType.HTTP.dataSource.http-metric-3.httpCall.services.unknownFailure");
        assertNotNull(failureMeter, "Unknown failure meter should be recorded");
        assertTrue(failureMeter.getCount() >= 1);
    }

    // ==================== services() - Invalid JSON (parse failure) ====================

    @Test
    void testServices_invalidJson_recordsParseFailure(WireMockRuntimeInfo wmInfo) {
        stubFor(get(urlPathEqualTo("/ranger/services/v1"))
                .willReturn(aResponse()
                        .withBody("not-valid-json{{{")
                        .withStatus(200)));

        val config = buildConfig(wmInfo, "http-metric-4");
        val communicator = RangerHttpUtils.<String>httpClient(config, MAPPER);

        assertThrows(Exception.class, communicator::services);

        // Verify parse failure meter
        val parseMeter = metricRegistry.getMeters().get(
                METRIC_PREFIX + ".dataStoreType.HTTP.dataSource.http-metric-4.httpCall.services.responseParseFailure");
        assertNotNull(parseMeter, "Services parse failure meter should be recorded");
        assertEquals(1, parseMeter.getCount());
    }

    // ==================== services() - Connection refused (unknown failure) ====================

    @Test
    void testServices_connectionRefused_recordsUnknownFailure() {
        // Use a port that is not listening
        val config = HttpClientConfig.builder()
                .id("http-metric-5")
                .host("127.0.0.1")
                .port(19999) // unlikely to have something listening
                .connectionTimeoutMs(500)
                .operationTimeoutMs(500)
                .build();
        val communicator = RangerHttpUtils.<String>httpClient(config, MAPPER);

        assertThrows(Exception.class, communicator::services);

        // Verify unknown failure meter
        val failureMeter = metricRegistry.getMeters().get(
                METRIC_PREFIX + ".dataStoreType.HTTP.dataSource.http-metric-5.httpCall.services.unknownFailure");
        assertNotNull(failureMeter, "Unknown failure meter should be recorded on connection error");
        assertEquals(1, failureMeter.getCount());
    }

    // ==================== listNodes() - Success ====================

    @Test
    void testListNodes_success_recordsStatusCode(WireMockRuntimeInfo wmInfo) throws Exception {
        val nodeResponse = ServiceNodesResponse.<String>builder()
                .data(List.of(
                        ServiceNode.<String>builder().host("h1").port(8080).nodeData("data1").build(),
                        ServiceNode.<String>builder().host("h2").port(8081).nodeData("data2").build()
                ))
                .build();
        stubFor(get(urlPathEqualTo("/ranger/nodes/v1/test-ns/test-svc"))
                .willReturn(aResponse()
                        .withBody(MAPPER.writeValueAsBytes(nodeResponse))
                        .withStatus(200)));

        val config = buildConfig(wmInfo, "http-metric-6");
        val communicator = RangerHttpUtils.<String>httpClient(config, MAPPER);
        val service = new Service("test-ns", "test-svc");

        HTTPResponseDataDeserializer<String> deserializer = data -> {
            try {
                return MAPPER.readValue(data, MAPPER.getTypeFactory()
                        .constructParametricType(ServiceNodesResponse.class, String.class));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };

        val nodes = communicator.listNodes(service, deserializer);

        assertNotNull(nodes);
        assertEquals(2, nodes.size());

        // Verify 200 status code for listNodes
        val statusMeter = metricRegistry.getMeters().get(
                METRIC_PREFIX + ".dataStoreType.HTTP.dataSource.http-metric-6.httpCall.listNodes.responseStatus.200");
        assertNotNull(statusMeter, "200 status meter for listNodes should exist");
        assertEquals(1, statusMeter.getCount());
    }

    // ==================== listNodes() - Server error ====================

    @Test
    void testListNodes_serverError_recordsStatusCodeAndUnknownFailure(WireMockRuntimeInfo wmInfo) {
        stubFor(get(urlPathEqualTo("/ranger/nodes/v1/test-ns/error-svc"))
                .willReturn(aResponse()
                        .withStatus(503)
                        .withBody("Service Unavailable")));

        val config = buildConfig(wmInfo, "http-metric-7");
        val communicator = RangerHttpUtils.<String>httpClient(config, MAPPER);
        val service = new Service("test-ns", "error-svc");

        HTTPResponseDataDeserializer<String> deserializer = data -> {
            try {
                return MAPPER.readValue(data, MAPPER.getTypeFactory()
                        .constructParametricType(ServiceNodesResponse.class, String.class));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };

        assertThrows(Exception.class, () -> communicator.listNodes(service, deserializer));

        // Verify 503 status code
        val statusMeter = metricRegistry.getMeters().get(
                METRIC_PREFIX + ".dataStoreType.HTTP.dataSource.http-metric-7.httpCall.listNodes.responseStatus.503");
        assertNotNull(statusMeter, "503 status meter for listNodes should exist");
        assertEquals(1, statusMeter.getCount());

        // Verify unknown failure (with service name)
        val failureMeter = metricRegistry.getMeters().get(
                METRIC_PREFIX + ".dataStoreType.HTTP.dataSource.http-metric-7.httpCall.listNodes.unknownFailure");
        assertNotNull(failureMeter, "Unknown failure meter for listNodes should exist");
        assertTrue(failureMeter.getCount() >= 1);
    }

    // ==================== listNodes() - Empty body ====================

    @Test
    void testListNodes_emptyBody_recordsNullOrEmptyListNodeResponse(WireMockRuntimeInfo wmInfo) {
        stubFor(get(urlPathEqualTo("/ranger/nodes/v1/test-ns/empty-svc"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBody(""))); // empty body

        val config = buildConfig(wmInfo, "http-metric-8");
        val communicator = RangerHttpUtils.<String>httpClient(config, MAPPER);
        val service = new Service("test-ns", "empty-svc");

        HTTPResponseDataDeserializer<String> deserializer = data -> {
            try {
                return MAPPER.readValue(data, MAPPER.getTypeFactory()
                        .constructParametricType(ServiceNodesResponse.class, String.class));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };

        assertThrows(Exception.class, () -> communicator.listNodes(service, deserializer));

        // The 200 status code should be recorded
        val statusMeter = metricRegistry.getMeters().get(
                METRIC_PREFIX + ".dataStoreType.HTTP.dataSource.http-metric-8.httpCall.listNodes.responseStatus.200");
        assertNotNull(statusMeter, "200 status meter should exist even for empty body");
        assertEquals(1, statusMeter.getCount());
    }

    // ==================== listNodes() - Invalid JSON (parse failure) ====================

    @Test
    void testListNodes_invalidJson_recordsParseFailure(WireMockRuntimeInfo wmInfo) {
        stubFor(get(urlPathEqualTo("/ranger/nodes/v1/test-ns/parse-fail-svc"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBody("{{invalid json}}")));

        val config = buildConfig(wmInfo, "http-metric-9");
        val communicator = RangerHttpUtils.<String>httpClient(config, MAPPER);
        val service = new Service("test-ns", "parse-fail-svc");

        HTTPResponseDataDeserializer<String> deserializer = data -> {
            throw new RuntimeException("Parse error simulation");
        };

        assertThrows(Exception.class, () -> communicator.listNodes(service, deserializer));

        // The listNodes parse failure meter should exist
        val aggregateParseMeter = metricRegistry.getMeters().get(
                METRIC_PREFIX + ".dataStoreType.HTTP.dataSource.http-metric-9.httpCall.listNodes.responseParseFailure");
        assertNotNull(aggregateParseMeter, "Aggregate list nodes parse failure meter should be recorded");
        assertEquals(1, aggregateParseMeter.getCount());

        val parseMeter = metricRegistry.getMeters().get(
                METRIC_PREFIX + ".dataStoreType.HTTP.dataSource.http-metric-9.httpCall.listNodes.serviceName.parse-fail-svc.responseParseFailure");
        assertNotNull(parseMeter, "List nodes parse failure meter should be recorded");
        assertEquals(1, parseMeter.getCount());
    }

    // ==================== listNodes() - Connection refused ====================

    @Test
    void testListNodes_connectionRefused_recordsUnknownFailureWithServiceName() {
        val config = HttpClientConfig.builder()
                .id("http-metric-10")
                .host("127.0.0.1")
                .port(19998)
                .connectionTimeoutMs(500)
                .operationTimeoutMs(500)
                .build();
        val communicator = RangerHttpUtils.<String>httpClient(config, MAPPER);
        val service = new Service("test-ns", "conn-fail-svc");

        HTTPResponseDataDeserializer<String> deserializer = data ->
                ServiceNodesResponse.<String>builder().data(List.of()).build();

        assertThrows(Exception.class, () -> communicator.listNodes(service, deserializer));

        // Verify unknown failure meter (generic)
        val genericFailureMeter = metricRegistry.getMeters().get(
                METRIC_PREFIX + ".dataStoreType.HTTP.dataSource.http-metric-10.httpCall.listNodes.unknownFailure");
        assertNotNull(genericFailureMeter, "Generic unknown failure meter should exist");
        assertEquals(1, genericFailureMeter.getCount());

        // Verify service-specific unknown failure meter
        val svcFailureMeter = metricRegistry.getMeters().get(
                METRIC_PREFIX + ".dataStoreType.HTTP.dataSource.http-metric-10.httpCall.listNodes.serviceName.conn-fail-svc.unknownFailure");
        assertNotNull(svcFailureMeter, "Service-specific unknown failure meter should exist");
        assertEquals(1, svcFailureMeter.getCount());
    }

    // ==================== Helper ====================

    private HttpClientConfig buildConfig(WireMockRuntimeInfo wmInfo, String upstreamId) {
        return HttpClientConfig.builder()
                .id(upstreamId)
                .host("127.0.0.1")
                .port(wmInfo.getHttpPort())
                .connectionTimeoutMs(30_000)
                .operationTimeoutMs(30_000)
                .build();
    }
}
