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
package io.appform.ranger.http.servicefinderhub;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import io.appform.ranger.core.model.Service;
import io.appform.ranger.core.util.MetricRecorder;
import io.appform.ranger.http.config.HttpClientConfig;
import io.appform.ranger.http.model.ServiceDataSourceResponse;
import io.appform.ranger.http.utils.RangerHttpUtils;
import lombok.val;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.junit.jupiter.api.Assertions.*;

@WireMockTest
class HttpServiceDataSourceMetricsIntegrationTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String METRIC_PREFIX = "io.appform.ranger";
    private MetricRegistry metricRegistry;

    @BeforeEach
    void setUp() {
        metricRegistry = new MetricRegistry();
        MetricRecorder.initialize(metricRegistry);
    }

    @Test
    void testServices_success_recordsServicesFetchSuccess(WireMockRuntimeInfo wmInfo) throws Exception {
        val responseObj = ServiceDataSourceResponse.builder()
                .data(Set.of(new Service("ns", "svc1")))
                .build();
        stubFor(get(urlPathEqualTo("/ranger/services/v1"))
                .willReturn(aResponse()
                        .withBody(MAPPER.writeValueAsBytes(responseObj))
                        .withStatus(200)));

        val config = buildConfig(wmInfo, "sds-metric-1");
        val httpServiceDataSource = new HttpServiceDataSource<>(
                "sds-metric-1", config, RangerHttpUtils.httpClient(config, MAPPER));
        val services = httpServiceDataSource.services();

        assertNotNull(services);
        assertFalse(services.isEmpty());

        // Verify services fetch success meter
        val successMeter = metricRegistry.getMeters().get(
                METRIC_PREFIX + ".dataStoreType.HTTP.dataSource.sds-metric-1.services.fetch.success");
        assertNotNull(successMeter, "Services fetch success meter should exist");
        assertEquals(1, successMeter.getCount());

        // No failure
        assertNull(metricRegistry.getMeters().get(
                METRIC_PREFIX + ".dataStoreType.HTTP.dataSource.sds-metric-1.services.fetch.failure"));
    }

    @Test
    void testServices_failure_recordsServicesFetchFailure(WireMockRuntimeInfo wmInfo) {
        stubFor(get(urlPathEqualTo("/ranger/services/v1"))
                .willReturn(aResponse()
                        .withStatus(500)
                        .withBody("error")));

        val config = buildConfig(wmInfo, "sds-metric-2");
        val httpServiceDataSource = new HttpServiceDataSource<>(
                "sds-metric-2", config, RangerHttpUtils.httpClient(config, MAPPER));

        assertThrows(Exception.class, httpServiceDataSource::services);

        // Verify services fetch failure meter
        val failureMeter = metricRegistry.getMeters().get(
                METRIC_PREFIX + ".dataStoreType.HTTP.dataSource.sds-metric-2.services.fetch.failure");
        assertNotNull(failureMeter, "Services fetch failure meter should exist");
        assertEquals(1, failureMeter.getCount());

        // No success
        assertNull(metricRegistry.getMeters().get(
                METRIC_PREFIX + ".dataStoreType.HTTP.dataSource.sds-metric-2.services.fetch.success"));
    }

    @Test
    void testServices_connectionRefused_recordsServicesFetchFailure() {
        val config = HttpClientConfig.builder()
                .id("sds-metric-3")
                .host("127.0.0.1")
                .port(19997) // not listening
                .connectionTimeoutMs(500)
                .operationTimeoutMs(500)
                .build();
        val httpServiceDataSource = new HttpServiceDataSource<>(
                "sds-metric-3", config, RangerHttpUtils.httpClient(config, MAPPER));

        assertThrows(Exception.class, httpServiceDataSource::services);

        val failureMeter = metricRegistry.getMeters().get(
                METRIC_PREFIX + ".dataStoreType.HTTP.dataSource.sds-metric-3.services.fetch.failure");
        assertNotNull(failureMeter, "Services fetch failure meter should exist on connection error");
        assertEquals(1, failureMeter.getCount());
    }

    @Test
    void testIsActive_recordsDataSourceStatus(WireMockRuntimeInfo wmInfo) {
        val config = buildConfig(wmInfo, "sds-metric-4");
        val httpServiceDataSource = new HttpServiceDataSource<>(
                "sds-metric-4", config, RangerHttpUtils.httpClient(config, MAPPER));

        val active = httpServiceDataSource.isActive();
        assertTrue(active, "HTTP data source should always be active");

        // HttpNodeDataStoreConnector.isActive() records status with config.getId()
        val statusMeter = metricRegistry.getMeters().get(
                METRIC_PREFIX + ".dataStoreType.HTTP.dataSource.sds-metric-4.active");
        assertNotNull(statusMeter, "Active status meter should be recorded");
        assertEquals(1, statusMeter.getCount());
    }

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
