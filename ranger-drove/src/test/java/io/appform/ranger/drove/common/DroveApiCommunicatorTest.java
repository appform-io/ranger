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
import io.appform.ranger.drove.config.DroveUpstreamConfig;
import io.appform.ranger.drove.utils.RangerDroveUtils;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.jupiter.api.Test;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for api based communicator
 */
@WireMockTest
class DroveApiCommunicatorTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    @SneakyThrows
    void testSuccess(final WireMockRuntimeInfo wm) {

        val response = ApiResponse.success(Map.of(
                "TEST_APP-1",
                new AppSummary("TEST_APP-1",
                               "TEST_APP",
                               4,
                               4,
                               4,
                               1024,
                               Map.of(),
                               ApplicationState.RUNNING,
                               new Date(),
                               new Date()),
                "TEST_APP-2",
                new AppSummary("TEST_APP-2",
                               "TEST_APP",
                               4,
                               4,
                               4,
                               1024,
                               Map.of(),
                               ApplicationState.RUNNING,
                               new Date(),
                               new Date()),
                "DEAD_APP-2",
                new AppSummary("DEAD_APP-2",
                               "DEAD_APP",
                               0,
                               0,
                               4,
                               1024,
                               Map.of(),
                               ApplicationState.MONITORING,
                               new Date(),
                               new Date()),
                "OTHER_APP-2",
                new AppSummary("OTHER_APP-2",
                               "OTHER_APP",
                               4,
                               4,
                               4,
                               1024,
                               Map.of(),
                               ApplicationState.RUNNING,
                               new Date(),
                               new Date())));
        stubFor(get("/apis/v1/applications")
                        .withBasicAuth("guest", "guest")
                        .willReturn(okJson(MAPPER.writeValueAsString(
                                response))));
        stubFor(get(urlPathEqualTo("/apis/v1/endpoints"))
                        .willReturn(aResponse()
                                            .withBody(MAPPER.writeValueAsBytes(
                                                    ApiResponse.success(List.of(new ExposedAppInfo(
                                                            "TEST_APP",
                                                            "test-0.1",
                                                            "test.appform.io",
                                                            Map.of(),
                                                            List.of(new ExposedAppInfo.ExposedHost(
                                                                    "executor001.internal",
                                                                    32456,
                                                                    PortType.HTTP)))))))
                                            .withStatus(200)));
        try (val client = RangerDroveUtils.buildDroveClient(
                "testns",
                DroveUpstreamConfig.builder()
                        .endpoints(List.of("http://localhost:" + wm.getHttpPort()))
                        .username("guest")
                        .password("guest")
                        .skipCaching(true)
                        .build(),
                MAPPER)) {

            val services = client.services();
            assertFalse(services.isEmpty());
            assertEquals(2, services.size());
            assertFalse(client.listNodes(Service.builder()
                                                 .namespace("testns")
                                                 .serviceName("TEST_APP")
                                                 .build())
                                .isEmpty());
            assertTrue(client.listNodes(Service.builder()
                                                 .namespace("testns")
                                                 .serviceName("OTHER_APP")
                                                 .build())
                               .isEmpty());
        }
    }


    @Test
    void testServicesAuthFail(final WireMockRuntimeInfo wm) {
        testAuthFail(wm, "/apis/v1/applications",
                     client -> assertThrows(DroveCommunicationException.class, client::services));
    }

    @Test
    void testLoadAuthFail(final WireMockRuntimeInfo wm) {
        val service = Service.builder()
                .namespace("testns")
                .serviceName("TEST_APP")
                .build();
        testAuthFail(wm, "/apis/v1/endpoints",
                     client -> assertThrows(DroveCommunicationException.class,
                                            () -> client.listNodes(service)));
    }

    @Test
    @SneakyThrows
    void testServicesNetworkError(final WireMockRuntimeInfo wm) {
        testNetworkFail(wm, "/apis/v1/applications",
                        client -> assertThrows(DroveCommunicationException.class, client::services));
    }

    @Test
    void testLoadNetworkFail(final WireMockRuntimeInfo wm) {
        val service = Service.builder()
                .namespace("testns")
                .serviceName("TEST_APP")
                .build();
        testNetworkFail(wm, "/apis/v1/endpoints",
                     client -> assertThrows(DroveCommunicationException.class,
                                            () -> client.listNodes(service)));
    }
    @Test
    void testServiceApiFail(final WireMockRuntimeInfo wm) {
        testApiFail(wm, "/apis/v1/applications",
                    client -> assertThrows(DroveCommunicationException.class, client::services));
    }

    @Test
    void testLoadApiFail(final WireMockRuntimeInfo wm) {
        val service = Service.builder()
                .namespace("testns")
                .serviceName("TEST_APP")
                .build();
        testApiFail(wm, "/apis/v1/endpoints",
                    client -> assertThrows(DroveCommunicationException.class, () -> client.listNodes(service)));
    }


    @SneakyThrows
    private void testAuthFail(
            final WireMockRuntimeInfo wm,
            final String api,
            Consumer<DroveCommunicator> test) {
        stubFor(get(urlPathEqualTo(api))
                        .withBasicAuth("guest", "guest")
                        .willReturn(okJson(MAPPER.writeValueAsString(
                                ApiResponse.success(List.of())))));
        try (val client = RangerDroveUtils.buildDroveClient(
                "testns",
                DroveUpstreamConfig.builder()
                        .endpoints(List.of("http://localhost:" + wm.getHttpPort()))
                        .username("guest")
                        .password("wrong")
                        .skipCaching(true)
                        .build(),
                MAPPER)) {

            test.accept(client);
        }
    }

    @SneakyThrows
    private void testApiFail(
            final WireMockRuntimeInfo wm,
            final String api,
            Consumer<DroveCommunicator> test) {
        stubFor(get(urlPathEqualTo(api))
                        .withBasicAuth("guest", "guest")
                        .willReturn(okJson(MAPPER.writeValueAsString(
                                ApiResponse.failure("Emotional Damage!!")))));
        try (val client = RangerDroveUtils.buildDroveClient(
                "testns",
                DroveUpstreamConfig.builder()
                        .endpoints(List.of("http://localhost:" + wm.getHttpPort()))
                        .username("guest")
                        .password("guest")
                        .skipCaching(true)
                        .build(),
                MAPPER)) {

            test.accept(client);
        }
    }

    @SneakyThrows
    private void testNetworkFail(
            final WireMockRuntimeInfo wm,
            final String api,
            Consumer<DroveCommunicator> test) {
        stubFor(get(urlPathEqualTo(api))
                        .withBasicAuth("guest", "guest")
                        .willReturn(aResponse().withFault(Fault.MALFORMED_RESPONSE_CHUNK)));
        try (val client = RangerDroveUtils.buildDroveClient(
                "testns",
                DroveUpstreamConfig.builder()
                        .endpoints(List.of("http://localhost:" + wm.getHttpPort()))
                        .username("guest")
                        .password("guest")
                        .skipCaching(true)
                        .build(),
                MAPPER)) {

            test.accept(client);
        }
    }
}