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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.extension.Extension;
import com.github.tomakehurst.wiremock.extension.Parameters;
import com.github.tomakehurst.wiremock.extension.ServeEventListener;
import com.github.tomakehurst.wiremock.http.Fault;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import com.github.tomakehurst.wiremock.stubbing.ServeEvent;
import com.phonepe.drove.models.api.ApiResponse;
import com.phonepe.drove.models.api.AppSummary;
import com.phonepe.drove.models.api.ExposedAppInfo;
import com.phonepe.drove.models.application.ApplicationState;
import com.phonepe.drove.models.application.PortType;
import com.phonepe.drove.models.events.events.DroveAppStateChangeEvent;
import com.phonepe.drove.models.events.events.datatags.AppEventDataTag;
import io.appform.ranger.core.model.Service;
import io.appform.ranger.core.utils.RangerTestUtils;
import io.appform.ranger.drove.config.DroveUpstreamConfig;
import io.appform.ranger.drove.utils.RangerDroveUtils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests for api based communicator
 */
@Slf4j
class DroveCachingCommunicatorTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @RegisterExtension
    private static final WireMockExtension extension = WireMockExtension.newInstance()
            .options(wireMockConfig()
                             .dynamicPort()
                             .extensions(new StateSetter()))
            .build();

    @AfterAll
    static void shutdown() {
        extension.shutdownServer();
    }

    @Test
    @SneakyThrows
    void testSuccess() {

        setupAppsResponse();
        setupEndpointsResponse();
        try (val client = RangerDroveUtils.buildDroveClient(
                "testns",
                config(),
                MAPPER)) {

            val services = client.services();
            assertFalse(services.isEmpty());
            assertEquals(2, services.size());
            assertFalse(client.listNodes(Service.builder()
                                                 .namespace("testns")
                                                 .serviceName("TEST_APP")
                                                 .build()).isEmpty());
        }
    }

    private static final class StateSetter implements Extension, ServeEventListener {

        @Override
        public void afterComplete(ServeEvent serveEvent, Parameters parameters) {
            if (isInScenario() && serveEvent.getRequest().getUrl().startsWith("/apis/v1/cluster/events")) {
                log.info("Flipping state");
                extension.setScenarioState("EventDrivenLoading", "AFTER_EVENT");
            }
        }

        @Override
        public String getName() {
            return "state_setter";
        }
    }

    @Test
    @SneakyThrows
    void testEventDrivenLoading() {
        val config = config();
        extension.stubFor(get(urlPathEqualTo("/apis/v1/applications"))
                                  .willReturn(okJson(toJson(ApiResponse.success(Map.of())))));
        extension.stubFor(get(urlPathEqualTo("/apis/v1/cluster/events"))
                                  .withServeEventListener(
                                          Set.of(ServeEventListener.RequestPhase.AFTER_COMPLETE),
                                          "state_setter",
                                          Map.of())
                                  .willReturn(okJson(toJson(
                                          ApiResponse.success(
                                                  List.of(new DroveAppStateChangeEvent(
                                                          Map.of(AppEventDataTag.APP_NAME, "TEST_APP"))))))));

        extension.stubFor(get(urlPathEqualTo("/apis/v1/endpoints"))
                                  .inScenario("EventDrivenLoading")
                                  .whenScenarioStateIs("BEFORE_EVENT")
                                  .willReturn(okJson(toJson(ApiResponse.success(List.of())))));

        extension.stubFor(get(urlPathEqualTo("/apis/v1/endpoints"))
                                  .inScenario("EventDrivenLoading")
                                  .whenScenarioStateIs("AFTER_EVENT")
                                  .willReturn(aResponse()
                                                      .withBody(toJson(
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
        extension.setScenarioState("EventDrivenLoading", "BEFORE_EVENT");
        try (val comm = RangerDroveUtils.buildDroveClient("testns",
                                                          config,
                                                          MAPPER)) {
            val service = Service.builder()
                    .namespace("testns")
                    .serviceName("TEST_APP")
                    .build();
            RangerTestUtils.sleepUntil(5, () -> !comm.listNodes(service).isEmpty());
            assertFalse(comm.listNodes(service).isEmpty());
        }
    }


    @Test
    void testServiceCommFailure() {
        setupNetworkError("/apis/v1/applications");
        assertThrows(DroveCommunicationException.class,
                     () -> {
                         try (val dc = RangerDroveUtils.buildDroveClient(
                                 "testns", config(), MAPPER)) {
                             fail("Should not have come here");
                         }
                     });
    }

    @Test
    void testBulkLoadCommFailure() {
        val config = config();
        setupAppsResponse();
        setupNetworkError("/apis/v1/endpoints");
        assertThrows(DroveCommunicationException.class,
                     () -> {
                         try (val dc = RangerDroveUtils.buildDroveClient(
                                 "testns", config, MAPPER)) {
                             fail("Should not have come here");
                         }
                     });
    }


    private static boolean isInScenario() {
        return extension.getAllScenarios()
                .getScenarios()
                .stream()
                .anyMatch(scenario -> scenario.getName().equals("EventDrivenLoading"));
    }

    private static String toJson(final Object object) throws JsonProcessingException {
        return MAPPER.writeValueAsString(object);
    }

    private static DroveUpstreamConfig config() {
        log.info("WM Port: {}", extension.getPort());
        return DroveUpstreamConfig.builder()
                .endpoints(List.of("http://localhost:" + extension.getPort()))
                .username("guest")
                .password("guest")
                .build();
    }


    @SneakyThrows
    private static void setupEndpointsResponse() {
        extension.stubFor(get(urlPathEqualTo("/apis/v1/endpoints"))
                                  .willReturn(aResponse()
                                                      .withBody(toJson(
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
    }

    @SneakyThrows
    private static void setupAppsResponse() {
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
        extension.stubFor(get("/apis/v1/applications")
                                  .withBasicAuth("guest", "guest")
                                  .willReturn(okJson(MAPPER.writeValueAsString(
                                          response))));
    }

    private static void setupNetworkError(String api) {
        extension.stubFor(get(urlPathEqualTo(api))
                                  .withBasicAuth("guest", "guest")
                                  .willReturn(aResponse().withFault(Fault.MALFORMED_RESPONSE_CHUNK)));
    }
}