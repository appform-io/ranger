/*
 * Copyright 2015 Flipkart Internet Pvt. Ltd.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.appform.ranger.client.drove;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import com.phonepe.drove.models.api.ApiResponse;
import com.phonepe.drove.models.api.AppSummary;
import com.phonepe.drove.models.api.ExposedAppInfo;
import com.phonepe.drove.models.application.ApplicationState;
import com.phonepe.drove.models.application.PortType;
import io.appform.ranger.drove.config.DroveUpstreamConfig;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Date;
import java.util.List;
import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;

@Slf4j
@Getter
public abstract class BaseRangerDroveClientTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private DroveUpstreamConfig clientConfig;

    @RegisterExtension
    static WireMockExtension wireMockExtension = WireMockExtension.newInstance()
            .options(wireMockConfig().dynamicPort().dynamicHttpsPort())
            .build();

    @BeforeEach
    public void prepareHttpMocks() throws Exception {
        wireMockExtension.stubFor(get(urlEqualTo("/apis/v1/endpoints/app/TEST_APP"))
                                          .willReturn(aResponse()
                                                              .withBody(objectMapper.writeValueAsBytes(
                                                                      ApiResponse.success(List.of(new ExposedAppInfo(
                                                                              "test",
                                                                              "test-0.1",
                                                                              "test.appform.io",
                                                                              Map.of(),
                                                                              List.of(new ExposedAppInfo.ExposedHost(
                                                                                      "executor001.internal",
                                                                                      32456,
                                                                                      PortType.HTTP)))))))
                                                              .withStatus(200)));
        wireMockExtension.stubFor(get(urlEqualTo("/apis/v1/endpoints/app/OTHER_APP"))
                                          .willReturn(aResponse()
                                                              .withBody(objectMapper.writeValueAsBytes(
                                                                      ApiResponse.success(List.of())))
                                                              .withStatus(200)));

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
        wireMockExtension.stubFor(get("/apis/v1/applications").willReturn(okJson(objectMapper.writeValueAsString(
                response))));

        clientConfig = DroveUpstreamConfig.builder()
                .endpoints(List.of("http://localhost:" + wireMockExtension.getPort()))
                .build();
        log.debug("Started http subsystem");
    }
}
