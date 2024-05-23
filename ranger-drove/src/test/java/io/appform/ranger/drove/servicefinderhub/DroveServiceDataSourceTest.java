/*
 * Copyright 2015 Flipkart Internet Pvt. Ltd.
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import com.phonepe.drove.client.DroveClientConfig;
import com.phonepe.drove.models.api.ApiResponse;
import com.phonepe.drove.models.api.AppSummary;
import com.phonepe.drove.models.application.ApplicationState;
import io.appform.ranger.core.model.Service;
import io.appform.ranger.core.units.TestNodeData;
import io.appform.ranger.drove.config.DroveConfig;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.junit.jupiter.api.Assertions.*;

@WireMockTest
class DroveServiceDataSourceTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    @SneakyThrows
    void testServiceDataSource(WireMockRuntimeInfo wireMockRuntimeInfo) {
        val response = ApiResponse.success(Map.of(
                "TEST_APP-1",
                new AppSummary("TEST_APP-1",
                               "TEST_APP",
                               4,
                               4,
                               4,
                               1024,
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
                               ApplicationState.RUNNING,
                               new Date(),
                               new Date())));
        val payload = MAPPER.writeValueAsString(response);
        stubFor(get("/apis/v1/applications").willReturn(okJson(payload)));
        val clientConfig = DroveConfig.builder()
                .cluster(new DroveClientConfig(List.of("http://localhost:" + wireMockRuntimeInfo.getHttpPort()),
                                               Duration.ofSeconds(1), Duration.ofSeconds(1), Duration.ofSeconds(1)))
                .build();
        val finder = new DroveServiceDataSource<TestNodeData>(clientConfig, MAPPER, "test");
        finder.start();
        val services = finder.services();
        assertFalse(services.isEmpty());
        assertEquals(2, services.size());
        assertTrue(services.contains(new Service("test", "TEST_APP")));
        assertTrue(services.contains(new Service("test", "OTHER_APP")));
    }

}
