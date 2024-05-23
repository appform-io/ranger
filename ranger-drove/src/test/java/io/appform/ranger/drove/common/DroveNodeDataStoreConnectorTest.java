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
package io.appform.ranger.drove.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import com.phonepe.drove.client.DroveClientConfig;
import io.appform.ranger.core.units.TestNodeData;
import io.appform.ranger.drove.config.DroveConfig;
import lombok.val;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.junit.jupiter.api.Assertions.assertTrue;

@WireMockTest
class DroveNodeDataStoreConnectorTest {
    
    @Test
    void testDroveNodeDataStoreConnector(WireMockRuntimeInfo wm){
        stubFor(get("/apis/v1/ping").willReturn(ok()));

        val clientConfig = DroveConfig.builder()
                .cluster(new DroveClientConfig(List.of("http://localhost:" + wm.getHttpPort()),
                                                   Duration.ofSeconds(1), Duration.ofSeconds(1), Duration.ofSeconds(1)))
                .build();
        val mapper = new ObjectMapper();
        val connector = new DroveNodeDataStoreConnector<TestNodeData>(clientConfig, mapper);
        Awaitility.await()
                .until(connector::isActive);
        assertTrue(connector.isActive());
    }
}
