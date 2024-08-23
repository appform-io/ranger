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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import com.phonepe.drove.models.api.ApiResponse;
import com.phonepe.drove.models.api.ExposedAppInfo;
import com.phonepe.drove.models.application.PortType;
import io.appform.ranger.drove.config.DroveUpstreamConfig;
import io.appform.ranger.drove.serde.DroveResponseDataDeserializer;
import io.appform.ranger.drove.utils.RangerDroveUtils;
import lombok.Value;
import lombok.val;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.awaitility.Awaitility.await;

/**
 *
 */
@WireMockTest
class DroveShardedServiceFinderBuilderTest {

    @Value
    private static class NodeData {
        String name;

        public NodeData(@JsonProperty("name") String name) {
            this.name = name;
        }
    }

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void testFinder(WireMockRuntimeInfo wm) throws Exception {
        val payload = MAPPER.writeValueAsBytes(
                ApiResponse.success(List.of(new ExposedAppInfo("test",
                                                               "test-0.1",
                                                               "test.appform.io",
                                                               Map.of(),
                                                               List.of(new ExposedAppInfo.ExposedHost(
                                                                       "executor001.internal",
                                                                       32456,
                                                                       PortType.HTTP))))));
        stubFor(get(urlPathEqualTo("/apis/v1/endpoints"))
                        .willReturn(aResponse()
                                            .withBody(payload)
                                            .withStatus(200)));
        val clientConfig = DroveUpstreamConfig.builder()
                .endpoints(List.of("http://localhost:" + wm.getHttpPort()))
                .skipCaching(true)
                .build();

        try(val droveClient = RangerDroveUtils.buildDroveClient("testns", clientConfig, MAPPER)) {
            val finder = new DroveShardedServiceFinderBuilder<NodeData>()
                    .withClientConfig(clientConfig)
                    .withNamespace("testns")
                    .withServiceName("test")
                    .withObjectMapper(MAPPER)
                    .withDeserializer(new DroveResponseDataDeserializer<>() {
                        @Override
                        public NodeData translate(ExposedAppInfo appInfo, ExposedAppInfo.ExposedHost host) {
                            return new NodeData(appInfo.getAppId());
                        }
                    })
                    .withShardSelector((criteria, registry) -> registry.nodeList())
                    .withNodeRefreshIntervalMs(1000)
                    .withDroveClient(droveClient)
                    .build();
            finder.start();
            await().atMost(Duration.ofSeconds(30))
                    .pollInterval(Duration.ofSeconds(1))
                    .until(() -> finder.getServiceRegistry().isRefreshed());
            Assertions.assertNotNull(finder.get(nodeData -> true).orElse(null));
            finder.stop();
        }
    }

}