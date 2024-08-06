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
package io.appform.ranger.drove.serviceprovider;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import lombok.Builder;
import lombok.Data;
import org.junit.jupiter.api.Test;

@WireMockTest
class DroveShardedServiceProviderBuilderTest {

    @Data
    private static final class TestNodeData {
        private final String farmId;

        @Builder
        public TestNodeData(@JsonProperty("farmId") String farmId) {
            this.farmId = farmId;
        }
    }

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void testProvider(WireMockRuntimeInfo wireMockRuntimeInfo) throws Exception {
        /*val farmNodeData = TestNodeData.builder().farmId("farm1").build();
        val testNode = ServiceNode.<TestNodeData>builder().host("127.0.0.1").port(80).nodeData(farmNodeData).build();
        Assertions.assertEquals(PortSchemes.HTTP, testNode.getPortScheme());
        val response = MAPPER.writeValueAsBytes(
                GenericResponse.builder()
                        .data(ServiceNode.<TestNodeData>builder()
                                .host("localhost")
                                .port(8080)
                                .build()
                        )
                        .build());
        byte[] requestBytes = MAPPER.writeValueAsBytes(testNode);
        stubFor(post(urlEqualTo("/ranger/nodes/v1/add/testns/test"))
                .withRequestBody(binaryEqualTo(requestBytes))
                .willReturn(aResponse()
                        .withBody(response)
                        .withStatus(200)));
        val clientConfig = DroveUpstreamConfig.builder()
                .host("127.0.0.1")
                .port(wireMockRuntimeInfo.getHttpPort())
                .connectionTimeoutMs(30_000)
                .operationTimeoutMs(30_000)
                .build();
        val serviceProvider = new DroveShardedServiceProviderBuilder<TestNodeData>()
                .withNamespace("testns")
                .withServiceName("test")
                .withHostname("localhost-1")
                .withPort(80)
                .withHealthcheck(Healthchecks.defaultHealthyCheck())
                .withHealthUpdateIntervalMs(1000)
                .withObjectMapper(MAPPER)
                .withClientConfiguration(clientConfig)
                .withNodeData(farmNodeData)
                .withSerializer(node -> requestBytes)
                .build();
        serviceProvider.start();
        Assertions.assertNotNull(serviceProvider);*/
    }

}
