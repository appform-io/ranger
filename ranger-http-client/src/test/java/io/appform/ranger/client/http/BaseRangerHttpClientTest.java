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
package io.appform.ranger.client.http;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.appform.ranger.core.healthcheck.HealthcheckStatus;
import io.appform.ranger.core.model.ServiceNode;
import io.appform.ranger.core.units.TestNodeData;
import io.appform.ranger.core.util.Exceptions;
import io.appform.ranger.core.utils.RangerTestUtils;
import io.appform.ranger.http.config.HttpClientConfig;
import io.appform.ranger.http.model.ServiceDataSourceResponse;
import io.appform.ranger.http.model.ServiceNodesResponse;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;

@Slf4j
@Getter
public abstract class BaseRangerHttpClientTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private HttpClientConfig httpClientConfig;

    @RegisterExtension
    static WireMockExtension wireMockExtension = WireMockExtension.newInstance()
            .options(wireMockConfig().dynamicPort().dynamicHttpsPort())
            .build();

    @BeforeEach
    public void prepareHttpMocks() throws Exception {
        val testNode = TestNodeData.builder().shardId(1).build();
        val node = ServiceNode.<TestNodeData>builder().host("127.0.0.1").port(80).nodeData(testNode).build();
        node.setHealthcheckStatus(HealthcheckStatus.healthy);
        node.setLastUpdatedTimeStamp(System.currentTimeMillis());
        val payload = objectMapper.writeValueAsBytes(
                ServiceNodesResponse.<TestNodeData>builder()
                        .data(Lists.newArrayList(node))
                        .build());
        wireMockExtension.stubFor(get(urlPathEqualTo("/ranger/nodes/v1/test-n/test-s"))
                .willReturn(aResponse()
                        .withBody(payload)
                        .withStatus(200)));

        val responseObj = ServiceDataSourceResponse.builder()
                .data(Sets.newHashSet(
                        RangerTestUtils.getService("test-n", "test-s")
                ))
                .build();
        val response = objectMapper.writeValueAsBytes(responseObj);
        wireMockExtension.stubFor(get(urlPathEqualTo("/ranger/services/v1"))
                .willReturn(aResponse()
                        .withBody(response)
                        .withStatus(200)));

        httpClientConfig = HttpClientConfig.builder()
                .host("127.0.0.1")
                .port(wireMockExtension.getPort())
                .connectionTimeoutMs(30_000)
                .operationTimeoutMs(30_000)
                .build();
        log.debug("Started http subsystem");
    }

    protected ServiceNodesResponse<TestNodeData> read(final byte[] data) {
        try {
            return getObjectMapper().readValue(data, new TypeReference<ServiceNodesResponse<TestNodeData>>() {});
        }
        catch (IOException e) {
            Exceptions.illegalState(e);
        }
        return null;
    }
}
