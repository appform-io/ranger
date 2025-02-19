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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import com.google.common.collect.Sets;
import io.appform.ranger.core.utils.RangerTestUtils;
import io.appform.ranger.http.config.HttpClientConfig;
import io.appform.ranger.http.model.ServiceDataSourceResponse;
import io.appform.ranger.http.utils.RangerHttpUtils;
import lombok.val;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static com.github.tomakehurst.wiremock.client.WireMock.*;

@WireMockTest
class HttpServiceDataSourceTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void testServiceDataSource(WireMockRuntimeInfo wireMockRuntimeInfo) throws IOException {
        val responseObjReplicated = ServiceDataSourceResponse.builder()
                .data(Sets.newHashSet(
                        RangerTestUtils.getService("test-n", "test-s"),
                        RangerTestUtils.getService("test-n", "test-s1"),
                        RangerTestUtils.getService("test-n", "test-s2")
                ))
                .build();
        stubFor(get(urlEqualTo("/ranger/services/v1?skipDataFromReplicationSources=false"))
                .willReturn(aResponse()
                        .withBody(MAPPER.writeValueAsBytes(responseObjReplicated))
                        .withStatus(200)));
        val responseObjReplicationSkipped = ServiceDataSourceResponse.builder()
                .data(Sets.newHashSet(
                        RangerTestUtils.getService("test-n", "test-s"),
                        RangerTestUtils.getService("test-n", "test-s1")))
                .build();
        stubFor(get(urlEqualTo("/ranger/services/v1?skipDataFromReplicationSources=true"))
                .willReturn(aResponse()
                        .withBody(MAPPER.writeValueAsBytes(responseObjReplicationSkipped))
                        .withStatus(200)));
        {
            val clientConfig = HttpClientConfig.builder()
                    .host("127.0.0.1")
                    .port(wireMockRuntimeInfo.getHttpPort())
                    .connectionTimeoutMs(30_000)
                    .operationTimeoutMs(30_000)
                    .build();
            val httpServiceDataSource = new HttpServiceDataSource<>(clientConfig,
                                                                    RangerHttpUtils.httpClient(clientConfig, MAPPER));
            val services = httpServiceDataSource.services();
            Assertions.assertNotNull(services);
            Assertions.assertFalse(services.isEmpty());
            Assertions.assertEquals(3, services.size());
            Assertions.assertFalse(services.stream()
                                           .noneMatch(each -> each.getServiceName().equalsIgnoreCase("test-s")));
            Assertions.assertFalse(services.stream()
                                           .noneMatch(each -> each.getServiceName().equalsIgnoreCase("test-s1")));
            Assertions.assertFalse(services.stream()
                                           .noneMatch(each -> each.getServiceName().equalsIgnoreCase("test-s2")));
        }
        { //Here we set skip replication data to false. so query param is set
            val clientConfig = HttpClientConfig.builder()
                    .host("127.0.0.1")
                    .port(wireMockRuntimeInfo.getHttpPort())
                    .connectionTimeoutMs(30_000)
                    .operationTimeoutMs(30_000)
                    .replicationSource(true)
                    .build();
            val httpServiceDataSource = new HttpServiceDataSource<>(clientConfig,
                                                                    RangerHttpUtils.httpClient(clientConfig, MAPPER));
            val services = httpServiceDataSource.services();
            Assertions.assertNotNull(services);
            Assertions.assertFalse(services.isEmpty());
            Assertions.assertEquals(2, services.size());
            Assertions.assertFalse(services.stream()
                                           .noneMatch(each -> each.getServiceName().equalsIgnoreCase("test-s")));
            Assertions.assertFalse(services.stream()
                                           .noneMatch(each -> each.getServiceName().equalsIgnoreCase("test-s1")));
        }
    }

}
