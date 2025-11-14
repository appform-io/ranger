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

package io.appform.ranger.hub.server.bundle;

import com.codahale.metrics.health.HealthCheckRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import io.appform.ranger.client.RangerHubClient;
import io.appform.ranger.common.server.ShardInfo;
import io.appform.ranger.core.healthcheck.HealthcheckStatus;
import io.appform.ranger.core.model.Service;
import io.appform.ranger.core.model.ServiceNode;
import io.appform.ranger.http.config.HttpClientConfig;
import io.appform.ranger.http.model.ServiceDataSourceResponse;
import io.appform.ranger.http.model.ServiceNodesResponse;
import io.appform.ranger.hub.server.bundle.configuration.RangerHttpUpstreamConfiguration;
import io.appform.ranger.hub.server.bundle.configuration.RangerServerConfiguration;
import io.dropwizard.Configuration;
import io.dropwizard.jersey.DropwizardResourceConfig;
import io.dropwizard.jersey.setup.JerseyEnvironment;
import io.dropwizard.lifecycle.setup.LifecycleEnvironment;
import io.dropwizard.setup.AdminEnvironment;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *
 */
@WireMockTest
class RangerHubServerBundleTest {

    @Test
    @SneakyThrows
    void test(final WireMockRuntimeInfo wm) {
        @Getter
        class TestConfig extends Configuration {
            private final RangerServerConfiguration upstreams = RangerServerConfiguration.builder()
                    .namespace("test")
                    .upstreams(List.of(
                            new RangerHttpUpstreamConfiguration()
                                    .setHttpClientConfigs(List.of(HttpClientConfig.builder()
                                            .host("localhost")
                                            .port(wm.getHttpPort())
                                            .build()))
                            )
                    )
                    .build();

        }
        final TestConfig testConfig = new TestConfig();
        final HealthCheckRegistry healthChecks = mock(HealthCheckRegistry.class);
        final JerseyEnvironment jerseyEnvironment = mock(JerseyEnvironment.class);
        final LifecycleEnvironment lifecycleEnvironment = mock(LifecycleEnvironment.class);
        final Environment environment = mock(Environment.class);
        final AdminEnvironment adminEnvironment = mock(AdminEnvironment.class);
        final Bootstrap<?> bootstrap = mock(Bootstrap.class);
        final ObjectMapper mapper = new ObjectMapper();

        when(jerseyEnvironment.getResourceConfig()).thenReturn(new DropwizardResourceConfig());
        when(environment.jersey()).thenReturn(jerseyEnvironment);
        when(environment.lifecycle()).thenReturn(lifecycleEnvironment);
        when(environment.healthChecks()).thenReturn(healthChecks);
        when(environment.admin()).thenReturn(adminEnvironment);
        when(environment.getObjectMapper()).thenReturn(mapper);
        when(bootstrap.getHealthCheckRegistry()).thenReturn(mock(HealthCheckRegistry.class));

        val bundle = new RangerHubServerBundle<TestConfig>() {

            @Override
            protected RangerServerConfiguration getRangerConfiguration(TestConfig configuration) {
                return testConfig.getUpstreams();
            }
        };
        val services = IntStream.rangeClosed(1, 10)
                .mapToObj(i -> Service.builder()
                        .namespace("test")
                        .serviceName("service-" + i)
                        .build())
                .collect(Collectors.toUnmodifiableSet());
        stubFor(get(urlPathEqualTo("/ranger/services/v1"))
                .willReturn(okJson(environment.getObjectMapper()
                        .writeValueAsString(ServiceDataSourceResponse.builder()
                                .data(services)
                                .build()))));
        stubFor(any(urlPathMatching("/ranger/nodes/v1/test/service-[0-9]+"))
                .willReturn(okJson(mapper.writeValueAsString(
                        ServiceNodesResponse.builder()
                                .data(IntStream.rangeClosed(1, 5)
                                        .mapToObj(i -> ServiceNode.builder()
                                                .host("host-" + i)
                                                .port(5000)
                                                .nodeData(ShardInfo.builder()
                                                        .environment("blah")
                                                        .region("reg")
                                                        .build())
                                                .healthcheckStatus(HealthcheckStatus.healthy)
                                                .lastUpdatedTimeStamp(System.currentTimeMillis())
                                                .build())
                                        .toList())
                                .build()))));

        bundle.initialize(bootstrap);
        bundle.run(testConfig, environment);
        bundle.getHubs().forEach(RangerHubClient::start);
        IntStream.rangeClosed(1, 10)
                .forEach(i -> {
                    val nodes = bundle.getHubs()
                            .stream()
                            .flatMap(hub -> hub.getAllNodes(Service.builder().namespace("test").serviceName("service-" + i).build()).stream())
                            .toList();
                    Assertions.assertEquals(5, nodes.size());
                });

    }

}