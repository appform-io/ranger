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

package io.appform.ranger.zookeeper.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import io.appform.ranger.core.healthcheck.Healthcheck;
import io.appform.ranger.core.healthcheck.HealthcheckStatus;
import io.appform.ranger.core.healthcheck.updater.HealthStatusHandler;
import io.appform.ranger.core.healthcheck.updater.HealthUpdateHandler;
import io.appform.ranger.core.healthcheck.updater.LastUpdatedHandler;
import io.appform.ranger.core.model.ServiceNode;
import io.appform.ranger.core.units.TestNodeData;
import io.appform.ranger.core.utils.RangerTestUtils;
import io.appform.ranger.zookeeper.ServiceFinderBuilders;
import io.appform.ranger.zookeeper.ServiceProviderBuilders;
import lombok.Getter;
import lombok.val;
import org.apache.curator.test.TestingCluster;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

class ServiceProviderHealthcheckTest {

    private TestingCluster testingCluster;
    private ObjectMapper objectMapper;
    private final Map<String, TestServiceProvider> serviceProviders = Maps.newHashMap();

    @BeforeEach
    public void startTestCluster() throws Exception {
        objectMapper = new ObjectMapper();
        testingCluster = new TestingCluster(3);
        testingCluster.start();
        registerService("localhost-1", 9000, 1);
        registerService("localhost-3", 9001, 2);
    }

    @AfterEach
    public void stopTestCluster() throws Exception {
        if (null != testingCluster) {
            testingCluster.close();
        }
    }

    @Test
    void testBasicDiscovery() {
        val serviceFinder = ServiceFinderBuilders.<TestNodeData>shardedFinderBuilder()
                .withConnectionString(testingCluster.getConnectString())
                .withNamespace("test")
                .withServiceName("test-service")
                .withDeserializer(data -> {
                    try {
                        return objectMapper.readValue(data,
                                new TypeReference<ServiceNode<TestNodeData>>() {
                                });
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    return null;
                })
                .withNodeRefreshIntervalMs(1000)
                .build();
        serviceFinder.start();
        val node = serviceFinder.get(RangerTestUtils.getCriteria(1)).orElse(null);
        Assertions.assertNotNull(node);
        Assertions.assertEquals("localhost-1", node.getHost());
        TestServiceProvider testServiceProvider = serviceProviders.get(node.getHost());
        testServiceProvider.oor();
        RangerTestUtils.sleepUntil(2); //Sleep till the increment refresh healthCheck interval (> 1sec), no upper bound condition.
        Assertions.assertFalse(serviceFinder.get(RangerTestUtils.getCriteria(1)).isPresent());
        serviceFinder.stop();
    }

    private static final class CustomHealthcheck implements Healthcheck {
        private HealthcheckStatus status = HealthcheckStatus.healthy;

        public void setStatus(HealthcheckStatus status) {
            this.status = status;
        }

        @Override
        public HealthcheckStatus check() {
            return status;
        }

    }

    private static final class TestServiceProvider {
        private final CustomHealthcheck healthcheck = new CustomHealthcheck();
        private final ObjectMapper objectMapper;
        private final String connectionString;
        private final String host;
        private final int port;
        private final int shardId;
        @Getter
        private boolean started = false;

        public TestServiceProvider(ObjectMapper objectMapper,
                                   String connectionString,
                                   String host,
                                   int port,
                                   int shardId) {
            this.objectMapper = objectMapper;
            this.connectionString = connectionString;
            this.host = host;
            this.port = port;
            this.shardId = shardId;
        }

        @SuppressWarnings("unused")
        public void bir() {
            healthcheck.setStatus(HealthcheckStatus.healthy);
        }

        public void oor() {
            healthcheck.setStatus(HealthcheckStatus.unhealthy);
        }

        public void start() {
            final HealthUpdateHandler<TestNodeData> healthUpdateHandler = new LastUpdatedHandler<TestNodeData>()
                    .setNext(new HealthStatusHandler<TestNodeData>());
            val serviceProvider = ServiceProviderBuilders.<TestNodeData>shardedServiceProviderBuilder()
                    .withConnectionString(connectionString)
                    .withNamespace("test")
                    .withServiceName("test-service")
                    .withSerializer(data -> {
                        try {
                            return objectMapper.writeValueAsBytes(data);
                        } catch (JsonProcessingException e) {
                            e.printStackTrace();
                        }
                        return null;
                    })
                    .withHostname(host)
                    .withPort(port)
                    .withNodeData(TestNodeData.builder().shardId(shardId).build())
                    .withHealthcheck(healthcheck)
                    .withHealthUpdateIntervalMs(1000)
                    .healthUpdateHandler(healthUpdateHandler)
                    .build();
            serviceProvider.start();
            started = true;
        }
    }

    private void registerService(String host, int port, int shardId) throws Exception {
        val serviceProvider = new TestServiceProvider(objectMapper, testingCluster.getConnectString(), host, port, shardId);
        serviceProvider.start();
        serviceProviders.put(host, serviceProvider);
    }

}