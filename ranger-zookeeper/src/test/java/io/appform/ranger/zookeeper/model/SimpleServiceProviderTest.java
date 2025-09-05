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
import com.google.common.collect.HashMultiset;
import io.appform.ranger.core.healthcheck.Healthchecks;
import io.appform.ranger.core.healthcheck.updater.HealthStatusHandler;
import io.appform.ranger.core.healthcheck.updater.HealthUpdateHandler;
import io.appform.ranger.core.healthcheck.updater.LastUpdatedHandler;
import io.appform.ranger.core.model.ServiceNode;
import io.appform.ranger.zookeeper.ServiceFinderBuilders;
import io.appform.ranger.zookeeper.ServiceProviderBuilders;
import lombok.val;
import org.apache.curator.test.TestingCluster;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.stream.LongStream;

class SimpleServiceProviderTest {

    private TestingCluster testingCluster;
    private ObjectMapper objectMapper;

    @BeforeEach
    public void startTestCluster() throws Exception {
        objectMapper = new ObjectMapper();
        testingCluster = new TestingCluster(3);
        testingCluster.start();
        registerService("localhost-1", 9000);
        registerService("localhost-2", 9001 );
        registerService("localhost-3", 9002);
    }

    @AfterEach
    public void stopTestCluster() throws Exception {
        if(null != testingCluster) {
            testingCluster.close();
        }
    }

    private static class UnshardedInfo {

        @Override
        public int hashCode() {
            return 0;
        }

        @Override
        public boolean equals(Object obj) {
            return super.equals(obj);
        }
    }

    @Test
    void testBasicDiscovery() {
        val serviceFinder = ServiceFinderBuilders.<UnshardedInfo>unshardedFinderBuilder()
                .withConnectionString(testingCluster.getConnectString())
                .withNamespace("test")
                .withServiceName("test-service")
                .withDisableWatchers()
                .withDeserializer(data -> {
                    try {
                        return objectMapper.readValue(data,
                                new TypeReference<ServiceNode<UnshardedInfo>>() {
                                });
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    return null;
                })
                .build();
        serviceFinder.start();
        {
            val node = serviceFinder.get(null).orElse(null);
            Assertions.assertNotNull(node);
            System.out.println(node.getHost());
        }
        val frequency = HashMultiset.create();
        val startTime = System.currentTimeMillis();
        LongStream.range(0, 1000000).mapToObj(i -> serviceFinder.get(null).orElse(null)).forEach(node -> {
            Assertions.assertNotNull(node);
            frequency.add(node.getHost());
        });
        System.out.println("1 Million lookups and freq counting took (ms):" + (System.currentTimeMillis() -startTime));
        System.out.println("Frequency: " + frequency);
    }

    private void registerService(String host, int port) {
        final HealthUpdateHandler<UnshardedInfo> healthUpdateHandler = new LastUpdatedHandler<UnshardedInfo>()
                .setNext(new HealthStatusHandler<UnshardedInfo>());
        val serviceProvider = ServiceProviderBuilders.<UnshardedInfo>unshardedServiceProviderBuilder()
                .withConnectionString(testingCluster.getConnectString())
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
                .withHealthcheck(Healthchecks.defaultHealthyCheck())
                .withHealthUpdateIntervalMs(1000)
                .healthUpdateHandler(healthUpdateHandler)
                .build();
        serviceProvider.start();
    }
}