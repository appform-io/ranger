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
import com.google.common.collect.Lists;
import io.appform.ranger.core.finder.nodeselector.RoundRobinServiceNodeSelector;
import io.appform.ranger.core.healthcheck.Healthchecks;
import io.appform.ranger.core.model.ServiceNode;
import io.appform.ranger.core.serviceprovider.ServiceProvider;
import io.appform.ranger.core.units.TestNodeData;
import io.appform.ranger.core.utils.RangerTestUtils;
import io.appform.ranger.zookeeper.ServiceFinderBuilders;
import io.appform.ranger.zookeeper.ServiceProviderBuilders;
import io.appform.ranger.zookeeper.serde.ZkNodeDataSerializer;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.curator.test.TestingCluster;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.stream.LongStream;

@Slf4j
class ServiceProviderTest {

    private TestingCluster testingCluster;
    private ObjectMapper objectMapper;
    private final List<ServiceProvider<TestNodeData, ZkNodeDataSerializer<TestNodeData>>> serviceProviders = Lists.newArrayList();

    @BeforeEach
    void startTestCluster() throws Exception {
        objectMapper = new ObjectMapper();
        testingCluster = new TestingCluster(3);
        testingCluster.start();
        registerService("localhost-1", 9000, 1);
        registerService("localhost-2", 9001, 1);
        registerService("localhost-3", 9002, 1);
        registerService("localhost-4", 9003, 2);
    }

    @AfterEach
    void stopTestCluster() throws Exception {
        serviceProviders.forEach(ServiceProvider::stop);
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
                        return objectMapper.readValue(data, new TypeReference<ServiceNode<TestNodeData>>() {});
                    }
                    catch (IOException e) {
                        e.printStackTrace();
                    }
                    return null;
                })
                .build();
        serviceFinder.start();
        {
            val node = serviceFinder.get(RangerTestUtils.getCriteria(1)).orElse(null);
            Assertions.assertNotNull(node);
            Assertions.assertEquals(1, node.getNodeData().getShardId());
        }
        {
            val node = serviceFinder.get(RangerTestUtils.getCriteria(1)).orElse(null);
            Assertions.assertNotNull(node);
            Assertions.assertEquals(1, node.getNodeData().getShardId());
        }
        val startTime = System.currentTimeMillis();
        LongStream.range(0, 1000000).mapToObj(i -> serviceFinder.get(RangerTestUtils.getCriteria(2)).orElse(null)).forEach(node -> {
            Assertions.assertNotNull(node);
            Assertions.assertEquals(2, node.getNodeData().getShardId());
        });
        log.info("PERF::RandomSelector::Took (ms):" + (System.currentTimeMillis() - startTime));
        {
            val node = serviceFinder.get(RangerTestUtils.getCriteria(99)).orElse(null);
            Assertions.assertNull(node);
        }
        serviceFinder.stop();
    }

    @Test
    void testBasicDiscoveryRR() {
        val serviceFinder
                = ServiceFinderBuilders.<TestNodeData>shardedFinderBuilder()
                .withConnectionString(testingCluster.getConnectString())
                .withNamespace("test")
                .withServiceName("test-service")
                .withNodeSelector(new RoundRobinServiceNodeSelector<>())
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
                .build();
        serviceFinder.start();
        {
            val node = serviceFinder.get(RangerTestUtils.getCriteria(1));
            Assertions.assertTrue(node.isPresent());
            Assertions.assertEquals(1, node.get().getNodeData().getShardId());
        }
        {
            val node = serviceFinder.get(RangerTestUtils.getCriteria(1));
            Assertions.assertTrue(node.isPresent());
            Assertions.assertEquals(1, node.get().getNodeData().getShardId());
        }
        long startTime = System.currentTimeMillis();
        LongStream.range(0, 1000000).mapToObj(i -> serviceFinder.get(RangerTestUtils.getCriteria(2))).forEach(node -> {
            Assertions.assertTrue(node.isPresent());
            Assertions.assertEquals(2, node.get().getNodeData().getShardId());
        });
        log.info("PERF::RoundRobinSelector::Took (ms):" + (System.currentTimeMillis() - startTime));
        {
            val node = serviceFinder.get(RangerTestUtils.getCriteria(99));
            Assertions.assertFalse(node.isPresent());
        }
        serviceFinder.stop();
    }

    @Test
    void testVisibility() {
        val serviceFinder = ServiceFinderBuilders.
                <TestNodeData>shardedFinderBuilder()
                .withConnectionString(testingCluster.getConnectString())
                .withNamespace("test")
                .withServiceName("test-service")
                .withNodeSelector(new RoundRobinServiceNodeSelector<>())
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
                .build();
        serviceFinder.start();
        val all = serviceFinder.getAll(RangerTestUtils.getCriteria(1));
        log.info("Testing ServiceFinder.getAll()");
        all.stream().map(serviceNode -> "node = " + serviceNode.getHost() + ":" + serviceNode.getPort() + "  " + serviceNode.getHealthcheckStatus() + " " + serviceNode
                .getLastUpdatedTimeStamp()).forEach(log::info);
        Assertions.assertEquals(3, all.size());
        serviceFinder.stop();
    }

    private void registerService(String host, int port, int shardId) {
        val serviceProvider = ServiceProviderBuilders.<TestNodeData>shardedServiceProviderBuilder()
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
                .withNodeData(TestNodeData.builder().shardId(shardId).build())
                .withHealthcheck(Healthchecks.defaultHealthyCheck())
                .withHealthUpdateIntervalMs(15000)
                .build();
        serviceProvider.start();
        serviceProviders.add(serviceProvider);
    }
}