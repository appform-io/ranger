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

package io.appform.ranger.core.finderhub;


import com.google.common.collect.Lists;
import io.appform.ranger.core.exceptions.CommunicationException;
import io.appform.ranger.core.finder.BaseServiceFinderBuilder;
import io.appform.ranger.core.finder.ServiceFinder;
import io.appform.ranger.core.finder.SimpleShardedServiceFinder;
import io.appform.ranger.core.finder.nodeselector.WarmupRoundRobinServiceNodeSelector;
import io.appform.ranger.core.finder.nodeselector.WeightedRandomServiceNodeSelector;
import io.appform.ranger.core.finder.serviceregistry.MapBasedServiceRegistry;
import io.appform.ranger.core.finder.shardselector.MatchingShardSelector;
import io.appform.ranger.core.healthcheck.HealthcheckStatus;
import io.appform.ranger.core.model.*;
import io.appform.ranger.core.units.TestNodeData;
import io.appform.ranger.core.utils.RangerTestUtils;
import lombok.val;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

class ServiceFinderHubTest {

    private final ServiceFinderHub<TestNodeData, MapBasedServiceRegistry<TestNodeData>> serviceFinderHub = new ServiceFinderHub<>(
            new DynamicDataSource(Lists.newArrayList(new Service("NS", "PRE_REGISTERED_SERVICE"))),
            service ->
                    new TestServiceFinderBuilder()
                            .withNamespace(service.getNamespace())
                            .withServiceName(service.getServiceName())
                            .withDeserializer(new Deserializer<TestNodeData>() {
                            })
                            .build());

    @Test
    void testDynamicServiceAddition() {
        serviceFinderHub.start();
        val preRegisteredServiceFinder = serviceFinderHub.finder(new Service("NS", "PRE_REGISTERED_SERVICE"))
                .orElseThrow(() -> new IllegalStateException("Finder should be present"));
        val node = preRegisteredServiceFinder.get(null, (criteria, serviceRegistry) -> serviceRegistry.nodeList());
        Assertions.assertTrue(node.isPresent());
        Assertions.assertEquals("HOST", node.get().getHost());
        Assertions.assertEquals(0, node.get().getPort());

        val dynamicServiceFinder = serviceFinderHub.buildFinder(new Service("NS", "SERVICE")).join();
        val dynamicServiceNode = dynamicServiceFinder.get(null, (criteria, serviceRegistry) -> serviceRegistry.nodeList());
        Assertions.assertTrue(dynamicServiceNode.isPresent());
        Assertions.assertEquals("HOST", dynamicServiceNode.get().getHost());
        Assertions.assertEquals(0, dynamicServiceNode.get().getPort());
    }

    @Test
    void testTimeoutOnHubStartup() {
        var testServiceFinderHub = new TestServiceFinderHubBuilder()
                .withServiceDataSource(new DynamicDataSource(Lists.newArrayList(new Service("NS", "SERVICE"))))
                .withServiceFinderFactory(new TestServiceFinderFactory())
                .withRefreshFrequencyMs(5_000)
                .withHubStartTimeout(1_000)
                .withServiceRefreshTimeout(10_000)
                .build();

        try {
            Exception exception = Assertions.assertThrows(IllegalStateException.class, testServiceFinderHub::start);
            Assertions.assertTrue(exception.getMessage()
                    .contains("Couldn't perform service hub refresh at this time. Refresh exceeded the start up time specified"));
        } finally {
            testServiceFinderHub.stop();
        }
    }

    @Test
    void testDelayedServiceAddition() {
        val delayedHub = new ServiceFinderHub<>(new DynamicDataSource(Lists.newArrayList(new Service("NS", "SERVICE"))),
                service ->  new TestServiceFinderBuilder()
                        .withNamespace(service.getNamespace())
                        .withServiceName(service.getServiceName())
                        .withDeserializer(new Deserializer<TestNodeData>() {})
                        .withSleepDuration(5)
                        .build(), 1_000, 5_000, Set.of());
        Assertions.assertThrows(IllegalStateException.class, delayedHub::start);
        val serviceFinderHub = new ServiceFinderHub<>(new DynamicDataSource(Lists.newArrayList(new Service("NS", "SERVICE"))),
                service ->  new TestServiceFinderBuilder()
                        .withNamespace(service.getNamespace())
                        .withServiceName(service.getServiceName())
                        .withDeserializer(new Deserializer<TestNodeData>() {})
                        .withSleepDuration(1)
                        .build(), 5_000, 5_000, Set.of());
        serviceFinderHub.start();
        Assertions.assertTrue(serviceFinderHub.finder(new Service("NS", "SERVICE")).isPresent());
    }


    @Test
    void testDynamicServiceAdditionWithNonDynamicDataSource() {
        val serviceFinderHub = new ServiceFinderHub<>(new StaticDataSource(new HashSet<>()), service -> new TestServiceFinderBuilder()
                .withNamespace(service.getNamespace())
                .withServiceName(service.getServiceName())
                .withDeserializer(new Deserializer<TestNodeData>() {
                })
                .build());
        serviceFinderHub.start();
        try {
            serviceFinderHub.buildFinder(new Service("NS", "SERVICE_NAME")).join();
            Assertions.fail("Exception should have been thrown");
        } catch (Exception exception) {
            Assertions.assertTrue(exception instanceof UnsupportedOperationException, "Unsupported exception should be thrown");
        }
    }

    @Test
    void testWeightedNodeSelectionWithVaryingWeights() {
        final ServiceFinderHub<TestNodeData, MapBasedServiceRegistry<TestNodeData>> serviceFinderHubVaryingWeights =
                new ServiceFinderHub<>(
                        new DynamicDataSource(Lists.newArrayList(new Service("NS", "PRE_REGISTERED_SERVICE"))),
                        service ->
                                new TestServiceFinderBuilder()
                                        .withNamespace(service.getNamespace())
                                        .withServiceName(service.getServiceName())
                                        .withNodeSelector(new WeightedRandomServiceNodeSelector<>(
                                                WeightedNodeSelectorConfig.builder()
                                                        .minNodeAgeMs(60_000)
                                                        .weightedSelectionThreshold(10)
                                                        .build()))
                                        .withDeserializer(new Deserializer<TestNodeData>() {
                                        })
                                        .withDataSource(new NodeDataSource<>() {
                                            @Override
                                            public Optional<List<ServiceNode<TestNodeData>>> refresh(
                                                    final Deserializer<TestNodeData> deserializer)
                                                    throws CommunicationException {

                                                val list = new ArrayList<ServiceNode<TestNodeData>>();
                                                list.add(new ServiceNode<>("HOST", 0,
                                                                           1f,
                                                                           TestNodeData.builder().shardId(1).build(),
                                                                           HealthcheckStatus.healthy, Long.MAX_VALUE,
                                                                           0, "HTTP"));
                                                list.add(new ServiceNode<>("HOST1", 1, 0.5,
                                                                           TestNodeData.builder().shardId(1).build(),
                                                                           HealthcheckStatus.healthy, Long.MAX_VALUE,
                                                                           0, "HTTP"));
                                                return Optional.of(list);
                                            }

                                            @Override
                                            public void start() {
                                                // No-op: Data source initialization not required for test
                                            }

                                            @Override
                                            public void ensureConnected() {
                                                // No-op: Connection management not required for test
                                            }

                                            @Override
                                            public void stop() {
                                                // No-op: Cleanup not required for test
                                            }

                                            @Override
                                            public boolean isActive() {
                                                return true;
                                            }
                                        })
                                        .build());
        serviceFinderHubVaryingWeights.start();
        final var preRegisteredServiceFinder = serviceFinderHubVaryingWeights.finder(new Service("NS", "PRE_REGISTERED_SERVICE"))
                .orElseThrow(() -> new IllegalStateException("Finder should be present"));
        final var all = preRegisteredServiceFinder.getAll(null);
        Assertions.assertEquals(2, all.size());
        int iterations = 10000;
        Map<String, Integer> selectionCounts = new HashMap<>();
        selectionCounts.put("HOST", 0);
        selectionCounts.put("HOST1", 0);
        for (int i = 0; i < iterations; i++) {
            Optional<ServiceNode<TestNodeData>> selectedNode = preRegisteredServiceFinder.get(null,
                                                                                              (criteria,
                                                                                               serviceRegistry) -> serviceRegistry.nodeList());

            Assertions.assertTrue(selectedNode.isPresent(), "Node should be selected");
            String host = selectedNode.get().getHost();

            selectionCounts.put(host, selectionCounts.getOrDefault(host, 0) + 1);
        }
        double probHost = selectionCounts.get("HOST") / (double) iterations;
        double probHost1 = selectionCounts.get("HOST1") / (double) iterations;

        // Expected probabilities
        double expectedProbHost = 1.0 / 1.5;
        double expectedProbHost1 = 0.5 / 1.5;

        // Allow some tolerance due to randomness, e.g., 5%
        double tolerance = 0.02;

        Assertions.assertTrue(Math.abs(probHost - expectedProbHost) < tolerance,
                              "Probability for HOST is not within expected tolerance");

        Assertions.assertTrue(Math.abs(probHost1 - expectedProbHost1) < tolerance,
                              "Probability for HOST1 is not within expected tolerance");

    }

    @Test
    void testWeightedNodeSelectionAboveThresholdWithVaryingWeights() {
        // Twelve nodes exceed the configured threshold of ten, exercising two-node selection.
        // HOST is sampled with probability 2/12, then beats a weight-0.5 peer with probability 1/1.5.
        final var nodeCount = 12;
        final var expectedProbHost = (2.0 / 12) * (1.0 / 1.5);
        final ServiceFinderHub<TestNodeData, MapBasedServiceRegistry<TestNodeData>> serviceFinderHubVaryingWeights =
                new ServiceFinderHub<>(
                        new DynamicDataSource(Lists.newArrayList(new Service("NS", "PRE_REGISTERED_SERVICE"))),
                        service ->
                                new TestServiceFinderBuilder()
                                        .withNamespace(service.getNamespace())
                                        .withServiceName(service.getServiceName())
                                        .withNodeSelector(new WeightedRandomServiceNodeSelector<>(
                                                WeightedNodeSelectorConfig.builder()
                                                        .minNodeAgeMs(60_000)
                                                        .weightedSelectionThreshold(10)
                                                        .build()))
                                        .withDeserializer(new Deserializer<TestNodeData>() {
                                        })
                                        .withDataSource(new NodeDataSource<>() {
                                            @Override
                                            public Optional<List<ServiceNode<TestNodeData>>> refresh(
                                                    final Deserializer<TestNodeData> deserializer)
                                                    throws CommunicationException {

                                                final var list = new ArrayList<ServiceNode<TestNodeData>>();
                                                for (int i = 0; i < nodeCount; i++) {
                                                    final var host = i == 0 ? "HOST" : "HOST" + i;
                                                    final var weight = i == 0 ? 1.0 : 0.5;
                                                    list.add(new ServiceNode<>(host, i, weight,
                                                                               TestNodeData.builder().shardId(1).build(),
                                                                               HealthcheckStatus.healthy, Long.MAX_VALUE,
                                                                               0, "HTTP"));
                                                }
                                                return Optional.of(list);
                                            }

                                            @Override
                                            public void start() {
                                                // No-op: Data source initialization not required for test
                                            }

                                            @Override
                                            public void ensureConnected() {
                                                // No-op: Connection management not required for test
                                            }

                                            @Override
                                            public void stop() {
                                                // No-op: Cleanup not required for test
                                            }

                                            @Override
                                            public boolean isActive() {
                                                return true;
                                            }
                                        })
                                        .build());
        try {
            serviceFinderHubVaryingWeights.start();
            final var finder = serviceFinderHubVaryingWeights.finder(new Service("NS", "PRE_REGISTERED_SERVICE"))
                    .orElseThrow(() -> new IllegalStateException("Finder should be present"));
            Assertions.assertEquals(nodeCount, finder.getAll(null).size());
            final var iterations = 10000;
            final var selectionCounts = new HashMap<String, Integer>();
            for (int i = 0; i < iterations; i++) {
                final var selectedNode = finder.get(null, (criteria, serviceRegistry) -> serviceRegistry.nodeList());
                Assertions.assertTrue(selectedNode.isPresent(), "Node should be selected");
                final var host = selectedNode.get().getHost();
                selectionCounts.put(host, selectionCounts.getOrDefault(host, 0) + 1);
            }

            final var tolerance = 0.02;
            // All other nodes have identical weights and split the remaining probability equally.
            final var expectedProbOtherHost = (1.0 - expectedProbHost) / (nodeCount - 1);
            for (int i = 0; i < nodeCount; i++) {
                final var host = i == 0 ? "HOST" : "HOST" + i;
                final var expectedProbability = i == 0 ? expectedProbHost : expectedProbOtherHost;
                final var probability = selectionCounts.getOrDefault(host, 0) / (double) iterations;
                Assertions.assertTrue(Math.abs(probability - expectedProbability) < tolerance,
                                      "Probability for " + host + " is not within expected tolerance");
            }
        } finally {
            serviceFinderHubVaryingWeights.stop();
        }
    }

    @Test
    void testWeightedNodeSelectionWithVaryingNodeAge() {
        final ServiceFinderHub<TestNodeData, MapBasedServiceRegistry<TestNodeData>> serviceFinderHubVaryingNodeAge =
                new ServiceFinderHub<>(
                        new DynamicDataSource(Lists.newArrayList(new Service("NS", "PRE_REGISTERED_SERVICE"))),
                        service ->
                                new TestServiceFinderBuilder()
                                        .withNamespace(service.getNamespace())
                                        .withServiceName(service.getServiceName())
                                        .withNodeSelector(new WeightedRandomServiceNodeSelector<>(
                                                WeightedNodeSelectorConfig.builder()
                                                        .minNodeAgeMs(60_000)
                                                        .weightedSelectionThreshold(10)
                                                        .build()))
                                        .withDeserializer(new Deserializer<TestNodeData>() {
                                        })
                                        .withDataSource(new NodeDataSource<>() {
                                            @Override
                                            public Optional<List<ServiceNode<TestNodeData>>> refresh(
                                                    final Deserializer<TestNodeData> deserializer)
                                                    throws CommunicationException {

                                                val list = new ArrayList<ServiceNode<TestNodeData>>();
                                                final long epochMilli = System.currentTimeMillis();
                                                final long twoMinutesInMillis = 120_000;
                                                list.add(new ServiceNode<>("HOST", 0, 1f,
                                                                           TestNodeData.builder().shardId(1).build(),
                                                                           HealthcheckStatus.healthy,
                                                                           Long.MAX_VALUE,
                                                                           epochMilli - twoMinutesInMillis, "HTTP"));
                                                list.add(new ServiceNode<>("HOST1", 1, 0.5,
                                                                           TestNodeData.builder().shardId(1).build(),
                                                                           HealthcheckStatus.healthy,
                                                                           Long.MAX_VALUE,
                                                                           epochMilli - 30_000, "HTTP"));
                                                return Optional.of(list);
                                            }

                                            @Override
                                            public void start() {
                                                // No-op: Data source initialization not required for test
                                            }

                                            @Override
                                            public void ensureConnected() {
                                                // No-op: Connection management not required for test
                                            }

                                            @Override
                                            public void stop() {
                                                // No-op: Cleanup not required for test
                                            }

                                            @Override
                                            public boolean isActive() {
                                                return true;
                                            }
                                        })
                                        .build());
        serviceFinderHubVaryingNodeAge.start();
        final var preRegisteredServiceFinder = serviceFinderHubVaryingNodeAge.finder(new Service("NS", "PRE_REGISTERED_SERVICE"))
                .orElseThrow(() -> new IllegalStateException("Finder should be present"));
        final var all = preRegisteredServiceFinder.getAll(null);
        Assertions.assertEquals(2, all.size());
        int iterations = 10000;
        Map<String, Integer> selectionCounts = new HashMap<>();
        selectionCounts.put("HOST", 0);
        selectionCounts.put("HOST1", 0);
        for (int i = 0; i < iterations; i++) {
            Optional<ServiceNode<TestNodeData>> selectedNode = preRegisteredServiceFinder.get(null,
                                                                                              (criteria,
                                                                                               serviceRegistry) -> serviceRegistry.nodeList());

            Assertions.assertTrue(selectedNode.isPresent(), "Node should be selected");
            String host = selectedNode.get().getHost();

            selectionCounts.put(host, selectionCounts.getOrDefault(host, 0) + 1);
        }
        double probHost = selectionCounts.get("HOST") / (double) iterations;
        double probHost1 = selectionCounts.get("HOST1") / (double) iterations;

        // HOST is mature; HOST1 is halfway through warm-up with routing weight 0.5.
        double expectedProbHost = 1.0 / 1.25;
        double expectedProbHost1 = 0.25 / 1.25;

        // Allow some tolerance due to randomness, e.g., 5%
        double tolerance = 0.02;

        Assertions.assertTrue(Math.abs(probHost - expectedProbHost) < tolerance,
                              "Probability for HOST is not within expected tolerance");

        Assertions.assertTrue(Math.abs(probHost1 - expectedProbHost1) < tolerance,
                              "Probability for HOST1 is not within expected tolerance");
    }

    @Test
    void testWarmupRoundRobinNodeSelectionWithVaryingNodeAge() {
        // A long window keeps acceptance close to 0.5 throughout the sampling loop.
        final var warmupDurationMs = 3_600_000L;
        final ServiceFinderHub<TestNodeData, MapBasedServiceRegistry<TestNodeData>> serviceFinderHubVaryingNodeAge =
                new ServiceFinderHub<>(
                        new DynamicDataSource(Lists.newArrayList(new Service("NS", "PRE_REGISTERED_SERVICE"))),
                        service ->
                                new TestServiceFinderBuilder()
                                        .withNamespace(service.getNamespace())
                                        .withServiceName(service.getServiceName())
                                        .withNodeSelector(new WarmupRoundRobinServiceNodeSelector<>(warmupDurationMs, 2))
                                        .withDeserializer(new Deserializer<TestNodeData>() {
                                        })
                                        .withDataSource(new NodeDataSource<>() {
                                            /** {@inheritDoc} */
                                            @Override
                                            public Optional<List<ServiceNode<TestNodeData>>> refresh(
                                                    final Deserializer<TestNodeData> deserializer) {
                                                final var epochMilli = System.currentTimeMillis();
                                                final var list = new ArrayList<ServiceNode<TestNodeData>>();
                                                list.add(new ServiceNode<>("HOST", 0, 1.0,
                                                                           TestNodeData.builder().shardId(1).build(),
                                                                           HealthcheckStatus.healthy, Long.MAX_VALUE,
                                                                           epochMilli - 2 * warmupDurationMs, "HTTP"));
                                                list.add(new ServiceNode<>("HOST1", 1, 1.0,
                                                                           TestNodeData.builder().shardId(1).build(),
                                                                           HealthcheckStatus.healthy, Long.MAX_VALUE,
                                                                           epochMilli - warmupDurationMs / 2, "HTTP"));
                                                return Optional.of(list);
                                            }

                                            /** {@inheritDoc} */
                                            @Override
                                            public void start() {
                                                // No-op: Data source initialization not required for test
                                            }

                                            /** {@inheritDoc} */
                                            @Override
                                            public void ensureConnected() {
                                                // No-op: Connection management not required for test
                                            }

                                            /** {@inheritDoc} */
                                            @Override
                                            public void stop() {
                                                // No-op: Cleanup not required for test
                                            }

                                            /** {@inheritDoc} */
                                            @Override
                                            public boolean isActive() {
                                                return true;
                                            }
                                        })
                                        .build());
        try {
            serviceFinderHubVaryingNodeAge.start();
            final var finder = serviceFinderHubVaryingNodeAge.finder(new Service("NS", "PRE_REGISTERED_SERVICE"))
                    .orElseThrow(() -> new IllegalStateException("Finder should be present"));
            Assertions.assertEquals(2, finder.getAll(null).size());

            final var iterations = 10000;
            final var selectionCounts = new HashMap<String, Integer>();
            selectionCounts.put("HOST", 0);
            selectionCounts.put("HOST1", 0);
            for (int i = 0; i < iterations; i++) {
                final var selectedNode = finder.get(null, (criteria, serviceRegistry) -> serviceRegistry.nodeList());
                Assertions.assertTrue(selectedNode.isPresent(), "Node should be selected");
                final var host = selectedNode.get().getHost();
                selectionCounts.put(host, selectionCounts.getOrDefault(host, 0) + 1);
            }

            final var probHost = selectionCounts.get("HOST") / (double) iterations;
            final var probHost1 = selectionCounts.get("HOST1") / (double) iterations;
            // Each round-robin cycle accepts HOST once and HOST1 with probability 0.5.
            // Two attempts always reach the mature node, so uniform fallback is not used.
            final var expectedProbHost = 1.0 / 1.5;
            final var expectedProbHost1 = 0.5 / 1.5;
            final var tolerance = 0.02;
            Assertions.assertTrue(Math.abs(probHost - expectedProbHost) < tolerance,
                                  "Probability for HOST is not within expected tolerance");
            Assertions.assertTrue(Math.abs(probHost1 - expectedProbHost1) < tolerance,
                                  "Probability for HOST1 is not within expected tolerance");
        } finally {
            serviceFinderHubVaryingNodeAge.stop();
        }
    }

    public class TestServiceFinderFactory  implements ServiceFinderFactory<TestNodeData, MapBasedServiceRegistry<TestNodeData>> {

        @Override
        public ServiceFinder<TestNodeData, MapBasedServiceRegistry<TestNodeData>> buildFinder(Service service) {
            val finder = new TestServiceFinderBuilder()
                    .withNamespace(service.getNamespace())
                    .withServiceName(service.getServiceName())
                    .withDeserializer(new Deserializer<TestNodeData>() {})
                    .withSleepDuration(60)
                    .build();

            finder.start();
            return finder;
        }
    }

private static class TestServiceFinderHubBuilder extends ServiceFinderHubBuilder<TestNodeData, MapBasedServiceRegistry<TestNodeData>> {

    @Override
    protected void preBuild() {

    }

    @Override
    protected void postBuild(ServiceFinderHub<TestNodeData, MapBasedServiceRegistry<TestNodeData>> serviceFinderHub) {

    }
}
    private static class TestServiceFinderBuilder extends BaseServiceFinderBuilder<TestNodeData, MapBasedServiceRegistry<TestNodeData>, ServiceFinder<TestNodeData, MapBasedServiceRegistry<TestNodeData>>, TestServiceFinderBuilder, Deserializer<TestNodeData>> {

        private int finderSleepDurationSeconds = 0;
        private NodeDataSource<TestNodeData, Deserializer<TestNodeData>> testNodeDataSource = new TestNodeDataSource();

        @Override
        public ServiceFinder<TestNodeData, MapBasedServiceRegistry<TestNodeData>> build() {
            val bf = buildFinder();
            bf.start();
            return bf;
        }

        @Override
        protected NodeDataSource<TestNodeData, Deserializer<TestNodeData>> dataSource(Service service) {
            return testNodeDataSource;
        }

        @Override
        protected ServiceFinder<TestNodeData, MapBasedServiceRegistry<TestNodeData>> buildFinder(Service service, ShardSelector<TestNodeData, MapBasedServiceRegistry<TestNodeData>> shardSelector, ServiceNodeSelector<TestNodeData> nodeSelector) {
            RangerTestUtils.sleepUntil(finderSleepDurationSeconds);
            if (null == shardSelector) {
                shardSelector = new MatchingShardSelector<>();
            }
            return new SimpleShardedServiceFinder<>(new MapBasedServiceRegistry<>(service), shardSelector, nodeSelector);
        }

        public TestServiceFinderBuilder withSleepDuration(final int finderSleepDurationSeconds) {
            this.finderSleepDurationSeconds = finderSleepDurationSeconds;
            return this;
        }

        public TestServiceFinderBuilder withDataSource(
                final NodeDataSource<TestNodeData, Deserializer<TestNodeData>> testNodeDataSource) {
            this.testNodeDataSource = testNodeDataSource;
            return this;
        }

        private static class TestNodeDataSource implements NodeDataSource<TestNodeData, Deserializer<TestNodeData>> {
            @Override
            public Optional<List<ServiceNode<TestNodeData>>> refresh(Deserializer<TestNodeData> deserializer) {
                val list = new ArrayList<ServiceNode<TestNodeData>>();
                list.add(new ServiceNode<>("HOST", 0, 1f, TestNodeData.builder().shardId(1).build(),
                                           HealthcheckStatus.healthy, Long.MAX_VALUE, 0, "HTTP"));
                return Optional.of(list);
            }

            @Override
            public void start() {

            }

            @Override
            public void ensureConnected() {

            }

            @Override
            public void stop() {

            }

            @Override
            public boolean isActive() {
                return true;
            }
        }
    }
}
