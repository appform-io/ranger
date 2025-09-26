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
import io.appform.ranger.core.finder.BaseServiceFinderBuilder;
import io.appform.ranger.core.finder.ServiceFinder;
import io.appform.ranger.core.finder.SimpleShardedServiceFinder;
import io.appform.ranger.core.finder.serviceregistry.MapBasedServiceRegistry;
import io.appform.ranger.core.finder.shardselector.MatchingShardSelector;
import io.appform.ranger.core.healthcheck.HealthcheckStatus;
import io.appform.ranger.core.model.Deserializer;
import io.appform.ranger.core.model.NodeDataSource;
import io.appform.ranger.core.model.Service;
import io.appform.ranger.core.model.ServiceNode;
import io.appform.ranger.core.model.ServiceNodeSelector;
import io.appform.ranger.core.model.ShardSelector;
import io.appform.ranger.core.units.TestNodeData;
import io.appform.ranger.core.utils.RangerTestUtils;
import lombok.val;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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
        // no-op
    }

    @Override
    protected void postBuild(ServiceFinderHub<TestNodeData, MapBasedServiceRegistry<TestNodeData>> serviceFinderHub) {
        // no-op
    }
}
    private static class TestServiceFinderBuilder extends BaseServiceFinderBuilder<TestNodeData, MapBasedServiceRegistry<TestNodeData>, ServiceFinder<TestNodeData, MapBasedServiceRegistry<TestNodeData>>, TestServiceFinderBuilder, Deserializer<TestNodeData>> {

        private int finderSleepDurationSeconds = 0;

        @Override
        public ServiceFinder<TestNodeData, MapBasedServiceRegistry<TestNodeData>> build() {
            val bf = buildFinder();
            bf.start();
            return bf;
        }

        @Override
        protected NodeDataSource<TestNodeData, Deserializer<TestNodeData>> dataSource(Service service) {
            return new TestNodeDataSource();
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

        private static class TestNodeDataSource implements NodeDataSource<TestNodeData, Deserializer<TestNodeData>> {
            @Override
            public Optional<List<ServiceNode<TestNodeData>>> refresh(Deserializer<TestNodeData> deserializer) {
                val list = new ArrayList<ServiceNode<TestNodeData>>();
                list.add(new ServiceNode<>("HOST", 0, TestNodeData.builder().shardId(1).build(), HealthcheckStatus.healthy, Long.MAX_VALUE, "HTTP"));
                return Optional.of(list);
            }

            @Override
            public void start() {
                // no-op
            }

            @Override
            public void ensureConnected() {
                // no-op
            }

            @Override
            public void stop() {
                // no-op
            }

            @Override
            public boolean isActive() {
                return true;
            }
        }
    }
}