package io.appform.ranger.core.finderhub;


import com.google.common.collect.Lists;
import io.appform.ranger.core.finder.BaseServiceFinderBuilder;
import io.appform.ranger.core.finder.ServiceFinder;
import io.appform.ranger.core.finder.SimpleShardedServiceFinder;
import io.appform.ranger.core.finder.serviceregistry.MapBasedServiceRegistry;
import io.appform.ranger.core.finder.shardselector.MatchingShardSelector;
import io.appform.ranger.core.healthcheck.HealthcheckStatus;
import io.appform.ranger.core.model.*;
import io.appform.ranger.core.units.TestNodeData;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class ServiceFinderHubTest {

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
    public void testDynamicServiceAddition() {
        serviceFinderHub.start();
        val preRegisteredServiceFinder = serviceFinderHub.finder(new Service("NS", "PRE_REGISTERED_SERVICE"))
                .orElseThrow(() -> new IllegalStateException("Finder should be present"));
        val node = preRegisteredServiceFinder.get(null, (criteria, serviceRegistry) -> serviceRegistry.nodeList());
        Assert.assertTrue(node.isPresent());
        Assert.assertEquals("HOST", node.get().getHost());
        Assert.assertEquals(0, node.get().getPort());

        serviceFinderHub.buildFinder(new Service("NS", "SERVICE")).join();
        val dynamicServiceFinder = serviceFinderHub.finder(new Service("NS", "SERVICE"))
                .orElseThrow(() -> new IllegalStateException("Finder should be present"));
        val dynamicServiceNode = dynamicServiceFinder.get(null, (criteria, serviceRegistry) -> serviceRegistry.nodeList());
        Assert.assertTrue(dynamicServiceNode.isPresent());
        Assert.assertEquals("HOST", dynamicServiceNode.get().getHost());
        Assert.assertEquals(0, dynamicServiceNode.get().getPort());
    }

    @Test
    public void testDynamicServiceAdditionAsync() throws InterruptedException {
        serviceFinderHub.start();
        serviceFinderHub.buildFinder(new Service("NS", "SERVICE_NAME"));
        val finderOpt = serviceFinderHub.finder(new Service("NS", "SERVICE_NAME"));
        Assert.assertFalse("Finders will not be availbale immediately", finderOpt.isPresent());
        Thread.sleep(1000);
        val finderAfterWaitOpt = serviceFinderHub.finder(new Service("NS", "SERVICE_NAME"));
        Assert.assertTrue("Finders should be availble after some time", finderAfterWaitOpt.isPresent());
    }


    @Test
    public void testDynamicServiceAdditionWithNonDynamicDataSource() {

        val serviceFinderHub = new ServiceFinderHub<>(new StaticDataSource(new HashSet<>()), service -> new TestServiceFinderBuilder()
                .withNamespace(service.getNamespace())
                .withServiceName(service.getServiceName())
                .withDeserializer(new Deserializer<TestNodeData>() {
                })
                .build());
        serviceFinderHub.start();
        try {
            val future = serviceFinderHub.buildFinder(new Service("NS", "SERVICE_NAME"));
            future.join();
            Assert.fail("Exception should have been thrown");
        } catch (Exception exception) {
            Assert.assertTrue("Unsupported exception should be thrown", exception instanceof UnsupportedOperationException);
        }

    }

    private static class TestServiceFinderBuilder extends BaseServiceFinderBuilder<TestNodeData, MapBasedServiceRegistry<TestNodeData>, ServiceFinder<TestNodeData, MapBasedServiceRegistry<TestNodeData>>, TestServiceFinderBuilder, Deserializer<TestNodeData>> {

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
            if (null == shardSelector) {
                shardSelector = new MatchingShardSelector<>();
            }
            return new SimpleShardedServiceFinder<>(new MapBasedServiceRegistry<>(service), shardSelector, nodeSelector);

        }

        private static class TestNodeDataSource implements NodeDataSource<TestNodeData, Deserializer<TestNodeData>> {
            @Override
            public List<ServiceNode<TestNodeData>> refresh(Deserializer<TestNodeData> deserializer) {
                val list = new ArrayList<ServiceNode<TestNodeData>>();
                list.add(new ServiceNode<>("HOST", 0, TestNodeData.builder().shardId(1).build(), HealthcheckStatus.healthy, 10L, "HTTP"));
                return list;
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