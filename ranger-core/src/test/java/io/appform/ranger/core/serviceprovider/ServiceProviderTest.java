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
package io.appform.ranger.core.serviceprovider;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.appform.ranger.core.healthcheck.Healthchecks;
import io.appform.ranger.core.model.NodeDataSink;
import io.appform.ranger.core.model.Serializer;
import io.appform.ranger.core.model.Service;
import io.appform.ranger.core.model.ServiceNode;
import io.appform.ranger.core.units.TestNodeData;
import lombok.val;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ServiceProviderTest {

    static TestNodeData testNodeData = null;

    @SuppressWarnings("unused")
    interface TestSerializer<T> extends Serializer<T> {
        byte[] serialize(final ServiceNode<TestNodeData> node);
    }

    static class TestSerializerImpl implements TestSerializer<TestNodeData> {
        private final ObjectMapper objectMapper;

        public TestSerializerImpl() {
            objectMapper = new ObjectMapper();
        }

        @Override
        public byte[] serialize(ServiceNode<TestNodeData> node) {
            try {
                return objectMapper.writeValueAsBytes(node);
            } catch (JsonProcessingException jpe) {
                return null;
            }
        }
    }

    static class TestNodeDataSink<T extends TestNodeData, S extends TestSerializer<T>> implements NodeDataSink<T, S> {

        public TestNodeDataSink() {
            //no-op
        }

        @Override
        public void updateState(S serializer, ServiceNode<T> serviceNode) {
            testNodeData = serviceNode.getNodeData();
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

    public static class TestServiceProviderBuilder<T extends TestNodeData> extends
            BaseServiceProviderBuilder<T, TestServiceProviderBuilder<T>, TestSerializer<T>> {

        @Override
        public ServiceProvider<T, TestSerializer<T>> build() {
            return super.buildProvider();
        }

        @Override
        protected NodeDataSink<T, TestSerializer<T>> dataSink(Service service) {
            return new TestNodeDataSink<>();
        }
    }

    @Test
    void testInvalidServiceProvider() {
        Assertions.assertThrowsExactly(NullPointerException.class, () -> new TestServiceProviderBuilder<>()
                .withServiceName("test-service")
                .withNamespace("test")
                .withHostname("localhost-1")
                .withPort(9000)
                .build());
    }

    @Test
    void testInvalidServiceProviderNoHealthCheck() {
        Assertions.assertThrowsExactly(IllegalArgumentException.class, () -> new TestServiceProviderBuilder<>()
                .withServiceName("test-service")
                .withNamespace("test")
                .withHostname("localhost-1")
                .withPort(9000)
                .withSerializer(new TestSerializerImpl())
                .build());
    }

    @Test
    void testBuildServiceProvider() {
        val testProvider = new TestServiceProviderBuilder<>()
                .withServiceName("test-service")
                .withNamespace("test")
                .withHostname("localhost-1")
                .withPort(9000)
                .withSerializer(new TestSerializerImpl())
                .withNodeData(TestNodeData.builder().shardId(1).build())
                .withHealthcheck(Healthchecks.defaultHealthyCheck())
                .withHealthUpdateIntervalMs(1000)
                .build();
        testProvider.start();
        Assertions.assertNotNull(testNodeData);
        Assertions.assertEquals(1, testNodeData.getShardId());
    }

}
