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
package io.appform.ranger.http.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.appform.ranger.core.model.Service;
import io.appform.ranger.core.model.ServiceNode;
import io.appform.ranger.http.config.HttpClientConfig;
import io.appform.ranger.http.serde.HTTPResponseDataDeserializer;
import io.appform.ranger.http.servicefinder.HttpCommunicator;
import io.appform.ranger.http.servicefinder.HttpNodeDataSource;
import io.appform.ranger.http.utils.RangerHttpUtils;
import lombok.val;
import okhttp3.OkHttpClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

class HttpNodeDataStoreConnectorTest {
    
    @Test
    void testHttpNodeDataStoreConnector(){
        val objectMapper = new ObjectMapper();
        val httpClientConfig = HttpClientConfig.builder()
                .host("localhost-1")
                .port(80)
                .build();
        val httpNodeDataStoreConnector = new HttpNodeDataStoreConnector<>(httpClientConfig,
                                                                          RangerHttpUtils.httpClient(httpClientConfig, objectMapper));
        Assertions.assertNotNull(httpNodeDataStoreConnector);
        Assertions.assertTrue(httpNodeDataStoreConnector.isActive());
    }

    @Test
    void testIsActiveReflectsCommunicatorHealth() {
        val communicator = new StubHttpCommunicator(true);
        val connector = new HttpNodeDataStoreConnector<>(testConfig(), communicator);
        Assertions.assertTrue(connector.isActive());
        communicator.setHealthy(false);
        Assertions.assertFalse(connector.isActive());
        communicator.setHealthy(true);
        Assertions.assertTrue(connector.isActive());
    }

    @Test
    void testIsInactiveWhenCommunicatorNull() {
        val connector = new HttpNodeDataStoreConnector<String>(testConfig(), null);
        Assertions.assertFalse(connector.isActive());
    }

    @Test
    void testNodeDataSourceIsActiveReflectsCommunicatorHealth() {
        val communicator = new StubHttpCommunicator(true);
        val dataSource = new HttpNodeDataSource<String, HTTPResponseDataDeserializer<String>>(
                Service.builder().namespace("ns").serviceName("svc").build(), testConfig(), communicator);
        Assertions.assertTrue(dataSource.isActive());
        communicator.setHealthy(false);
        Assertions.assertFalse(dataSource.isActive());
        communicator.setHealthy(true);
        Assertions.assertTrue(dataSource.isActive());
    }

    private static HttpClientConfig testConfig() {
        return HttpClientConfig.builder()
                .host("localhost-1")
                .port(80)
                .build();
    }

    private static final class StubHttpCommunicator implements HttpCommunicator<String> {
        private final AtomicBoolean healthy;

        StubHttpCommunicator(boolean healthy) {
            this.healthy = new AtomicBoolean(healthy);
        }

        void setHealthy(boolean healthy) {
            this.healthy.set(healthy);
        }

        @Override
        public boolean healthy() {
            return healthy.get();
        }

        @Override
        public Set<Service> services() {
            return Collections.emptySet();
        }

        @Override
        public List<ServiceNode<String>> listNodes(Service service, HTTPResponseDataDeserializer<String> deserializer) {
            return Collections.emptyList();
        }

        @Override
        public OkHttpClient getHttpClient() {
            return null;
        }

        @Override
        public void close() {
            //Nothing to do
        }
    }
}
