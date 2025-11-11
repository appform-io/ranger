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

package io.appform.ranger.zookeeper.serviceprovider;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.appform.ranger.core.healthcheck.Healthchecks;
import io.appform.ranger.zookeeper.ServiceProviderBuilders;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.curator.test.TestingCluster;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * @author tushar.naik
 * @version 1.0
 * {@code @date} 12/03/16 - 7:40 PM
 */
@Slf4j
class BaseServiceProviderBuilderTest {

    private TestingCluster testingCluster;
    private ObjectMapper objectMapper;

    @BeforeEach
    void startTestCluster() throws Exception {
        objectMapper = new ObjectMapper();
        testingCluster = new TestingCluster(3);
        testingCluster.start();
    }

    @AfterEach
    void stopTestCluster() throws Exception {
        if (null != testingCluster) {
            testingCluster.close();
        }
    }

    @Test
    void testServiceProviderBuilder() {
        val host = "localhost";
        val port = 9000;
        Exception exception = null;
        try {
            val serviceProvider = ServiceProviderBuilders.unshardedServiceProviderBuilder()
                    .withConnectionString(testingCluster.getConnectString())
                    .withNamespace("test")
                    .withServiceName("test-service")
                    .withSerializer(data -> {
                        try {
                            return objectMapper.writeValueAsBytes(data);
                        } catch (JsonProcessingException e) {
                            log.error("Serialization error", e);
                        }
                        return null;
                    })
                    .withHostname(host)
                    .withPort(port)
                    .withHealthUpdateIntervalMs(1000)
                    .build();
            serviceProvider.start();
        } catch (Exception e) {
            exception = e;
        }
        assertInstanceOf(IllegalArgumentException.class, exception);
        val serviceProvider = ServiceProviderBuilders.unshardedServiceProviderBuilder()
                .withConnectionString(testingCluster.getConnectString())
                .withNamespace("test")
                .withServiceName("test-service")
                .withSerializer(data -> {
                    try {
                        return objectMapper.writeValueAsBytes(data);
                    } catch (JsonProcessingException e) {
                        log.error("Serialization error", e);
                    }
                    return null;
                })
                .withHostname(host)
                .withHealthcheck(Healthchecks.defaultHealthyCheck())
                .withPort(port)
                .withHealthUpdateIntervalMs(1000)
                .build();
        serviceProvider.start();
    }
}