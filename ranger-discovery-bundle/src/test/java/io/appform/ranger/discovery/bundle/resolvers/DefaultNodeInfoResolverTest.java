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
package io.appform.ranger.discovery.bundle.resolvers;

import io.appform.ranger.discovery.common.ServiceDiscoveryConfiguration;
import io.appform.ranger.discovery.common.resolvers.DefaultNodeInfoResolver;
import lombok.val;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class DefaultNodeInfoResolverTest {

    @Test
    void testNodeInfoResolver() {
        val configuration = ServiceDiscoveryConfiguration.builder()
                .zookeeper("connectionString")
                .namespace("test")
                .environment("testing")
                .connectionRetryIntervalMillis(5000)
                .publishedHost("TestHost")
                .publishedPort(8021)
                .initialRotationStatus(true)
                .build();
        val resolver = new DefaultNodeInfoResolver();
        val nodeInfo = resolver.resolve(configuration);
        Assertions.assertNotNull(nodeInfo);
        Assertions.assertEquals("testing", configuration.getEnvironment());
        Assertions.assertNull(nodeInfo.getRegion());
        Assertions.assertNull(nodeInfo.getTags());
    }
}
