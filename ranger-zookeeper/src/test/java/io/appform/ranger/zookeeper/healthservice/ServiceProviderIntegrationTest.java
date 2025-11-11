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

package io.appform.ranger.zookeeper.healthservice;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.appform.ranger.core.finder.SimpleShardedServiceFinder;
import io.appform.ranger.core.healthcheck.Healthchecks;
import io.appform.ranger.core.healthservice.TimeEntity;
import io.appform.ranger.core.healthservice.monitor.sample.RotationStatusMonitor;
import io.appform.ranger.core.model.ServiceNode;
import io.appform.ranger.core.units.TestNodeData;
import io.appform.ranger.core.util.Exceptions;
import io.appform.ranger.core.utils.RangerTestUtils;
import io.appform.ranger.zookeeper.ServiceFinderBuilders;
import io.appform.ranger.zookeeper.ServiceProviderBuilders;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.curator.test.TestingCluster;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
class ServiceProviderIntegrationTest {

    final String filePath = "/tmp/rangerRotationFile.html";
    File file = new File(filePath);
    final String filePath2 = "/tmp/rangerRotationFile2.html";
    File anotherFile = new File(filePath2);

    private TestingCluster testingCluster;
    private ObjectMapper objectMapper;

    SimpleShardedServiceFinder<TestNodeData> serviceFinder;

    @BeforeEach
    void startTestCluster() throws Exception {
        objectMapper = new ObjectMapper();
        testingCluster = new TestingCluster(3);
        testingCluster.start();

        /* registering 3 with RotationMonitor on file and 1 on anotherFile */
        registerService("localhost-1", 9000, 1, file);
        registerService("localhost-2", 9001, 1, file);
        registerService("localhost-3", 9002, 2, file);

        registerService("localhost-4", 9003, 2, anotherFile);

        serviceFinder = ServiceFinderBuilders.<TestNodeData>shardedFinderBuilder()
                .withConnectionString(testingCluster.getConnectString())
                .withNamespace("test")
                .withServiceName("test-service")
                .withDeserializer(data -> {
                    try {
                        return objectMapper.readValue(data, new TypeReference<ServiceNode<TestNodeData>>() {
                        });
                    } catch (IOException e) {
                        Exceptions.illegalState(e);
                    }
                    return null;
                })
                .withNodeRefreshIntervalMs(1000)
                .build();
        serviceFinder.start();
    }

    @AfterEach
    void stopTestCluster() throws Exception {
        if (null != testingCluster) {
            testingCluster.close();
        }
        serviceFinder.stop();
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    void testBasicDiscovery() throws Exception {

        /* clean slate */
        file.delete();
        anotherFile.delete();

        /* with file existing, 3 nodes should be healthy */
        file.createNewFile();
        System.out.println("created file");
        RangerTestUtils.sleepUntil(5);
        List<ServiceNode<TestNodeData>> all = serviceFinder.getAll(null);
        System.out.println("all = " + all);
        assertEquals(3, all.size());

        /* with file deleted, all 3 nodes should be unhealthy */
        file.delete();
        System.out.println("deleted file");
        RangerTestUtils.sleepUntil(5);
        all = serviceFinder.getAll(null);
        System.out.println("all = " + all);
        assertEquals(0, all.size());

        /* with anotherFile created, the 4th node should become healthy and discoverable */
        anotherFile.createNewFile();
        System.out.println("created anotherFile");
        RangerTestUtils.sleepUntil(5);
        all = serviceFinder.getAll(null);
        System.out.println("all = " + all);
        assertEquals(1, all.size());

        /* clean slate */
        file.delete();
        anotherFile.delete();
    }

    private void registerService(String host, int port, int shardId, File file) {
        val serviceProvider = ServiceProviderBuilders.<TestNodeData>shardedServiceProviderBuilder()
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
                .withNodeData(TestNodeData.builder().shardId(shardId).build())
                .withHostname(host)
                .withPort(port)
                .withHealthcheck(Healthchecks.defaultHealthyCheck())
                .withIsolatedHealthMonitor(new RotationStatusMonitor(TimeEntity.everySecond(), file.getAbsolutePath()))
                .build();
        serviceProvider.start();
    }
}
