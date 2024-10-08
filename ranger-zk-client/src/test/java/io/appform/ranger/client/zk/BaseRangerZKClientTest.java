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
package io.appform.ranger.client.zk;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.appform.ranger.core.healthcheck.HealthcheckResult;
import io.appform.ranger.core.healthcheck.HealthcheckStatus;
import io.appform.ranger.core.model.ServiceNode;
import io.appform.ranger.core.serviceprovider.ServiceProvider;
import io.appform.ranger.core.signals.ExternalTriggeredSignal;
import io.appform.ranger.core.units.TestNodeData;
import io.appform.ranger.core.util.Exceptions;
import io.appform.ranger.zookeeper.ServiceProviderBuilders;
import io.appform.ranger.zookeeper.serde.ZkNodeDataSerializer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingCluster;
import org.junit.jupiter.api.AfterEach;

import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.util.Collections;
import java.util.Date;

@Slf4j
@Getter
public abstract class BaseRangerZKClientTest {

    private TestingCluster testingCluster;
    private ObjectMapper objectMapper = new ObjectMapper();
    private CuratorFramework curatorFramework;
    private ServiceProvider<TestNodeData, ZkNodeDataSerializer<TestNodeData>> provider;

    @BeforeEach
    public void startTestCluster() throws Exception {
        objectMapper = new ObjectMapper();
        testingCluster = new TestingCluster(3);
        testingCluster.start();
        curatorFramework = CuratorFrameworkFactory.builder()
                .namespace("test-n")
                .connectString(testingCluster.getConnectString())
                .retryPolicy(new ExponentialBackoffRetry(1000, 100))
                .build();
        curatorFramework.start();
        curatorFramework.blockUntilConnected();
        initilizeProvider();
        log.debug("Started zk subsystem");
    }

    @AfterEach
    public void stopTestCluster() throws Exception {
        log.debug("Stopping zk subsystem");
        curatorFramework.close();
        if (null != testingCluster) {
            testingCluster.close();
        }
        if(null != provider){
            provider.stop();
        }
    }

    protected ServiceNode<TestNodeData> read(final byte[] data) {
        try {
            return getObjectMapper().readValue(data, new TypeReference<ServiceNode<TestNodeData>>() {});
        }
        catch (IOException e) {
            Exceptions.illegalState(e);
        }
        return null;
    }

    protected byte[] write(final ServiceNode<TestNodeData> node) {
        try {
            return getObjectMapper().writeValueAsBytes(node);
        }
        catch (IOException e) {
            Exceptions.illegalState(e);
        }
        return null;
    }

    protected void initilizeProvider(){
        val refreshProviderSignal = new ExternalTriggeredSignal<>(
                () -> HealthcheckResult.builder()
                        .status(HealthcheckStatus.healthy)
                        .updatedTime(new Date().getTime())
                        .build(), Collections.emptyList());
        provider = ServiceProviderBuilders.<TestNodeData>shardedServiceProviderBuilder()
                .withHostname("localhost")
                .withPort(1080)
                .withNamespace("test-n")
                .withServiceName("s1")
                .withSerializer(this::write)
                .withNodeData(TestNodeData.builder().shardId(1).build())
                .withHealthcheck(() -> HealthcheckStatus.healthy)
                .withAdditionalRefreshSignal(refreshProviderSignal)
                .withCuratorFramework(curatorFramework)
                .build();
        provider.start();
        refreshProviderSignal.trigger();
    }


}
