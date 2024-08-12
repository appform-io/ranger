/*
 * Copyright 2015 Flipkart Internet Pvt. Ltd.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.appform.ranger.client.drove;

import com.phonepe.drove.models.api.ExposedAppInfo;
import io.appform.ranger.core.units.TestNodeData;
import io.appform.ranger.core.utils.RangerTestUtils;
import io.appform.ranger.drove.serde.DroveResponseDataDeserializer;
import io.appform.ranger.drove.utils.RangerDroveUtils;
import lombok.val;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class UnshardedRangerDroveClientTest extends BaseRangerDroveClientTest {

    @Test
    void testUnshardedRangerHubClient(){
        val clientConfig = getClientConfig();
        val namespace = "test";
        val client = UnshardedRangerDroveHubClient.<TestNodeData>builder()
                .clientConfig(clientConfig)
                .droveCommunicator(RangerDroveUtils.buildDroveClient(namespace, clientConfig, getObjectMapper()))
                .namespace(namespace)
                .deserializer(new DroveResponseDataDeserializer<>() {
                    @Override
                    public TestNodeData translate(ExposedAppInfo appInfo, ExposedAppInfo.ExposedHost host) {
                        return TestNodeData.builder().shardId(1).build();
                    }
                })
                .mapper(getObjectMapper())
                .nodeRefreshTimeMs(1000)
                .build();
        client.start();
        val service = RangerTestUtils.getService(namespace, "TEST_APP");
        Assertions.assertNotNull(client.getNode(service).orElse(null));
        Assertions.assertNotNull(client.getNode(service, nodeData -> nodeData.getShardId() == 1).orElse(null));
        Assertions.assertNull(client.getNode(service, nodeData -> nodeData.getShardId() == 2).orElse(null));
        client.stop();
    }
}
