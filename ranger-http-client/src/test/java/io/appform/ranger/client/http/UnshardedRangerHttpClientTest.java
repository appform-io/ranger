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
package io.appform.ranger.client.http;

import io.appform.ranger.core.units.TestNodeData;
import io.appform.ranger.core.utils.RangerTestUtils;
import io.appform.ranger.http.utils.RangerHttpUtils;
import lombok.val;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class UnshardedRangerHttpClientTest extends BaseRangerHttpClientTest {

    @Test
    void testUnshardedRangerHubClient(){
        val httpClientConfig = getHttpClientConfig();
        val client = UnshardedRangerHttpHubClient.<TestNodeData>builder()
                .clientConfig(httpClientConfig)
                .httpClient(RangerHttpUtils.httpClient(httpClientConfig, getObjectMapper()))
                .namespace("test-n")
                .deserializer(this::read)
                .mapper(getObjectMapper())
                .nodeRefreshTimeMs(1000)
                .build();
        client.start();
        val service = RangerTestUtils.getService("test-n", "test-s");
        Assertions.assertNotNull(client.getNode(service).orElse(null));
        Assertions.assertNotNull(client.getNode(service, nodeData -> nodeData.getShardId() == 1).orElse(null));
        Assertions.assertNull(client.getNode(service, nodeData -> nodeData.getShardId() == 2).orElse(null));
        client.stop();
    }
}
