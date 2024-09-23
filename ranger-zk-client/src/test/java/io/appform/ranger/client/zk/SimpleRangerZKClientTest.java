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

import io.appform.ranger.core.units.TestNodeData;
import lombok.val;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SimpleRangerZKClientTest extends BaseRangerZKClientTest {

    @Test
    void testBaseClient(){
        val client  = SimpleRangerZKClient.<TestNodeData>builder()
                .curatorFramework(getCuratorFramework())
                .deserializer(this::read)
                .namespace("test-n")
                .serviceName("s1")
                .disableWatchers(true)
                .mapper(getObjectMapper())
                .build();
        client.start();
        Assertions.assertNotNull( client.getNode().orElse(null));
        Assertions.assertNotNull(client.getNode(c -> c.getShardId() == 1).orElse(null));
        Assertions.assertNull(client.getNode(c -> c.getShardId() == 2).orElse(null));
    }
}
