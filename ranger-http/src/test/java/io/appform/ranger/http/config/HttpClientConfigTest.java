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
package io.appform.ranger.http.config;

import io.appform.ranger.http.ResourceHelper;
import lombok.val;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class HttpClientConfigTest {

    @Test
    void testHttpClientConfig(){
        val resource = ResourceHelper.getResource("fixtures/httpClientConfig.json", HttpClientConfig.class);
        Assertions.assertNotNull(resource);
        Assertions.assertEquals("localhost-1", resource.getHost());
        Assertions.assertEquals(80, resource.getPort());
        Assertions.assertEquals(10, resource.getConnectionTimeoutMs());
        Assertions.assertEquals(10, resource.getOperationTimeoutMs());
    }
}
