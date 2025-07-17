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
package io.appform.ranger.discovery.common.resolvers;

import io.appform.ranger.common.server.ShardInfo;
import io.appform.ranger.discovery.common.ServiceDiscoveryConfiguration;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;


@NoArgsConstructor
@Slf4j
public class DefaultNodeInfoResolver implements NodeInfoResolver {

    private static final String FARM_ID = "FARM_ID";

    @Override
    public ShardInfo resolve(ServiceDiscoveryConfiguration configuration) {
        val region = System.getenv(FARM_ID);
        log.debug("The region received from the env variable FARM_ID is {}. Setting the same in nodeInfo", region);
        return ShardInfo.builder()
                .environment(configuration.getEnvironment())
                .region(region)
                .tags(configuration.getTags())
                .build();
    }
}
