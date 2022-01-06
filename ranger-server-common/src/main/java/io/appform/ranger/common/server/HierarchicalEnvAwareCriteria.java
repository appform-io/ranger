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
package io.appform.ranger.common.server;

import lombok.*;
import lombok.extern.jackson.Jacksonized;

import java.util.Arrays;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/*
    An example Predicate that you can use along with initializing ranger servers, should you wish to.

    If the current criteria is initialized with env as *, return true by default. All nodes fetched from the registry are applicable.
    Else, split the provided environment by the separator, for example, usa.cf.la gets split into ["usa", "cf", "la"]
    If the concerned ShardInfo belongs to either of these environments and the same region, provided the criteria is initialized with a region, then return true.
 */
@Value
@Builder
@Jacksonized
public class HierarchicalEnvAwareCriteria implements Predicate<ShardInfo> {
    private static final String SEPARATOR = "\\.";
    private static final String ALL_ENV = "*";

    String environment;
    String region;

    @Override
    public boolean test(ShardInfo shardInfo) {
        if(null == shardInfo || null == shardInfo.getEnvironment() || null == environment) return false;
        if(environment.equalsIgnoreCase(ALL_ENV)) return true;
        val environments = Arrays.stream(environment.split(SEPARATOR)).collect(Collectors.toList());
        return environments.stream().anyMatch(each ->
                each.equalsIgnoreCase(shardInfo.getEnvironment()) &&
                        (null == region || region.equalsIgnoreCase(shardInfo.getRegion()))
        );
    }
}
