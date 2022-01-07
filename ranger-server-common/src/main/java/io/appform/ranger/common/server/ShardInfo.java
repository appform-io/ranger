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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

import java.util.Collections;
import java.util.Set;

/*
    An example nodeData with which we have written our ranger-servers. When you write your own servers you could define your own node data!
    The idea of this nodeData is to include an environment (prod/stage etc.) along with the region to indicate the DC you may be running your
    service on. It also additionally contains a livelinessScore, ranging from 0 -> 100 (that you can compute basis your own monitor running) and
    a bunch of tags, so help run an AB or the like should it be desired.
 */
@Value
@Builder
@Jacksonized
@JsonIgnoreProperties(ignoreUnknown = true)
public class ShardInfo {
    String environment;
    String region;
    @Builder.Default
    double livelinessScore = 100;
    @Builder.Default
    Set<String> tags = Collections.emptySet();
}
