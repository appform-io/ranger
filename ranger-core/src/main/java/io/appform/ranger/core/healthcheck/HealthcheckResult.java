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
package io.appform.ranger.core.healthcheck;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class HealthcheckResult {
    HealthcheckStatus status;
    long updatedTime;
    /*
        The expectation is that, generally there will be a bounded range associated with this.
        And precision (or the lack of it, of let's say a double) may not be all that much of an issue.
        Setting it to a default 100 in case no score is going to be provided.
     */
    @Builder.Default
    int livelinessScore = 100;
}
