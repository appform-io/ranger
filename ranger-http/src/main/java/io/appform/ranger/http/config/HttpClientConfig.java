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
package io.appform.ranger.http.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

/**
 *
 */
@Value
@Builder
@Jacksonized
@JsonIgnoreProperties(ignoreUnknown = true)
public class HttpClientConfig {
    String host;
    int port;
    boolean secure;
    @Builder.Default
    int maxConnPerRoute = 10;
    @Builder.Default
    int maxConnTotal = 20;
    @Builder.Default
    int operationTimeout = 10000;
    @Builder.Default
    long connectionTimeoutMs = 10000;
    @Builder.Default
    long validateAfterInactivityMs = 10000;
    @Builder.Default
    long ttlMs = 60000;
    @Builder.Default
    long idleConnEvictMs = 60000;
}
