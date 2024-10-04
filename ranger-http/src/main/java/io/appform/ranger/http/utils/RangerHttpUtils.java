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

package io.appform.ranger.http.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.appform.ranger.http.config.HttpClientConfig;
import io.appform.ranger.http.servicefinder.HttpApiCommunicator;
import io.appform.ranger.http.servicefinder.HttpCommunicator;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import okhttp3.ConnectionPool;
import okhttp3.OkHttpClient;

import java.util.concurrent.TimeUnit;

/**
 * Set of utilities for http client
 */
@UtilityClass
@Slf4j
public class RangerHttpUtils {
    public static <T> HttpCommunicator<T> httpClient(
            final HttpClientConfig config,
            final ObjectMapper mapper) {
        log.info("Creating http client for {}:{}", config.getHost(), config.getPort());
        return new HttpApiCommunicator<>(
                new OkHttpClient.Builder()
                        .callTimeout(config.getOperationTimeoutMs() == 0
                                     ? 3000
                                     : config.getOperationTimeoutMs(), TimeUnit.MILLISECONDS)
                        .connectTimeout(config.getConnectionTimeoutMs() == 0
                                        ? 3000
                                        : config.getConnectionTimeoutMs(), TimeUnit.MILLISECONDS)
                        .followRedirects(true)
                        .connectionPool(new ConnectionPool(1, 30, TimeUnit.SECONDS))
                        .build(),
                config,
                mapper);
    }
}
