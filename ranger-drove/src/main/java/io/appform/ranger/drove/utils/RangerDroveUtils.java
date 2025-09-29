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

package io.appform.ranger.drove.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.phonepe.drove.client.DroveClient;
import com.phonepe.drove.client.DroveClientConfig;
import com.phonepe.drove.client.decorators.AuthHeaderDecorator;
import com.phonepe.drove.client.decorators.BasicAuthDecorator;
import io.appform.ranger.drove.common.DroveApiCommunicator;
import io.appform.ranger.drove.common.DroveCachingCommunicator;
import io.appform.ranger.drove.common.DroveCommunicator;
import io.appform.ranger.drove.common.DroveOkHttpTransport;
import io.appform.ranger.drove.config.DroveUpstreamConfig;
import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import okhttp3.ConnectionPool;
import okhttp3.OkHttpClient;
import org.jetbrains.annotations.NotNull;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 *
 */
@Slf4j
@UtilityClass
public class RangerDroveUtils {


    @SneakyThrows
    public static OkHttpClient createOkHttpClient(final DroveUpstreamConfig config) {
        val connectionTimeout
                = Objects.requireNonNullElse(config.getConnectionTimeout(),
                        DroveUpstreamConfig.DEFAULT_CONNECTION_TIMEOUT)
                .toJavaDuration();
        val operationTimeout = Objects.requireNonNullElse(config.getOperationTimeout(),
                DroveUpstreamConfig.DEFAULT_OPERATION_TIMEOUT).toJavaDuration();
        val okHttpBuilder = new OkHttpClient.Builder();
        if (config.isInsecure()) {
            val trustAllCerts = new TrustManager[]{
                    new X509TrustManager() {
                        @Override
                        public void checkClientTrusted(java.security.cert.X509Certificate[] chain, String authType) {
                            // Trust all client certificates
                        }

                        @Override
                        public void checkServerTrusted(java.security.cert.X509Certificate[] chain, String authType) {
                            // Trust all server certificates
                        }

                        @Override
                        public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                            return new java.security.cert.X509Certificate[]{};
                        }
                    }
            };
            val sslContext = SSLContext.getInstance("SSL");
            sslContext.init(null, trustAllCerts, new java.security.SecureRandom());
            okHttpBuilder.sslSocketFactory(sslContext.getSocketFactory(), (X509TrustManager) trustAllCerts[0]);
            okHttpBuilder.hostnameVerifier((hostname, session) -> true);
            log.warn("SSL verification turned off for drove transport");
        }
        return okHttpBuilder
                .callTimeout(operationTimeout)
                .connectTimeout(connectionTimeout)
                .followRedirects(false)
                .connectionPool(new ConnectionPool(1, 30, TimeUnit.SECONDS))
                .build();
    }

    public static <T> DroveCommunicator buildDroveClient(
            String namespace,
            DroveUpstreamConfig config, ObjectMapper mapper) {
        log.info("Building drove client for: {}", config.getEndpoints());
        final var droveConfig = convertToDCConfig(config);
        final var droveClient = new DroveClient(droveConfig,
                List.of(new BasicAuthDecorator(config.getUsername(),
                                config.getPassword()),
                        new AuthHeaderDecorator(config.getAuthHeader())),
                new DroveOkHttpTransport(createOkHttpClient(config)));
        val apiCommunicator = new DroveApiCommunicator(namespace, config, droveClient, mapper);
        return config.isSkipCaching()
                ? apiCommunicator
                : new DroveCachingCommunicator(apiCommunicator, namespace, config, droveClient, mapper);
    }

    @NotNull
    public static DroveClientConfig convertToDCConfig(DroveUpstreamConfig config) {
        return new DroveClientConfig(config.getEndpoints(),
                Objects.requireNonNullElse(config.getCheckInterval(),
                                DroveUpstreamConfig.DEFAULT_CHECK_INTERVAL)
                        .toJavaDuration(),
                Objects.requireNonNullElse(config.getConnectionTimeout(),
                                DroveUpstreamConfig.DEFAULT_CONNECTION_TIMEOUT)
                        .toJavaDuration(),
                Objects.requireNonNullElse(config.getOperationTimeout(),
                                DroveUpstreamConfig.DEFAULT_OPERATION_TIMEOUT)
                        .toJavaDuration());
    }
}
