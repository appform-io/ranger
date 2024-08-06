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
package io.appform.ranger.drove.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.phonepe.drove.client.DroveClient;
import com.phonepe.drove.client.DroveClientConfig;
import com.phonepe.drove.client.decorators.AuthHeaderDecorator;
import com.phonepe.drove.client.decorators.BasicAuthDecorator;
import com.phonepe.drove.client.transport.httpcomponent.DroveHttpComponentsTransport;
import io.appform.ranger.core.model.NodeDataStoreConnector;
import io.appform.ranger.drove.config.DroveUpstreamConfig;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.apache.hc.client5.http.ssl.SSLConnectionSocketFactoryBuilder;
import org.apache.hc.client5.http.ssl.TrustAllStrategy;
import org.apache.hc.core5.ssl.SSLContextBuilder;
import org.apache.hc.core5.util.TimeValue;
import org.apache.hc.core5.util.Timeout;

import java.util.List;
import java.util.Objects;

/**
 *
 */
@Slf4j
public class DroveNodeDataStoreConnector<T> implements NodeDataStoreConnector<T> {

    protected final DroveUpstreamConfig config;
    protected final ObjectMapper mapper;
    protected final DroveClient droveClient;

    public DroveNodeDataStoreConnector(
            final DroveUpstreamConfig config,
            final ObjectMapper mapper) {
        this(config,
             mapper,
             buildDroveClient(config));
    }

    public DroveNodeDataStoreConnector(
            final DroveUpstreamConfig config,
            final ObjectMapper mapper,
            final DroveClient droveClient) {
        this.config = config;
        this.mapper = mapper;
        this.droveClient = droveClient;
    }


    @Override
    public void start() {
        //Nothing to do here
    }

    @Override
    @SneakyThrows
    public void ensureConnected() {
        do {
            Thread.sleep(1_000);

        } while (droveClient.leader().orElse(null) == null);
    }

    @Override
    public void stop() {
        //Nothing to do here
    }

    @Override
    public boolean isActive() {
        return droveClient.leader().isPresent();
    }

    @SneakyThrows
    private static CloseableHttpClient createHttpClient(final DroveUpstreamConfig config) {
        val connectionTimeout
                = Objects.requireNonNullElse(config.getConnectionTimeout(),
                                             DroveUpstreamConfig.DEFAULT_CONNECTION_TIMEOUT)
                .toJavaDuration();
        val cmBuilder = PoolingHttpClientConnectionManagerBuilder.create();
        if (config.isInsecure()) {
            log.debug("Creating insecure http client");
            cmBuilder.setSSLSocketFactory(SSLConnectionSocketFactoryBuilder.create()
                                                  .setSslContext(
                                                          SSLContextBuilder.create()
                                                                  .loadTrustMaterial(TrustAllStrategy.INSTANCE)
                                                                  .build())
                                                  .setHostnameVerifier(NoopHostnameVerifier.INSTANCE)
                                                  .build());
        }
        val connectionManager = cmBuilder.build();
        connectionManager.setDefaultMaxPerRoute(Integer.MAX_VALUE);
        connectionManager.setMaxTotal(Integer.MAX_VALUE);
        connectionManager.setDefaultConnectionConfig(ConnectionConfig.custom()
                                                             .setConnectTimeout(Timeout.of(connectionTimeout))
                                                             .setSocketTimeout(Timeout.of(connectionTimeout))
                                                             .setValidateAfterInactivity(TimeValue.ofSeconds(10))
                                                             .setTimeToLive(TimeValue.ofHours(1))
                                                             .build());
        val rc = RequestConfig.custom()
                .setConnectionRequestTimeout(Timeout.of(connectionTimeout))
                .setResponseTimeout(Timeout.of(Objects.requireNonNullElse(config.getOperationTimeout(),
                                                                          DroveUpstreamConfig.DEFAULT_OPERATION_TIMEOUT)
                                                       .toJavaDuration()))
                .build();
        return HttpClients.custom()
                .disableRedirectHandling()
                .setConnectionManager(connectionManager)
                .setDefaultRequestConfig(rc)
                .build();
    }

    private static DroveClient buildDroveClient(DroveUpstreamConfig config) {
        val droveConfig = new DroveClientConfig(config.getEndpoints(),
                                                Objects.requireNonNullElse(config.getCheckInterval(),
                                                                           DroveUpstreamConfig.DEFAULT_CHECK_INTERVAL)
                                                        .toJavaDuration(),
                                                Objects.requireNonNullElse(config.getConnectionTimeout(),
                                                                           DroveUpstreamConfig.DEFAULT_CONNECTION_TIMEOUT)
                                                        .toJavaDuration(),
                                                Objects.requireNonNullElse(config.getOperationTimeout(),
                                                                           DroveUpstreamConfig.DEFAULT_OPERATION_TIMEOUT)
                                                        .toJavaDuration());
        return new DroveClient(droveConfig,
                               List.of(new BasicAuthDecorator(config.getUsername(), config.getPassword()),
                                       new AuthHeaderDecorator(config.getAuthHeader())),
                               new DroveHttpComponentsTransport(droveConfig, createHttpClient(config)));
    }
}
