package io.appform.ranger.drove.utils;

import com.phonepe.drove.client.DroveClient;
import com.phonepe.drove.client.DroveClientConfig;
import com.phonepe.drove.client.decorators.AuthHeaderDecorator;
import com.phonepe.drove.client.decorators.BasicAuthDecorator;
import com.phonepe.drove.client.transport.httpcomponent.DroveHttpComponentsTransport;
import io.appform.ranger.drove.config.DroveUpstreamConfig;
import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;
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
@UtilityClass
public class RangerDroveUtils {
    @SneakyThrows
    public static CloseableHttpClient createHttpClient(final DroveUpstreamConfig config) {
        val connectionTimeout
                = Objects.requireNonNullElse(config.getConnectionTimeout(),
                                             DroveUpstreamConfig.DEFAULT_CONNECTION_TIMEOUT)
                .toJavaDuration();
        val cmBuilder = PoolingHttpClientConnectionManagerBuilder.create();
        if (config.isInsecure()) {
            log.warn("Creating insecure http client for drove endpoint: {}", config.getEndpoints());
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

    public static DroveClient buildDroveClient(DroveUpstreamConfig config) {
        log.info("Building drove client for: {}", config.getEndpoints());
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
