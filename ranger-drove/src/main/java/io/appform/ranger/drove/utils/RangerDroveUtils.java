package io.appform.ranger.drove.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.phonepe.drove.client.DroveClient;
import com.phonepe.drove.client.DroveClientConfig;
import com.phonepe.drove.client.decorators.AuthHeaderDecorator;
import com.phonepe.drove.client.decorators.BasicAuthDecorator;
import io.appform.ranger.drove.common.DroveOkHttpTransport;
import io.appform.ranger.drove.config.DroveUpstreamConfig;
import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import okhttp3.ConnectionPool;
import okhttp3.OkHttpClient;

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
        if(config.isInsecure()) {
            val trustAllCerts = new TrustManager[]{
                    new X509TrustManager() {
                        @Override
                        public void checkClientTrusted(java.security.cert.X509Certificate[] chain, String authType) {
                        }

                        @Override
                        public void checkServerTrusted(java.security.cert.X509Certificate[] chain, String authType) {
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

    public static <T> DroveCommunicator<T> buildDroveClient(
            String namespace,
            DroveUpstreamConfig config, ObjectMapper mapper) {
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
        final var droveClient = new DroveClient(droveConfig,
                                                List.of(new BasicAuthDecorator(config.getUsername(),
                                                                               config.getPassword()),
                                                        new AuthHeaderDecorator(config.getAuthHeader())),
                                                new DroveOkHttpTransport(createOkHttpClient(config)));
//                                                new DroveHttpComponentsTransport(droveConfig,
//                                                                                 createHttpClient(config)));
        val apiCommunicator = new DroveApiCommunicator<T>(namespace, config, droveClient, mapper);
        return config.isSkipCaching()
               ? apiCommunicator
               : new DroveCachingCommunicator<>(apiCommunicator, namespace, config, droveClient, mapper);
    }
}
