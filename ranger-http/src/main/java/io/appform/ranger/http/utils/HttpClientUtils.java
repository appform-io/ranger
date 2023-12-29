package io.appform.ranger.http.utils;

import io.appform.ranger.http.config.HttpClientConfig;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.hc.client5.http.ClientProtocolException;
import org.apache.hc.client5.http.HttpResponseException;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.fluent.Executor;
import org.apache.hc.client5.http.fluent.Request;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.io.SocketConfig;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.util.TimeValue;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@UtilityClass
@Slf4j
public class HttpClientUtils {

    public static CloseableHttpClient getCloseableClient(final HttpClientConfig clientConfig) {
        return HttpClientBuilder.create()
                .setConnectionManager(PoolingHttpClientConnectionManagerBuilder.create()
                        .useSystemProperties()
                        .setMaxConnPerRoute(clientConfig.getMaxConnPerRoute())
                        .setMaxConnTotal(clientConfig.getMaxConnTotal())
                        .setDefaultSocketConfig(SocketConfig.custom()
                                .setTcpNoDelay(true)
                                .setSoTimeout(clientConfig.getOperationTimeout(), TimeUnit.MILLISECONDS)
                                .build())
                        .setDefaultConnectionConfig(ConnectionConfig.custom()
                                .setConnectTimeout(clientConfig.getConnectionTimeoutMs(), TimeUnit.MILLISECONDS)
                                .setSocketTimeout(clientConfig.getOperationTimeout(), TimeUnit.MILLISECONDS)
                                .setValidateAfterInactivity(TimeValue.ofMilliseconds(clientConfig.getValidateAfterInactivityMs()))
                                .setTimeToLive(clientConfig.getTtlMs(), TimeUnit.MILLISECONDS)
                                .build())
                        .build())
                .useSystemProperties()
                .evictExpiredConnections()
                .evictIdleConnections(TimeValue.ofMilliseconds(clientConfig.getIdleConnEvictMs()))
                .setDefaultRequestConfig(RequestConfig.custom()
                        .setResponseTimeout(clientConfig.getOperationTimeout(), TimeUnit.MILLISECONDS)
                        .build())
                .build();
    }

    public static <T> T executeRequest(final Executor httpExecutor,
                                       final Request request,
                                       final Function<byte[], T> successHandler,
                                       final Function<Exception, T> failureHandler) {
        try {
            return successHandler.apply(httpExecutor.execute(request).handleResponse(httpResponse -> {
                val code = httpResponse.getCode();

                if (code >= HttpStatus.SC_REDIRECTION) {
                    throw new HttpResponseException(code, httpResponse.getReasonPhrase());
                }

                val entity = httpResponse.getEntity();
                if (null == entity) {
                    throw new ClientProtocolException("Response contains no content");
                }

                return EntityUtils.toByteArray(entity);
            }));
        } catch (Exception e) {
            return failureHandler.apply(e);
        }
    }
}
