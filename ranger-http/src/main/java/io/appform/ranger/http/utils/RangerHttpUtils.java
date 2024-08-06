package io.appform.ranger.http.utils;

import io.appform.ranger.http.config.HttpClientConfig;
import lombok.experimental.UtilityClass;
import okhttp3.ConnectionPool;
import okhttp3.OkHttpClient;

import java.util.concurrent.TimeUnit;

/**
 * Set of utilities for http client
 */
@UtilityClass
public class RangerHttpUtils {
    public static OkHttpClient httpClient(final HttpClientConfig config) {
        return new OkHttpClient.Builder()
                .callTimeout(config.getOperationTimeoutMs() == 0
                             ? 3000
                             : config.getOperationTimeoutMs(), TimeUnit.MILLISECONDS)
                .connectTimeout(config.getConnectionTimeoutMs() == 0
                                ? 3000
                                : config.getConnectionTimeoutMs(), TimeUnit.MILLISECONDS)
                .followRedirects(true)
                .connectionPool(new ConnectionPool(1, 30, TimeUnit.SECONDS))
                .build();
    }
}
