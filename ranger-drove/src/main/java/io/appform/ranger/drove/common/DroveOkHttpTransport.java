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

package io.appform.ranger.drove.common;

import com.phonepe.drove.client.DroveClient;
import com.phonepe.drove.client.DroveHttpTransport;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import okhttp3.Headers;
import okhttp3.OkHttpClient;
import okhttp3.Request;

import java.net.URI;
import java.util.List;
import java.util.Map;

/**
 * OkHttp client transport for drove
 */
@Slf4j
public class DroveOkHttpTransport implements DroveHttpTransport {
    private final OkHttpClient httpClient;

    public DroveOkHttpTransport(final OkHttpClient httpClient) {
        this.httpClient = httpClient;
        log.info("Okhttp based transport initialized");
    }

    @Override
    @SneakyThrows
    public <T> T get(
            URI uri,
            Map<String, List<String>> headers,
            DroveClient.ResponseHandler<T> responseHandler) {
        val headersBuilder = new Headers.Builder();
        headers.forEach((headerName, headerValues) -> headerValues.forEach(value -> headersBuilder.add(headerName, value)));
        val request = new Request.Builder()
                .url(uri.toURL())
                .headers(headersBuilder.build())
                .get()
                .build();
        log.debug("Calling url: {}", uri);
        try (val response = httpClient.newCall(request).execute()) {
            val body = response.body();
            var strResponse = body != null ? body.string() : null;
            if (!response.isSuccessful()) {
                log.error("Error calling drove api {}: Status: {} Body: {}",
                        uri, response.code(), strResponse);
            }
            val droveResponse = new DroveClient.Response(response.code(),
                    response.headers().toMultimap(),
                    strResponse);
            return responseHandler.handle(droveResponse);
        } catch (Exception e) {
            log.error("Error calling drove: {}. Error: {}", e.getMessage(), e.getClass().getSimpleName());
            throw new DroveCommunicationException(e.getMessage());
        }
    }

    @Override
    public <T> T post(
            URI uri,
            Map<String, List<String>> headers,
            String body,
            DroveClient.ResponseHandler<T> responseHandler) {
        throw new UnsupportedOperationException("POST is not supported on this transport");
    }

    @Override
    public <T> T put(
            URI uri,
            Map<String, List<String>> headers,
            String body,
            DroveClient.ResponseHandler<T> responseHandler) {
        throw new UnsupportedOperationException("PUT is not supported on this transport");
    }

    @Override
    public <T> T delete(URI uri, Map<String, List<String>> headers, DroveClient.ResponseHandler<T> responseHandler) {
        throw new UnsupportedOperationException("DELETE is not supported on this transport");
    }

    @Override
    public void close() throws Exception {
        //Nothing to do here
    }
}
