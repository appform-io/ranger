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

package io.appform.ranger.http.servicefinder;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.appform.ranger.core.model.DataStoreType;
import io.appform.ranger.core.model.Service;
import io.appform.ranger.core.model.ServiceNode;
import io.appform.ranger.core.util.MetricRecorder;
import io.appform.ranger.http.config.HttpClientConfig;
import io.appform.ranger.http.model.ServiceDataSourceResponse;
import io.appform.ranger.http.serde.HTTPResponseDataDeserializer;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import static io.appform.ranger.core.util.MetricRecorder.*;

/**
 * Direct api based communication
 */
@Slf4j
public class HttpApiCommunicator<T> implements HttpCommunicator<T> {
    private final AtomicBoolean upstreamAvailable = new AtomicBoolean(true);
    private final ScheduledExecutorService resetter = Executors.newSingleThreadScheduledExecutor();

    private final String upstreamId;
    @Getter
    private final OkHttpClient httpClient;
    private final HttpClientConfig config;
    private final ObjectMapper mapper;

    public HttpApiCommunicator(OkHttpClient httpClient, HttpClientConfig config, ObjectMapper mapper) {
        Objects.requireNonNull(mapper, "mapper has not been set for node data");
        this.upstreamId = config.getId();
        this.httpClient = httpClient;
        this.config = config;
        this.mapper = mapper;
        resetter.scheduleWithFixedDelay(() -> upstreamAvailable.set(true), 0, 60, TimeUnit.SECONDS);
    }

    @Override
    public boolean healthy() {
        return upstreamAvailable.get();
    }

    @Override
    public Set<Service> services() {
        return executeRemoteCall(() -> {
            val httpUrl = new HttpUrl.Builder()
                    .scheme(config.isSecure() ? "https" : "http")
                    .host(config.getHost())
                    .port(config.getPort() == 0 ? defaultPort() : config.getPort())
                    .encodedPath("/ranger/services/v1")
                    .addQueryParameter("skipDataFromReplicationSources", Objects.toString(config.isReplicationSource()))
                    .build();
            val request = new Request.Builder()
                    .url(httpUrl)
                    .get()
                    .build();

            try (val response = httpClient.newCall(request).execute()) {
                MetricRecorder.recordRemoteCallStatusCode(DataStoreType.HTTP, upstreamId, SERVICES_LIST, response.code());
                if (response.isSuccessful()) {
                    return parseServices(response, httpUrl);
                }
                else {
                    throw new HttpCommunicationException(
                            "Http call to returned a failure response. Url:" + httpUrl + " status: " + response.code());
                }
            }
            catch (Exception e) {
                MetricRecorder.recordRemoteCallUnknownFailure(DataStoreType.HTTP, upstreamId, SERVICES_LIST, e.getClass().getSimpleName());
                throw new HttpCommunicationException(
                        "Error parsing the response from server for url: " + httpUrl
                                + " with exception " + e.getClass().getSimpleName() + ": " + e.getMessage());
            }
        });
    }

    @Override
    public List<ServiceNode<T>> listNodes(
            Service service,
            HTTPResponseDataDeserializer<T> deserializer) {
        return executeRemoteCall(() -> {
            val url = String.format("/ranger/nodes/v1/%s/%s", service.getNamespace(), service.getServiceName());

            log.debug("Refreshing the node list from url {}", url);
            val httpUrl = new HttpUrl.Builder()
                    .scheme(config.isSecure() ? "https" : "http")
                    .host(config.getHost())
                    .port(config.getPort() == 0 ? defaultPort() : config.getPort())
                    .encodedPath(url)
                    .addQueryParameter("skipDataFromReplicationSources", Objects.toString(config.isReplicationSource()))
                    .build();
            val request = new Request.Builder()
                    .url(httpUrl)
                    .get()
                    .build();

            try (val response = httpClient.newCall(request).execute()) {
                MetricRecorder.recordRemoteCallStatusCode(DataStoreType.HTTP, upstreamId, LIST_NODES, response.code());
                if (response.isSuccessful()) {
                    return parseNodeList(service, deserializer, response, httpUrl);
                }
                else {
                    throw new HttpCommunicationException("HTTP call failed. url: " + httpUrl + " status: " + response.code());
                }
            }
            catch (Exception e) {
                MetricRecorder.recordRemoteCallUnknownFailure(DataStoreType.HTTP, upstreamId, LIST_NODES, e.getClass().getSimpleName(), service.getServiceName());
                throw new HttpCommunicationException("Error getting node data from the http endpoint: " + httpUrl +
                                                             ". Error: " + e.getMessage());
            }
        });
    }

    @Override
    public void close() throws Exception {
        // No resources to release; HTTP client lifecycle is managed externally
    }

    private <U> U executeRemoteCall(Supplier<U> executor) {
        upstreamAvailable.set(true);
        try {
            return executor.get();
        }
        catch (HttpCommunicationException e) {
            upstreamAvailable.set(false);
            throw e;
        }
    }

    private int defaultPort() {
        return config.isSecure()
               ? 443
               : 80;
    }

    private Set<Service> parseServices(Response response, HttpUrl httpUrl) {
        try (val body = response.body()) {
            if (null == body) {
                throw new HttpCommunicationException("Empty response body from: " + httpUrl);
            }
            else {
                val bytes = body.bytes();
                val serviceDataSourceResponse = mapper.readValue(bytes, ServiceDataSourceResponse.class);

                if(serviceDataSourceResponse == null || serviceDataSourceResponse.getData() == null || serviceDataSourceResponse.getData().isEmpty()) {
                    MetricRecorder.recordNullOrEmptyServicesListResponse(DataStoreType.HTTP, upstreamId);
                    log.warn("Received empty services list from http. Response body: {}", response.body());
                }

                if (serviceDataSourceResponse.valid()) {
                    return serviceDataSourceResponse.getData();
                }
                else {
                    throw new HttpCommunicationException(
                            "Http call to returned a failure response. Url:" + httpUrl + " data: " + serviceDataSourceResponse);
                }
            }
        } catch (HttpCommunicationException httpCommunicationException){
            throw httpCommunicationException;
        } catch (Exception e) {
            MetricRecorder.recordServicesParseFailure(DataStoreType.HTTP, upstreamId);
            throw new HttpCommunicationException(
                    "Error reading data from server. Url: " + httpUrl + "Error: " + e.getMessage());
        }
    }

    private <T> List<ServiceNode<T>> parseNodeList(
            Service service,
            HTTPResponseDataDeserializer<T> deserializer,
            Response response,
            HttpUrl httpUrl) {
        try (val body = response.body()) {
            if (null == body) {
                log.warn("HTTP call to {} returned empty body", httpUrl);
                MetricRecorder.recordNullOrEmptyListNodeResponse(DataStoreType.HTTP, upstreamId);
                MetricRecorder.recordNullOrEmptyListNodeResponse(DataStoreType.HTTP, upstreamId, service.getServiceName());
                throw new HttpCommunicationException("Empty response received for call to " + httpUrl);
            }
            else {
                val bytes = body.bytes();
                val serviceNodesResponse = deserializer.deserialize(bytes);

                if(serviceNodesResponse == null || serviceNodesResponse.getData() == null || serviceNodesResponse.getData().isEmpty()) {
                    MetricRecorder.recordNullOrEmptyListNodeResponse(DataStoreType.HTTP, upstreamId);
                    MetricRecorder.recordNullOrEmptyListNodeResponse(DataStoreType.HTTP, upstreamId, service.getServiceName());
                }

                if (serviceNodesResponse.valid()) {
                    return serviceNodesResponse.getData();
                }
                else {
                    throw new HttpCommunicationException(
                            "Http call returned null nodes for url: " + httpUrl + " response: " + serviceNodesResponse);
                }
            }
        } catch (HttpCommunicationException httpCommunicationException){
            throw httpCommunicationException;
        } catch (Exception e) {
            MetricRecorder.recordListNodesParseFailure(DataStoreType.HTTP, upstreamId);
            MetricRecorder.recordListNodesParseFailure(DataStoreType.HTTP, upstreamId, service.getServiceName());
            throw new HttpCommunicationException(
                    "Error parsing node data from server. Url: " + httpUrl + "Error: " + e.getMessage());
        }
    }
}
