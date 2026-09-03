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
package io.appform.ranger.http.serviceprovider;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.appform.ranger.core.model.DataStoreType;
import io.appform.ranger.core.model.NodeDataSink;
import io.appform.ranger.core.model.Service;
import io.appform.ranger.core.model.ServiceNode;
import io.appform.ranger.core.util.Exceptions;
import io.appform.ranger.core.util.MetricRecorder;
import io.appform.ranger.http.common.HttpNodeDataStoreConnector;
import io.appform.ranger.http.config.HttpClientConfig;
import io.appform.ranger.http.model.ServiceRegistrationResponse;
import io.appform.ranger.http.serde.HttpRequestDataSerializer;
import io.appform.ranger.http.servicefinder.HttpCommunicator;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import okhttp3.HttpUrl;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.ResponseBody;

import java.io.IOException;
import java.util.Optional;

import static io.appform.ranger.core.util.MetricRecorder.*;
import static java.util.Objects.requireNonNull;

@Slf4j
public class HttpNodeDataSink<T, S extends HttpRequestDataSerializer<T>> extends HttpNodeDataStoreConnector<T> implements NodeDataSink<T, S> {

    private final String upstreamId;
    private final Service service;
    private final ObjectMapper mapper;

    public HttpNodeDataSink(String upstreamId, Service service, HttpClientConfig config, ObjectMapper mapper, HttpCommunicator<T> httpClient) {
        super(config, httpClient);
        this.upstreamId = upstreamId;
        this.service = service;
        this.mapper = mapper;
    }

    @Override
    public DataStoreType getDataStoreType() {
        return DataStoreType.HTTP;
    }

    @Override
    public String getUpstreamId() {
        return upstreamId;
    }

    @Override
    public void updateState(S serializer, ServiceNode<T> serviceNode) {
        requireNonNull(config, "client config has not been set for node data");
        requireNonNull(mapper, "mapper has not been set for node data");

        val url = String.format("/ranger/nodes/v1/add/%s/%s", service.getNamespace(), service.getServiceName());
        log.debug("Updating state at the url {}", url);

        val httpUrl = new HttpUrl.Builder()
                .scheme(config.isSecure()
                        ? "https"
                        : "http")
                .host(config.getHost())
                .port(config.getPort() == 0
                        ? defaultPort()
                        : config.getPort())
                .encodedPath(url)
                .build();
        val serializedData = getSerializedData(service.getServiceName(), serializer, serviceNode);
        val requestBody = RequestBody.create(serializedData);
        val serviceRegistrationResponse = registerService(service.getServiceName(), httpUrl, requestBody).orElse(null);
        if(null == serviceRegistrationResponse || !serviceRegistrationResponse.valid()){
            log.warn("Http call to {} returned a failure response {}", httpUrl, serviceRegistrationResponse);
            recordNullOrEmptyRegisterServiceResponse();
            Exceptions.illegalState("Error updating state on the server for node data: " + httpUrl);
        }
        MetricRecorder.recordNodeDataSinkUpdateStatus(DataStoreType.HTTP, upstreamId, SUCCESS);
    }

    private void recordNullOrEmptyRegisterServiceResponse() {
        MetricRecorder.recordNullOrEmptyRegisterServiceResponse(DataStoreType.HTTP, upstreamId, service.getServiceName());
        MetricRecorder.recordNodeDataSinkUpdateStatus(DataStoreType.HTTP, upstreamId, FAILURE);
    }

    private <T, S extends HttpRequestDataSerializer<T>> byte[] getSerializedData(String serviceName, S serializer, ServiceNode<T> serviceNode) {
        try {
            return serializer.serialize(serviceNode);
        } catch (Exception e) {
            MetricRecorder.recordNodeDataSinkSerDeFailure(DataStoreType.HTTP, upstreamId, MetricRecorder.SERIALIZATION, serviceName, e.getClass().getSimpleName());
            log.error("Error serializing data for service {} with node {} with exception", serviceName, serviceNode, e);
            throw e;
        }
    }

    private Optional<ServiceRegistrationResponse<T>> registerService(String serviceName, HttpUrl httpUrl, RequestBody requestBody){
        val request = new Request.Builder()
                .url(httpUrl)
                .post(requestBody)
                .build();
        try (val response = httpCommunicator.getHttpClient().newCall(request).execute()) {
            MetricRecorder.recordRemoteCallStatusCode(DataStoreType.HTTP, upstreamId, REGISTER_SERVICE, response.code());
            if (response.isSuccessful()) {
                try (val body = response.body()) {
                    if (null == body) {
                        log.warn("HTTP call to {} returned empty body", httpUrl);
                    }
                    else {
                        return Optional.of(parseRegisterServiceResponse(serviceName, body));
                    }
                }
            }
            else {
                log.warn("HTTP call to {} has returned: {}", httpUrl, response.code());
            }
        }
        catch (IOException e) {
            MetricRecorder.recordNodeDataSinkUnknownFailure(DataStoreType.HTTP, upstreamId, serviceName, e.getClass().getSimpleName());
            log.error("Error updating state on the server with httpUrl {} with exception {} ",  httpUrl, e);
        }
        return Optional.empty();
    }

    private ServiceRegistrationResponse<T> parseRegisterServiceResponse(String serviceName, ResponseBody body) throws IOException {
        try {
           return  mapper.readValue(body.bytes(),
                    new TypeReference<ServiceRegistrationResponse<T>>() {
                    });
        }
        catch (IOException e) {
            MetricRecorder.recordNodeDataSinkSerDeFailure(DataStoreType.HTTP, upstreamId, DESERIALIZATION, serviceName, e.getClass().getSimpleName());
            throw e;
        }
    }
}
