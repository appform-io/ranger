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
package io.appform.ranger.http.servicefinderhub;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import io.appform.ranger.core.finderhub.ServiceDataSource;
import io.appform.ranger.core.model.Service;
import io.appform.ranger.http.common.HttpNodeDataStoreConnector;
import io.appform.ranger.http.config.HttpClientConfig;
import io.appform.ranger.http.model.ServiceDataSourceResponse;
import io.appform.ranger.http.utils.HttpClientUtils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.hc.client5.http.fluent.Executor;
import org.apache.hc.client5.http.fluent.Request;
import org.apache.hc.core5.net.URIBuilder;
import org.apache.hc.core5.util.Timeout;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Slf4j
public class HttpServiceDataSource<T> extends HttpNodeDataStoreConnector<T> implements ServiceDataSource {

    public HttpServiceDataSource(HttpClientConfig config, ObjectMapper mapper, Executor httpExecutor) {
        super(config, mapper, httpExecutor);
    }

    @Override
    @SneakyThrows
    public Collection<Service> services() {
        Preconditions.checkNotNull(config, "client config has not been set for node data");
        Preconditions.checkNotNull(mapper, "mapper has not been set for node data");

        val httpUrl = new URIBuilder()
                .setScheme(config.isSecure()
                        ? "https"
                        : "http")
                .setHost(config.getHost())
                .setPort(config.getPort() == 0
                      ? defaultPort()
                      : config.getPort())
                .setPath("/ranger/services/v1")
                .build();

        return HttpClientUtils.executeRequest(httpExecutor, Request.get(httpUrl), (Function<byte[], Collection<Service>>) responseBytes -> {
            val serviceDataSourceResponse = getServiceDataSourceResponse(responseBytes);

            if (serviceDataSourceResponse.valid()) {
                return serviceDataSourceResponse.getData();
            } else {
                log.warn("Http call to {} returned an invalid response with data {}",
                        httpUrl,
                        serviceDataSourceResponse);
                return Set.of();
            }

        }, (Function<Exception, Collection<Service>>) exception -> {
            log.error("Error getting list of services from the server with httpUrl {} with exception. Executing the error handler ", httpUrl, exception);
            return Set.of();
        });
    }

    @SneakyThrows
    private ServiceDataSourceResponse getServiceDataSourceResponse(final byte[] responseBytes) {
        return mapper.readValue(responseBytes, ServiceDataSourceResponse.class);
    }
}
