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
package io.appform.ranger.http.servicefinderhub;


import io.appform.ranger.core.finderhub.ServiceDataSource;
import io.appform.ranger.core.model.Service;
import io.appform.ranger.core.model.DataStoreType;
import io.appform.ranger.core.util.MetricRecorder;
import io.appform.ranger.http.common.HttpNodeDataStoreConnector;
import io.appform.ranger.http.config.HttpClientConfig;
import io.appform.ranger.http.servicefinder.HttpCommunicator;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.Collection;
import java.util.Objects;

import static io.appform.ranger.core.util.MetricRecorder.FAILURE;
import static io.appform.ranger.core.util.MetricRecorder.SUCCESS;

@Slf4j
public class HttpServiceDataSource<T> extends HttpNodeDataStoreConnector<T> implements ServiceDataSource {

    private final String upstreamId;

    public HttpServiceDataSource(String upstreamId, HttpClientConfig config, HttpCommunicator<T> httpClient) {
        super(config, httpClient);
        this.upstreamId = upstreamId;
    }

    @Override
    public Collection<Service> services() {
       Objects.requireNonNull(config, "client config has not been set for node data");
        try {
            val result = httpCommunicator.services();
            MetricRecorder.recordServicesFetchStatus(DataStoreType.HTTP, upstreamId, SUCCESS);
            return result;
        }
        catch (Exception e) {
            MetricRecorder.recordServicesFetchStatus(DataStoreType.HTTP, upstreamId, FAILURE);
            throw e;
        }
    }
}
