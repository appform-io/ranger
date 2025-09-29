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
package io.appform.ranger.http.common;

import io.appform.ranger.core.model.NodeDataStoreConnector;
import io.appform.ranger.http.config.HttpClientConfig;
import io.appform.ranger.http.servicefinder.HttpCommunicator;
import lombok.extern.slf4j.Slf4j;

/**
 *
 */
@Slf4j
public class HttpNodeDataStoreConnector<T> implements NodeDataStoreConnector<T> {

    protected final HttpClientConfig config;
    protected final HttpCommunicator<T> httpCommunicator;

    public HttpNodeDataStoreConnector(
            final HttpClientConfig config,
            final HttpCommunicator<T> httpCommunicator) {
        this.httpCommunicator = httpCommunicator;
        this.config = config;
    }


    @Override
    public void start() {
        //Nothing to do here
    }

    @Override
    public void ensureConnected() {
        //Nothing to do here
    }

    @Override
    public void stop() {
        //Nothing to do here
    }

    protected int defaultPort() {
        return config.isSecure()
                ? 443
                : 80;
    }

    @Override
    public boolean isActive() {
        return true;
    }
}
