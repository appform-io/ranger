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
package io.appform.ranger.client.http;

import io.appform.ranger.client.AbstractRangerHubClient;
import io.appform.ranger.core.finder.nodeselector.RandomServiceNodeSelector;
import io.appform.ranger.core.finderhub.ServiceDataSource;
import io.appform.ranger.core.finderhub.ServiceFinderHub;
import io.appform.ranger.core.model.ServiceNodeSelector;
import io.appform.ranger.core.model.ServiceRegistry;
import io.appform.ranger.http.config.HttpClientConfig;
import io.appform.ranger.http.serde.HTTPResponseDataDeserializer;
import io.appform.ranger.http.servicefinder.HttpCommunicator;
import io.appform.ranger.http.servicefinderhub.HttpServiceDataSource;
import io.appform.ranger.http.servicefinderhub.HttpServiceFinderHubBuilder;
import io.appform.ranger.http.utils.RangerHttpUtils;
import lombok.Builder;
import lombok.Getter;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;

@Slf4j
@Getter
@SuperBuilder
public abstract class AbstractRangerHttpHubClient<T, R extends ServiceRegistry<T>,
        D extends HTTPResponseDataDeserializer<T>>
        extends AbstractRangerHubClient<T, R, D> {

    private final HttpClientConfig clientConfig;

    private final HttpCommunicator<T> httpClient;

    @Builder.Default
    private final ServiceNodeSelector<T> nodeSelector = new RandomServiceNodeSelector<>();

    @Override
    protected ServiceDataSource getDefaultDataSource() {
        return new HttpServiceDataSource<>(clientConfig,
                                           Objects.requireNonNullElseGet(getHttpClient(),
                                                                         () -> RangerHttpUtils.httpClient(
                                                                                 clientConfig,
                                                                                 getMapper())));
    }

    @Override
    protected ServiceFinderHub<T, R> buildHub() {
        return new HttpServiceFinderHubBuilder<T, R>()
                .withServiceDataSource(getServiceDataSource())
                .withServiceFinderFactory(getFinderFactory())
                .withRefreshFrequencyMs(getNodeRefreshTimeMs())
                .withHubStartTimeout(getHubStartTimeoutMs())
                .withServiceRefreshTimeout(getServiceRefreshTimeoutMs())
                .withExcludedServices(getExcludedServices())
                .build();
    }

    @Override
    public boolean isReplicationSource() {
        return clientConfig.isReplicationSource();
    }
}
