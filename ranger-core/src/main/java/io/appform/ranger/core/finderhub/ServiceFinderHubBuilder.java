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
package io.appform.ranger.core.finderhub;

import com.google.common.base.Preconditions;
import io.appform.ranger.core.model.HubConstants;
import io.appform.ranger.core.model.ServiceRegistry;
import io.appform.ranger.core.signals.ScheduledSignal;
import io.appform.ranger.core.signals.Signal;
import lombok.val;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

/**
 *
 */
@SuppressWarnings({"unused", "UnusedReturnValue"})
public abstract class ServiceFinderHubBuilder<T, R extends ServiceRegistry<T>> {
    private ServiceDataSource serviceDataSource;
    private ServiceFinderFactory<T, R> serviceFinderFactory;
    private long refreshFrequencyMs = HubConstants.REFRESH_FREQUENCY_MS;
    private final List<Consumer<Void>> extraStartSignalConsumers = new ArrayList<>();
    private final List<Consumer<Void>> extraStopSignalConsumers = new ArrayList<>();
    private final List<Signal<Void>> extraRefreshSignals = new ArrayList<>();
    private long serviceRefreshTimeoutMs = HubConstants.SERVICE_REFRESH_TIMEOUT_MS;
    private long hubStartTimeoutMs = HubConstants.HUB_START_TIMEOUT_MS;

    private Set<String> excludedServices = new HashSet<>();

    public ServiceFinderHubBuilder<T, R> withServiceDataSource(ServiceDataSource serviceDataSource) {
        this.serviceDataSource = serviceDataSource;
        return this;
    }

    public ServiceFinderHubBuilder<T, R> withServiceFinderFactory(ServiceFinderFactory<T, R> serviceFinderFactory) {
        this.serviceFinderFactory = serviceFinderFactory;
        return this;
    }

    public ServiceFinderHubBuilder<T, R> withRefreshFrequencyMs(long refreshFrequencyMs) {
        this.refreshFrequencyMs = refreshFrequencyMs;
        return this;
    }

    public ServiceFinderHubBuilder<T, R> withExtraStartSignalConsumer(Consumer<Void> consumer) {
        this.extraStartSignalConsumers.add(consumer);
        return this;
    }

    public ServiceFinderHubBuilder<T, R> withExtraStopSignalConsumer(Consumer<Void> consumer) {
        this.extraStopSignalConsumers.add(consumer);
        return this;
    }

    public ServiceFinderHubBuilder<T, R> withExtraRefreshSignal(Signal<Void> extraRefreshSignal) {
        this.extraRefreshSignals.add(extraRefreshSignal);
        return this;
    }

    public ServiceFinderHubBuilder<T, R> withServiceRefreshTimeout(long serviceRefreshTimeoutMs) {
        this.serviceRefreshTimeoutMs = serviceRefreshTimeoutMs;
        return this;
    }

    public ServiceFinderHubBuilder<T, R> withHubStartTimeout(long hubStartTimeoutMs) {
        this.hubStartTimeoutMs = hubStartTimeoutMs;
        return this;
    }

    public ServiceFinderHubBuilder<T, R> withExcludedServices(Set<String> excludedServices) {
        this.excludedServices = Objects.requireNonNullElseGet(excludedServices, Set::of);
        return this;
    }

    public ServiceFinderHub<T, R> build() {
        preBuild();
        Preconditions.checkNotNull(serviceDataSource, "Provide a non-null service data source");
        Preconditions.checkNotNull(serviceFinderFactory, "Provide a non-null service finder factory");

        val hub = new ServiceFinderHub<>(serviceDataSource, serviceFinderFactory, serviceRefreshTimeoutMs,
                hubStartTimeoutMs, excludedServices);
        final ScheduledSignal<Void> refreshSignal = new ScheduledSignal<>("service-hub-refresh-timer",
                                                                          () -> null,
                                                                          Collections.emptyList(),
                                                                          refreshFrequencyMs);
        hub.registerUpdateSignal(refreshSignal);
        extraRefreshSignals.forEach(hub::registerUpdateSignal);

        hub.getStartSignal()
                .registerConsumer(x -> serviceDataSource.start())
                .registerConsumer(x -> refreshSignal.start())
                .registerConsumers(extraStartSignalConsumers);
        hub.getStopSignal()
                .registerConsumers(extraStopSignalConsumers)
                .registerConsumer(x -> refreshSignal.stop())
                .registerConsumer(x -> serviceDataSource.stop());
        postBuild(hub);
        return hub;
    }

    protected abstract void preBuild();

    protected abstract void postBuild(ServiceFinderHub<T, R> serviceFinderHub);
}
