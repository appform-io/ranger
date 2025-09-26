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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.phonepe.drove.client.DroveClient;
import com.phonepe.drove.eventslistener.DroveEventPollingOffsetInMemoryStore;
import com.phonepe.drove.eventslistener.DroveRemoteEventListener;
import com.phonepe.drove.models.api.ExposedAppInfo;
import com.phonepe.drove.models.events.DroveEvent;
import com.phonepe.drove.models.events.DroveEventType;
import com.phonepe.drove.models.events.events.DroveAppStateChangeEvent;
import com.phonepe.drove.models.events.events.DroveEventVisitorAdapter;
import com.phonepe.drove.models.events.events.DroveInstanceStateChangeEvent;
import com.phonepe.drove.models.events.events.datatags.AppEventDataTag;
import com.phonepe.drove.models.events.events.datatags.AppInstanceEventDataTag;
import io.appform.ranger.core.model.Service;
import io.appform.ranger.drove.config.DroveUpstreamConfig;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.Map;
import java.util.EnumSet;
import java.util.List;

/**
 * This is an optimization to reduce upstream service calls
 */
@Slf4j
public class DroveCachingCommunicator implements DroveCommunicator {
    private final DroveCommunicator root;
    private final DroveRemoteEventListener listener;
    //Zombie check is 60 secs .. so this provides about 10 secs
    // for nodes to be refreshed
    private final LoadingCache<Service, List<ExposedAppInfo>> cache;

    public DroveCachingCommunicator(
            DroveCommunicator root,
            String namespace,
            DroveUpstreamConfig config,
            DroveClient droveClient,
            ObjectMapper mapper) {
        this.root = root;
        val offsetStore = new DroveEventPollingOffsetInMemoryStore();
        offsetStore.setLastOffset(System.currentTimeMillis()); //Only interested in new events
        this.listener = DroveRemoteEventListener.builder()
                .droveClient(droveClient)
                .mapper(mapper)
                .offsetStore(offsetStore)
                .pollInterval(Objects.requireNonNullElse(config.getEventPollingInterval(),
                                                         DroveUpstreamConfig.DEFAULT_EVENT_POLLING_INTERVAL)
                                      .toJavaDuration())
                .build();
        this.cache = Caffeine.newBuilder()
                .expireAfterWrite(Duration.ofSeconds(45)) //Delete the data after 45 secs. Will lead to sync refresh
                // if upstream is down
                .refreshAfterWrite(Duration.ofSeconds(30)) //Load async every 30 secs
                .build(new CacheLoader<>() {
                    @Override
                    public @Nullable List<ExposedAppInfo> load(@NonNull Service service) {
                        return root.listNodes(service);
                    }

                    @Override
                    public @NonNull Map<Service, List<ExposedAppInfo>> loadAll(
                            @NonNull Iterable<? extends Service> services) {
                        return root.listNodes(services); //This will throw in the case of comm failure, which is correct
                    }
                });
        val relevantEvents = EnumSet.of(DroveEventType.APP_STATE_CHANGE, DroveEventType.INSTANCE_STATE_CHANGE);
        Lists.partition(Objects.requireNonNullElse(services(), List.<String>of())
                                .stream()
                                .map(name -> new Service(namespace, name))
                                .toList(), 10)
                .forEach(cache::getAll);
        log.info("Batch loading completed");
        listener.onEventReceived().connect(events -> handleEvents(namespace, events, relevantEvents));
        listener.start();
    }

    @Override
    @SneakyThrows
    public void close() {
        listener.close();
        root.close();
    }

    @Override
    public Optional<String> leader() {
        return root.leader();
    }

    @Override
    public boolean healthy() {
        return root.healthy();
    }

    @Override
    public List<String> services() {
        return root.services();
    }

    @Override
    @SneakyThrows
    public Map<Service, List<ExposedAppInfo>> listNodes(Iterable<? extends Service> services) {
        return cache.getAll(services);
    }


    @SuppressWarnings({"unchecked", "rawtypes"})
    private void handleEvents(String namespace, List<DroveEvent> events, EnumSet<DroveEventType> relevantEvents) {
        events.stream()
                .filter(event -> relevantEvents.contains(event.getType()))
                .map(event -> event.accept(new DroveEventVisitorAdapter<Service>(null) {
                    @Override
                    public Service visit(DroveAppStateChangeEvent appStateChanged) {
                        val appName = appStateChanged.getMetadata().get(AppEventDataTag.APP_NAME);
                        log.info("Received app state change event for app: {}", appName);
                        return new Service(namespace, appName.toString());
                    }

                    @Override
                    public Service visit(DroveInstanceStateChangeEvent instanceStateChanged) {
                        val appName = instanceStateChanged.getMetadata().get(AppInstanceEventDataTag.APP_NAME);
                        log.info("Received instance state change event for app: {}", appName);
                        return new Service(namespace, appName.toString());
                    }
                }))
                .filter(Objects::nonNull)
                .map(Service.class::cast)
                .forEach(service -> {
                    log.info("Refreshing data for app due to cluster event: {}", service);
                    this.cache.refresh(service);
                });
    }
}
