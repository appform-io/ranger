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
package io.appform.ranger.core.finderhub;

import com.github.rholder.retry.RetryerBuilder;
import com.google.common.base.Stopwatch;
import io.appform.ranger.core.finder.ServiceFinder;
import io.appform.ranger.core.model.Service;
import io.appform.ranger.core.model.ServiceRegistry;
import io.appform.ranger.core.signals.ExternalTriggeredSignal;
import io.appform.ranger.core.signals.ScheduledSignal;
import io.appform.ranger.core.signals.Signal;
import io.appform.ranger.core.util.Exceptions;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 *
 */
@Slf4j
public class ServiceFinderHub<T, R extends ServiceRegistry<T>> {
    @Getter
    private final AtomicReference<Map<Service, ServiceFinder<T, R>>> finders = new AtomicReference<>(new ConcurrentHashMap<>());
    private final Lock updateLock = new ReentrantLock();
    private final Condition updateCond = updateLock.newCondition();
    private boolean updateAvailable = false;
    private final ExecutorService executorService = Executors.newFixedThreadPool(1);

    @Getter
    private final ExternalTriggeredSignal<Void> startSignal
            = new ExternalTriggeredSignal<>(() -> null, Collections.emptyList());
    @Getter
    private final ExternalTriggeredSignal<Void> stopSignal
            = new ExternalTriggeredSignal<>(() -> null, Collections.emptyList());

    private final List<Signal<Void>> refreshSignals = new ArrayList<>();

    @Getter
    private final ServiceDataSource serviceDataSource;
    private final ServiceFinderFactory<T, R> finderFactory;

    private final AtomicBoolean alreadyUpdating = new AtomicBoolean(false);
    private Future<?> monitorFuture = null;

    public ServiceFinderHub(
            ServiceDataSource serviceDataSource,
            ServiceFinderFactory<T, R> finderFactory) {
        this.serviceDataSource = serviceDataSource;
        this.finderFactory = finderFactory;
        this.refreshSignals.add(new ScheduledSignal<>("service-hub-updater",
                () -> null,
                Collections.emptyList(),
                10_000));
    }

    public Optional<ServiceFinder<T, R>> finder(final Service service) {
        return Optional.ofNullable(finders.get().get(service));
    }

    public CompletableFuture<ServiceFinder<T, R>> buildFinder(final Service service) {
        val finder = finders.get().get(service);
        if (finder != null) {
            return CompletableFuture.completedFuture(finder);
        }
        serviceDataSource.add(service);
        return CompletableFuture.supplyAsync(() -> {
            try {
                updateAvailable();
                waitTillServiceIsReady(service);
                return finders.get().get(service);
            } catch(Exception e) {
                log.warn("Exception whiling building finder", e);
                throw e;
            }
        });
    }

    public void start() {
        log.info("Waiting for the service finder hub to start");
        val stopwatch = Stopwatch.createStarted();
        monitorFuture = executorService.submit(this::monitor);
        refreshSignals.forEach(signal -> signal.registerConsumer(x -> updateAvailable()));
        startSignal.trigger();
        updateAvailable();
        waitTillHubIsReady();
        log.info("Service finder hub started in {} ms", stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }

    public void stop() {
        stopSignal.trigger();
        if (null != monitorFuture) {
            try {
                monitorFuture.cancel(true);
            } catch (Exception e) {
                log.warn("Error stopping service finder hub monitor: {}", e.getMessage());
            }
        }
        log.info("Service finder hub stopped");
    }
    public void registerUpdateSignal(final Signal<Void> refreshSignal) {
        refreshSignals.add(refreshSignal);
    }

    public void updateAvailable() {
        try {
            updateLock.lock();
            updateAvailable = true;
            updateCond.signalAll();
        } finally {
            updateLock.unlock();
        }
    }

    private void monitor() {
        while (true) {
            try {
                updateLock.lock();
                while (!updateAvailable) {
                    updateCond.await();
                }
                updateRegistry();
            } catch (InterruptedException e) {
                log.info("Updater thread interrupted");
                Thread.currentThread().interrupt();
                break;
            } finally {
                updateAvailable = false;
                updateLock.unlock();
            }
        }
    }

    private void updateRegistry() {
        if (alreadyUpdating.get()) {
            log.warn("Service hub is already updating");
            return;
        }
        alreadyUpdating.set(true);
        final Map<Service, ServiceFinder<T, R>> updatedFinders = new HashMap<>();
        try {
            val services = serviceDataSource.services();
            val knownServiceFinders = finders.get();
            val newFinders = services.stream()
                    .filter(service -> !knownServiceFinders.containsKey(service))
                    .collect(Collectors.toMap(Function.identity(), finderFactory::buildFinder));
            val matchingServices = knownServiceFinders.entrySet()
                    .stream()
                    .filter(entry -> services.contains(entry.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            if (newFinders.isEmpty() && matchingServices.size() == knownServiceFinders.size()) {
                log.debug("No update to known list of services. Skipping update on the registry.");
                return;
            }
            updatedFinders.putAll(newFinders);
            updatedFinders.putAll(matchingServices);
            finders.set(updatedFinders);
        } catch (Exception e) {
            log.error("Error updating service list. Will maintain older list", e);
        } finally {
            alreadyUpdating.set(false);
        }
    }

    private void waitTillHubIsReady() {
        serviceDataSource.services().forEach(this::waitTillServiceIsReady);
    }

    private void waitTillServiceIsReady(Service service) {
        try {
            RetryerBuilder.<Boolean>newBuilder()
                    .retryIfResult(r -> !r)
                    .build()
                    .call(() -> Optional.ofNullable(getFinders().get().get(service))
                            .map(ServiceFinder::getServiceRegistry)
                            .map(ServiceRegistry::isRefreshed)
                            .orElse(false));
        } catch (Exception e) {
            Exceptions
                    .illegalState("Could not perform initial state for service: " + service.getServiceName(), e);
        }
    }
}
