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
package io.appform.ranger.zookeeper.common;

import com.github.rholder.retry.*;
import io.appform.ranger.core.model.NodeDataStoreConnector;
import io.appform.ranger.core.model.Service;
import io.appform.ranger.core.util.Exceptions;
import io.appform.ranger.zookeeper.util.PathBuilder;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.zookeeper.KeeperException;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 */
@Slf4j
public class ZkNodeDataStoreConnector<T> implements NodeDataStoreConnector<T> {

    @Getter(AccessLevel.PROTECTED)
    protected final Service service;
    @Getter(AccessLevel.PROTECTED)
    protected final CuratorFramework curatorFramework;
    protected final ZkStoreType storeType;

    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean stopped = new AtomicBoolean(false);

    @SuppressWarnings({"UnstableApiUsage", "ConstantConditions"})
    private final Retryer<Boolean> discoveryRetrier = RetryerBuilder.<Boolean>newBuilder()
            .retryIfException(e -> IllegalStateException.class.isAssignableFrom(e.getClass()))
            .retryIfResult(aBoolean -> false)
            .withAttemptTimeLimiter(AttemptTimeLimiters.noTimeLimit())
            .withWaitStrategy(WaitStrategies.fixedWait(1, TimeUnit.SECONDS))
            .withBlockStrategy(BlockStrategies.threadSleepStrategy())
            .withRetryListener(new RetryListener() {
                @Override
                public <V> void onRetry(Attempt<V> attempt) {
                    log.debug("Retrying with attempt: {}", attempt);
                }
            })
            .build();

    protected ZkNodeDataStoreConnector(
            final Service service,
            final CuratorFramework curatorFramework,
            final ZkStoreType storeType) {
        this.service = service;
        this.curatorFramework = curatorFramework;
        this.storeType = storeType;
    }

    @Override
    public void start() {
        if (started.get()) {
            log.info("Start called on already initialized data source for service {}. Ignoring.",
                     service.getServiceName());
            return;
        }

        switch (storeType){
            case SOURCE:
                startSource();
                break;
            case SINK:
                startSink();
                break;
            default:
                Exceptions.illegalState("Unknown store type: " + storeType + " for service: " + service.getServiceName());
        }
        started.set(true);
    }

    private void startSource() {
        log.info(
                "Start called on a data source. Will ensure connection to zk cluster for service: {}",service.getServiceName());
        try {
            curatorFramework.blockUntilConnected();
            log.info("ZK Node Data Source is connected to zookeeper cluster for {}", service.getServiceName());
        } catch (InterruptedException e) {
            log.error("Thread interrupted while connecting to zk for data source");
            Thread.currentThread().interrupt();
            Exceptions.illegalState("Could not start ZK data source for service: "
                + service.getServiceName()
                + " as thread was interrupted");
        } catch (Exception e) {
            Exceptions.illegalState(
                "Could not start ZK data source for service: " + service.getServiceName(), e);
        }
    }

    private void startSink() {
        val path = PathBuilder.servicePath(service);
        try {
            curatorFramework.blockUntilConnected();
            log.info("ZK Node Data Sink is connected to zookeeper cluster for {}", service.getServiceName());
            curatorFramework
                    .create()
                    .creatingParentContainersIfNeeded()
                    .forPath(path);
            log.info("Successfully created parent containers for path : {}", path);
        }
        catch (KeeperException e) {
            if (e.code() == KeeperException.Code.NODEEXISTS) {
                log.info("Service node {} already exists for service: {}", path, service.getServiceName());
            }
        }
        catch (InterruptedException e) {
            log.error("Thread interrupted while connecting to zk for data sink");
            Thread.currentThread().interrupt();
            Exceptions.illegalState("Could not start ZK data sink for service: "
                                            + service.getServiceName()
                                            + " as thread was interrupted");
        }
        catch (Exception e) {
            Exceptions.illegalState("Could not start ZK data sink for service: " + service.getServiceName(), e);
        }
    }

    @Override
    public void ensureConnected() {
        try {
            discoveryRetrier.call(this::isActive);
        }
        catch (Exception e) {
            Exceptions.illegalState("Could not get zk connection", e);
        }
    }

    @Override
    public void stop() {
        if (stopped.get()) {
            log.info("Data source has already been stopped for: {}. Ignoring.", service.getServiceName());
            return;
        }
        if (!started.get()) {
            log.warn("Shutdown called for service: {}, but data source is not started.", service.getServiceName());
        }
        log.info("Shutting down data source for service: {}.  (It's a no-op.)",
                 service.getServiceName());
        stopped.set(true);
    }

    @Override
    public boolean isActive() {
        return curatorFramework != null && curatorFramework.getZookeeperClient() != null
                && curatorFramework.getZookeeperClient().isConnected();
    }

    protected boolean isStarted() {
        return started.get();
    }

    protected boolean isStopped() {
        return stopped.get();
    }
}
