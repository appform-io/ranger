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

package io.appform.ranger.core.healthservice.monitor;

import io.appform.ranger.core.healthservice.TimeEntity;
import io.appform.ranger.core.healthservice.monitor.sample.CountMonitor;
import io.appform.ranger.core.healthservice.monitor.sample.PingCheckMonitor;
import io.appform.ranger.core.healthservice.monitor.sample.RotationStatusMonitor;
import lombok.Getter;

import java.util.Date;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A health monitor that implements a {@link Monitor} and {@link Runnable}
 * Maintains the latest state of the Monitor, and its latest updated timestamp
 *
 * @see PingCheckMonitor
 * @see RotationStatusMonitor
 * @see CountMonitor
 */
public abstract class IsolatedHealthMonitor<T> implements Runnable, Monitor<T> {

    /* name of the monitor */
    @Getter
    protected String name;

    /* reference of the health that this monitor tracks */
    private final AtomicReference<T> healthStatus;

    /* reference to when this monitor ran successfully */
    private Date lastStatusUpdateTime;

    /* how often should this monitor run */
    @Getter
    private final TimeEntity runInterval;

    /* reference to if this monitor is disabled or not (default: false) */
    private final AtomicBoolean disabled = new AtomicBoolean(false);

    /* after how long (in milliseconds) should the monitor be regarded as stale (default: 60 seconds) */
    private final long stalenessAllowedInMillis;

    /**
     * @param name        name of the monitor
     * @param runInterval initial delay, time interval of how regularly the monitor is to be run, and timeunit
     *                    to specify how often the {@link #monitor()} check needs to be executed
     */
    protected IsolatedHealthMonitor(String name, TimeEntity runInterval) {
        this(name, runInterval, 60000);
    }

    /**
     * @param name                     name of the monitor
     * @param runInterval              initial delay, time interval of how regularly the monitor is to be run, and timeunit
     *                                 to specify how often the {@link #monitor()} check needs to be executed
     * @param stalenessAllowedInMillis after how long (in milliseconds) should the monitor be regarded as stale (default: 60 seconds)
     */
    protected IsolatedHealthMonitor(String name, TimeEntity runInterval, long stalenessAllowedInMillis) {
        this.name = name;
        this.stalenessAllowedInMillis = stalenessAllowedInMillis;
        this.healthStatus = new AtomicReference<>();
        this.runInterval = runInterval;
        this.disabled.set(false);
    }

    /**
     * updates the healthStatus with the latest value from the monitor check
     * also updates the {@code lastStatusUpdateTime}
     */
    @Override
    public void run() {
        healthStatus.set(monitor());
        lastStatusUpdateTime = new Date();
    }

    /**
     * disable the monitor, and don't use this monitor to track the aggregated health of the system
     * monitor is enabled by default
     */
    public void disable() {
        disabled.set(true);
    }

    /**
     * enable the monitor, and consider it while aggregating the health of the system
     * monitor is enabled by default
     */
    public void enable() {
        disabled.set(false);
    }

    public T getHealthStatus() {
        return healthStatus.get();
    }

    @Override
    public boolean isDisabled() {
        return disabled.get();
    }

    public boolean hasValidUpdatedTime(Date currentTime) {
        return null != lastStatusUpdateTime && (currentTime.getTime() - lastStatusUpdateTime.getTime() <= stalenessAllowedInMillis);
    }
}
