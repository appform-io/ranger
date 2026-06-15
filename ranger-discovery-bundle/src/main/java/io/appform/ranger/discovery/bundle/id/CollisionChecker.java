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

package io.appform.ranger.discovery.bundle.id;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.BitSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Checks collisions between ids in given period
 */
@Slf4j
public class CollisionChecker {
    private final BitSet bitSet = new BitSet(1000);
    private long currentInstant = 0;
    private final Lock dataLock = new ReentrantLock();
    private static final long NANO_TIME_MS;
    private static final long CURRENT_TIME_MS;
    private static final long NANO_TO_EPOCH_OFFSET_MS;

    static {
        NANO_TIME_MS = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
        CURRENT_TIME_MS = System.currentTimeMillis();
        NANO_TO_EPOCH_OFFSET_MS = NANO_TIME_MS - CURRENT_TIME_MS;
        log.info("CollisionChecker init: NANO_TIME_MS={}, CURRENT_TIME_MS={}, NANO_TO_EPOCH_OFFSET_MS={}",
                NANO_TIME_MS, CURRENT_TIME_MS, NANO_TO_EPOCH_OFFSET_MS);
    }

    private final TimeUnit resolution;

    public CollisionChecker() {
        this(TimeUnit.MILLISECONDS);
    }

    public CollisionChecker(@NonNull TimeUnit resolution) {
        this.resolution = resolution;
    }

    public long checkAndGetTime(int location) {
        dataLock.lock();
        try {
            long resolvedTime = resolution.convert(
                    System.nanoTime(), TimeUnit.NANOSECONDS
            ) - resolution.convert(NANO_TO_EPOCH_OFFSET_MS, TimeUnit.MILLISECONDS);
            if (currentInstant != resolvedTime) {
                currentInstant = resolvedTime;
                bitSet.clear();
            }
            if (bitSet.get(location)) {
                return -1;
            }
            bitSet.set(location);
            return resolvedTime;
        } finally {
            dataLock.unlock();
        }
    }

    public void free(long time, int location) {
        dataLock.lock();
        try {
            if (currentInstant != time) {
                return;
            }
            bitSet.clear(location);
        }
        finally {
            dataLock.unlock();
        }
    }
}
