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
package io.appform.ranger.core.signals;

import io.appform.ranger.core.utils.RangerTestUtils;
import lombok.val;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;

class ScheduledSignalTest {

    @Test
    void testReentrantStartStop() {
        val counter = new AtomicInteger(0);
        /* Large interval so that only the initial (zero delay) execution happens per start */
        val signal = ScheduledSignal.<Integer>builder()
                .service(RangerTestUtils.service)
                .signalDataGenerator(counter::incrementAndGet)
                .refreshIntervalMillis(Duration.ofHours(1).toMillis())
                .build();

        /* Multiple starts should schedule only once */
        signal.start();
        signal.start();
        await().atMost(Duration.ofSeconds(5)).until(() -> counter.get() >= 1);
        RangerTestUtils.sleepUntil(1);
        Assertions.assertEquals(1, counter.get());

        /* Multiple stops should be no-ops after the first */
        Assertions.assertDoesNotThrow(() -> {
            signal.stop();
            signal.stop();
        });

        /* Should be able to restart after stop */
        signal.start();
        await().atMost(Duration.ofSeconds(5)).until(() -> counter.get() >= 2);
        RangerTestUtils.sleepUntil(1);
        Assertions.assertEquals(2, counter.get());
        signal.stop();
    }
}
