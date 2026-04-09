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


import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import java.util.stream.IntStream;

import static org.awaitility.Awaitility.await;


/**
 * Test on {@link CollisionChecker}
 */
class CollisionCheckerTest {

    @Test
    void testCheckAndGetTimeAndCollision() {
        CollisionChecker collisionChecker = new CollisionChecker();

        long firstAttempt = collisionChecker.checkAndGetTime(1);
        long secondAttempt = collisionChecker.checkAndGetTime(1);

        Assertions.assertTrue(firstAttempt > 0, "Should successfully return a valid timestamp");

        // the second attempt MUST be a collision if still we are on same millisecond (-1)
        if (System.currentTimeMillis() == firstAttempt) {
            Assertions.assertEquals(-1L, secondAttempt, "Should return -1 on collision");
        }
    }

    @Test
    void testTimeRolloverClearsBitSet() {
        CollisionChecker collisionChecker = new CollisionChecker();

        long firstTime = collisionChecker.checkAndGetTime(10);
        Assertions.assertTrue(firstTime > 0);
        
        await().atMost(101, TimeUnit.MILLISECONDS)
                .until(() -> System.currentTimeMillis() > firstTime);

        long secondTime = collisionChecker.checkAndGetTime(10);

        Assertions.assertTrue(secondTime > firstTime, "Time should have moved forward");
        Assertions.assertNotEquals(-1L, secondTime, "Should successfully acquire location 10 in the new millisecond");
    }

    @Test
    void testHighVolumeCheckAndGetTimeInSingleMillisecond() {
        CollisionChecker collisionChecker = new CollisionChecker();

        // Generate 1000 IDs as fast as possible.
        // We assert that any duplicate location requested in the exact same ms returns -1
        IntStream.range(0, 1000).forEach(i -> {
            long time1 = collisionChecker.checkAndGetTime(i);
            long time2 = collisionChecker.checkAndGetTime(i);
            Assertions.assertNotEquals(time2, time1);
        });
    }
}