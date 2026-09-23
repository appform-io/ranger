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
package io.appform.ranger.id;


import io.appform.ranger.id.NodeUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class IdGeneratorLifecycleTest {
    @BeforeEach
    @AfterEach
    void cleanup() {
        IdGenerator.cleanUp();
        IdGeneratorV2.cleanUp();
        NodeUtils.reset();
    }

    @Test
    void zeroIsAConfiguredNode() {
        NodeUtils.setNode(0);
        assertThrows(IllegalStateException.class, () -> NodeUtils.setNode(23));
    }

    @Test
    void repeatedInitializationDoesNotRetainExtraRegistrations() {
        IdGenerator.initialize(23);
        IdGenerator.initialize(23);
        IdGeneratorV2.initialize();
        IdGeneratorV2.initialize();
        IdGenerator.cleanUp();
        IdGeneratorV2.cleanUp();
        assertDoesNotThrow(() -> IdGenerator.initialize(42));
        assertEquals(42, IdGenerator.generate("TEST").getNode());
    }

    @Test
    void legacyCleanupKeepsV2NodeReserved() {
        IdGenerator.initialize(23);
        IdGeneratorV2.initialize();
        IdGenerator.cleanUp();
        IdGenerator.cleanUp();
        assertThrows(IllegalStateException.class, () -> NodeUtils.setNode(42));
        assertEquals(23, IdGeneratorV2.generate(
                TestUtils.getDefaultV2Formatter("TEST", "", "", false, null)).getNode());
        IdGeneratorV2.cleanUp();
        assertDoesNotThrow(() -> IdGenerator.initialize(42));
    }

    @Test
    void v2CleanupKeepsLegacyNodeReserved() {
        IdGenerator.initialize(23);
        IdGeneratorV2.initialize();
        IdGeneratorV2.cleanUp();
        assertThrows(IllegalStateException.class, () -> NodeUtils.setNode(42));
        assertEquals(23, IdGenerator.generate("TEST").getNode());
        IdGenerator.cleanUp();
        NodeUtils.setNode(42);
        IdGeneratorV2.initialize();
        assertEquals(42, IdGeneratorV2.generate(
                TestUtils.getDefaultV2Formatter("TEST", "", "", false, null)).getNode());
    }

    @Test
    void explicitResetCannotInvalidateActiveGenerators() {
        IdGenerator.initialize(0);
        assertThrows(IllegalStateException.class, NodeUtils::reset);
        assertThrows(IllegalStateException.class, () -> IdGenerator.initialize(23));
        assertEquals(0, IdGenerator.generate("TEST").getNode());
    }

    @Test
    void defaultInitializationReservesZeroUntilCleanup() {
        IdGeneratorV2.initialize();
        assertThrows(IllegalStateException.class, () -> NodeUtils.setNode(23));
        IdGeneratorV2.cleanUp();
        assertDoesNotThrow(() -> NodeUtils.setNode(23));
    }
}
