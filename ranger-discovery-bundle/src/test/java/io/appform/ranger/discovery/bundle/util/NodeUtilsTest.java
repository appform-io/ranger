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
package io.appform.ranger.discovery.bundle.util;

import lombok.val;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class NodeUtilsTest {

    @BeforeEach
    @AfterEach
    void resetNode() {
        NodeUtils.reset();
    }

    @Test
    void shouldSetNodeWhenUnset() {
        NodeUtils.setNode(23);

        Assertions.assertEquals(23, NodeUtils.getNode());
    }

    @Test
    void shouldAllowSettingSameNodeAgain() {
        NodeUtils.setNode(23);
        NodeUtils.setNode(23);

        Assertions.assertEquals(23, NodeUtils.getNode());
    }

    @Test
    void shouldRejectChangingConfiguredNode() {
        NodeUtils.setNode(23);

        val exception = Assertions.assertThrows(
                IllegalStateException.class,
                () -> NodeUtils.setNode(42));

        Assertions.assertEquals("Node ID already set to 23; cannot change it to 42", exception.getMessage());
        Assertions.assertEquals(23, NodeUtils.getNode());
    }

    @Test
    void shouldAllowChangingNodeAfterReset() {
        NodeUtils.setNode(23);
        NodeUtils.reset();

        NodeUtils.setNode(42);

        Assertions.assertEquals(42, NodeUtils.getNode());
    }
}
