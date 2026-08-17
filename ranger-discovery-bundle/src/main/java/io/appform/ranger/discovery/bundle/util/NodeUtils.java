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

import lombok.experimental.UtilityClass;

/**
 * Utility class for managing the process-level node identifier used during ID generation.
 * The node ID is a fixed machine/process identifier — it is set once at startup
 * and shared across all threads.
 */
@UtilityClass
public class NodeUtils {
    private static volatile int node = 0;

    /**
     * Set the node ID for this process. Typically called once at service startup. Repeating the
     * same value is allowed, but replacing an already configured node ID is rejected until
     * {@link #reset()} is called.
     *
     * @param nodeId the node identifier
     */
    public static synchronized void setNode(int nodeId) {
        if (node != 0 && node != nodeId) {
            throw new IllegalStateException(
                    String.format("Node ID already set to %d; cannot change it to %d", node, nodeId));
        }
        node = nodeId;
    }

    /**
     * Get the process-level node ID.
     *
     * @return the node identifier
     */
    public static int getNode() {
        return node;
    }

    /**
     * Reset the process-level node ID back to its default (unset) value. Intended for use by
     * generator {@code cleanUp()} paths (primarily tests) so that a stale node ID from a previous
     * {@code initialize()} isn't silently reused by a subsequent {@code initialize()} that forgets
     * to call {@link #setNode(int)} again.
     */
    public static void reset() {
        node = 0;
    }
}
