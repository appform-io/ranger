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
     * Set the node ID for this process. Typically called once at service startup.
     *
     * @param nodeId the node identifier
     */
    public static void setNode(int nodeId) {
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
}
