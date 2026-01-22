package io.appform.ranger.discovery.bundle.util;

import lombok.Setter;
import lombok.experimental.UtilityClass;

/**
 * Utility class for managing node ID information with thread-safe access.
 * Stores the node identifier used during ID generation.
 */
@UtilityClass
public class NodeUtils {
    private static final ThreadLocal<Integer> NODE_THREAD_LOCAL = new ThreadLocal<>();
    /**
     * -- SETTER --
     *  Set the default node ID used when no thread-local value is set.
     *
     * @param node the default node identifier
     */
    @Setter
    private static volatile int defaultNode = 0;

    /**
     * Set the node ID for the current thread.
     *
     * @param node the node identifier to set
     */
    public static void setNode(int node) {
        NODE_THREAD_LOCAL.set(node);
    }
    
    /**
     * Get the node ID for the current thread, or the default if not set.
     *
     * @return the node identifier
     */
    public static int getNode() {
        Integer threadNode = NODE_THREAD_LOCAL.get();
        return threadNode != null ? threadNode : defaultNode;
    }

    /**
     * Clear the thread-local node ID.
     */
    public static void clearNode() {
        NODE_THREAD_LOCAL.remove();
    }
}
