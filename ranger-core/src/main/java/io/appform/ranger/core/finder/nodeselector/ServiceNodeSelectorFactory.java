package io.appform.ranger.core.finder.nodeselector;

import io.appform.ranger.core.model.NodeSelectorConfig;
import io.appform.ranger.core.model.ServiceNodeSelector;

import java.util.Objects;

/** Creates generic node selectors from Ranger-owned configuration. */
public final class ServiceNodeSelectorFactory {

    private ServiceNodeSelectorFactory() {
    }

    /**
     * Creates a new selector, without caching or sharing state between invocations.
     * Applications must handle absent configuration using their existing default or override.
     *
     * @param config the non-null selector configuration
     * @param <T> the node data type
     * @return a new selector configured from the supplied object
     * @throws NullPointerException if config is null
     * @throws IllegalArgumentException if the algorithm's configuration is invalid
     */
    public static <T> ServiceNodeSelector<T> create(final NodeSelectorConfig config) {
        return Objects.requireNonNull(config, "config must not be null").toServiceNodeSelector();
    }
}
