package io.appform.ranger.core.model;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Configuration for Ranger node selection, selected by a named JSON/YAML type.
 * Currently supports weighted selection; other selectors remain available programmatically.
 * Applications own the containing configuration and any fallback when this config is absent.
 *
 * <p>Selection is generic over node data and does not depend on a discovery transport
 * or application framework. Programmatic custom selectors can still be supplied directly.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = WeightedNodeSelectorConfig.class, name = "weighted")
})
public abstract class NodeSelectorConfig {

    /**
     * Creates a new selector after validating the algorithm's configuration.
     * Each invocation returns a fresh instance.
     *
     * @param <T> the node data type
     * @return a new selector configured from this object
     * @throws IllegalArgumentException if the configuration is invalid
     */
    public abstract <T> ServiceNodeSelector<T> toServiceNodeSelector();
}
