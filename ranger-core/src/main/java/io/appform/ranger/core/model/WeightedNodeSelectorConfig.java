package io.appform.ranger.core.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.OptBoolean;
import io.appform.ranger.core.finder.nodeselector.WeightedRandomServiceNodeSelector;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

import static io.appform.ranger.core.finder.nodeselector.WeightedRandomServiceNodeSelector.DEFAULT_MIN_NODE_AGE;
import static io.appform.ranger.core.finder.nodeselector.WeightedRandomServiceNodeSelector.DEFAULT_WEIGHTED_SELECTION_MIN_NODES_THRESHOLD;

/** Configuration for weighted selection using routing weights and linear healthy-age warm-up. */
@Data
@EqualsAndHashCode(callSuper = false)
@ToString(callSuper = false)
@NoArgsConstructor
@AllArgsConstructor
@Builder
// weightBoostMultiplier was removed; ignore it so existing configs that still set it keep deserializing.
@JsonIgnoreProperties({"weightBoostMultiplier"})
@JsonTypeName("weighted")
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", requireTypeIdForSubtypes = OptBoolean.FALSE)
public class WeightedNodeSelectorConfig extends NodeSelectorConfig {
    /** Healthy age at full linear warm-up weight; zero disables warm-up. */
    @Min(0)
    @Max(600000)
    @Builder.Default
    private long minNodeAgeMs = DEFAULT_MIN_NODE_AGE;

    @Min(1)
    @Max(50)
    @Builder.Default
    private int weightedSelectionThreshold = DEFAULT_WEIGHTED_SELECTION_MIN_NODES_THRESHOLD;

    /**
     * Validates the warm-up duration and the threshold for exact pool-wide selection.
     *
     * @throws IllegalArgumentException if either setting is outside its supported range
     */
    public void validate() {
        if (this.getMinNodeAgeMs() < 0 || this.getMinNodeAgeMs() > 600000) {
            throw new IllegalArgumentException("minNodeAgeMs must be between 0 and 600000");
        }
        if (this.getWeightedSelectionThreshold() < 1 || this.getWeightedSelectionThreshold() > 50) {
            throw new IllegalArgumentException("weightedSelectionThreshold must be between 1 and 50");
        }
    }

    /** {@inheritDoc} */
    @Override
    public <T> ServiceNodeSelector<T> toServiceNodeSelector() {
        return new WeightedRandomServiceNodeSelector<>(this);
    }
}
