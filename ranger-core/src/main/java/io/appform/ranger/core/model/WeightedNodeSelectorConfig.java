package io.appform.ranger.core.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.constraints.DecimalMax;
import javax.validation.constraints.DecimalMin;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

import static io.appform.ranger.core.finder.nodeselector.WeightedRandomServiceNodeSelector.DEFAULT_BOOST_FACTOR;
import static io.appform.ranger.core.finder.nodeselector.WeightedRandomServiceNodeSelector.DEFAULT_MIN_NODE_AGE;
import static io.appform.ranger.core.finder.nodeselector.WeightedRandomServiceNodeSelector.DEFAULT_WEIGHTED_SELECTION_MIN_NODES_THRESHOLD;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class WeightedNodeSelectorConfig {
    @Min(0)
    @Max(600000)
    private long minNodeAgeMs = DEFAULT_MIN_NODE_AGE;

    @DecimalMin("1.0")
    @DecimalMax("5.0")
    private double weightBoostMultiplier = DEFAULT_BOOST_FACTOR; // Multiplier applied to node weights for boosting older nodes during selection

    @Min(1)
    @Max(50)
    private int weightedSelectionThreshold = DEFAULT_WEIGHTED_SELECTION_MIN_NODES_THRESHOLD;

    public void validate() {
        if (this.getMinNodeAgeMs() < 0 || this.getMinNodeAgeMs() > 600000) {
            throw new IllegalArgumentException("minNodeAgeMs must be between 0 and 600000");
        }
        if (this.getWeightBoostMultiplier() < 1.0 || this.getWeightBoostMultiplier() > 5.0) {
            throw new IllegalArgumentException("boostFactor must be between 1.0 and 5.0");
        }
        if (this.getWeightedSelectionThreshold() < 1 || this.getWeightedSelectionThreshold() > 50) {
            throw new IllegalArgumentException("weightedSelectionThreshold must be between 1 and 50");
        }
    }
}
