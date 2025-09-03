package io.appform.ranger.core.model;

import io.appform.ranger.core.Constants;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.constraints.DecimalMax;
import javax.validation.constraints.DecimalMin;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class WeightedNodeSelectorConfig {
    @Min(0)
    @Max(600000)
    private long minNodeAgeMs = Constants.DEFAULT_MIN_NODE_AGE;

    @DecimalMin("1.0")
    @DecimalMax("5.0")
    private double boostFactor = Constants.DEFAULT_BOOST_FACTOR;

    @Min(1)
    @Max(50)
    private int weightedSelectionThreshold = Constants.DEFAULT_WEIGHTED_SELECTION_THRESHOLD;

    public void validate() {
        if (this.getMinNodeAgeMs() < 0 || this.getMinNodeAgeMs() > 600000) {
            throw new IllegalArgumentException("minNodeAgeMs must be between 0 and 600000");
        }
        if (this.getBoostFactor() < 1.0 || this.getBoostFactor() > 5.0) {
            throw new IllegalArgumentException("boostFactor must be between 1.0 and 5.0");
        }
        if (this.getWeightedSelectionThreshold() < 1 || this.getWeightedSelectionThreshold() > 50) {
            throw new IllegalArgumentException("weightedSelectionThreshold must be between 1 and 50");
        }
    }
}
