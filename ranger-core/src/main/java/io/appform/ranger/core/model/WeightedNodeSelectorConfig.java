package io.appform.ranger.core.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class WeightedNodeSelectorConfig {
    private long minNodeAgeMs;
    private double boostFactor;
    private int weightedSelectionThreshold;
}
