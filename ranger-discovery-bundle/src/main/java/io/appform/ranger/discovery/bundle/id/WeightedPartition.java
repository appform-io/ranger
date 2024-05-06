package io.appform.ranger.discovery.bundle.id;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class WeightedPartition {
    @NotNull
    @Valid
    private PartitionRange partitionRange;

    @NotNull
    @Min(0)
    private int weight;
}
