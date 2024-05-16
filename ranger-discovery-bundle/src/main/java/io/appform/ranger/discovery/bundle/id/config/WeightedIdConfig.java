package io.appform.ranger.discovery.bundle.id.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.dropwizard.validation.ValidationMethod;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

@Getter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class WeightedIdConfig {
    @NotNull
    @Valid
    private List<WeightedPartition> partitions;

    @ValidationMethod(message = "Invalid Partition Range")
    @JsonIgnore
    public boolean isPartitionWeightsValid() {
        List<WeightedPartition> sortedPartitions = new ArrayList<>(partitions);
        sortedPartitions.sort(Comparator.comparingInt(k -> k.getPartitionRange().getStart()));
        for (int i = 0; i < sortedPartitions.size() - 1; i++) {
            WeightedPartition currentPartition = sortedPartitions.get(i);
            WeightedPartition nextPartition = sortedPartitions.get(i + 1);
            if (currentPartition.getPartitionRange().getEnd() + 1 != nextPartition.getPartitionRange().getStart()) {
                return false;
            }
        }
        return true;
    }
}