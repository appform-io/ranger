package io.appform.ranger.discovery.bundle.id.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.dropwizard.validation.ValidationMethod;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static io.appform.ranger.discovery.bundle.id.Constants.MAX_DATA_STORAGE_TIME_LIMIT_IN_SECONDS;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class IdGeneratorConfig {
    @NotEmpty
    private IdGeneratorRetryConfig retryConfig;

    @Valid
    private WeightedIdConfig weightedIdConfig;

    @NotNull
    @Min(1)
    private int idPoolSize;

    @NotNull
    @Min(1)
    private int partitionCount;

    @Min(1)
    @Builder.Default
    private int maxDataBufferTimeInSeconds = MAX_DATA_STORAGE_TIME_LIMIT_IN_SECONDS;

    @ValidationMethod(message = "Invalid Partition Range")
    @JsonIgnore
    public boolean isPartitionCountValid() {
        if (weightedIdConfig != null) {
            List<WeightedPartition> sortedPartitions = new ArrayList<>(weightedIdConfig.getPartitions());
            sortedPartitions.sort(Comparator.comparingInt(k -> k.getPartitionRange().getStart()));
            if (sortedPartitions.get(sortedPartitions.size()-1).getPartitionRange().getEnd() - sortedPartitions.get(sortedPartitions.size()-1).getPartitionRange().getStart() != partitionCount) {
                return false;
            }
        }
        return true;
    }

}