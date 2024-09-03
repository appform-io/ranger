package io.appform.ranger.discovery.bundle.id.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.appform.ranger.discovery.bundle.id.Constants;
import io.dropwizard.validation.ValidationMethod;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.Valid;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;


@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class IdGeneratorConfig {

    /** Optional for non-weighted scenarios */
    @Valid
    private WeightedIdConfig weightedIdConfig;

    @NotNull
    @Valid
    private DefaultNamespaceConfig defaultNamespaceConfig;

    @Builder.Default
    @Valid
    private Set<NamespaceConfig> namespaceConfig = Collections.emptySet();

    @NotNull
    @Min(1)
    private int partitionCount;

    /** Buffer time to pre-generate IDs for */
    @Min(1)
    @Max(300)
    @Builder.Default
    private int dataStorageLimitInSeconds = Constants.DEFAULT_DATA_STORAGE_TIME_LIMIT_IN_SECONDS;

    /** Retry limit for selecting a valid partition. Not required for unconstrained scenarios */
    @Min(1)
    @Builder.Default
    private int partitionRetryCount = Constants.DEFAULT_PARTITION_RETRY_COUNT;

    @ValidationMethod(message = "Namespaces should be unique")
    @JsonIgnore
    public boolean areNamespacesUnique() {
        Set<String> namespaces = namespaceConfig.stream()
                        .map(NamespaceConfig::getNamespace)
                        .collect(Collectors.toSet());
        return namespaceConfig.size() == namespaces.size();
    }

    @ValidationMethod(message = "Invalid Partition Range")
    @JsonIgnore
    public boolean isPartitionCountValid() {
        if (weightedIdConfig != null) {
            List<WeightedPartition> sortedPartitions = new ArrayList<>(weightedIdConfig.getPartitions());
            sortedPartitions.sort(Comparator.comparingInt(k -> k.getPartitionRange().getStart()));
            return sortedPartitions.get(sortedPartitions.size() - 1).getPartitionRange().getEnd() - sortedPartitions.get(0).getPartitionRange().getStart() + 1 == partitionCount;
        }
        return true;
    }

}