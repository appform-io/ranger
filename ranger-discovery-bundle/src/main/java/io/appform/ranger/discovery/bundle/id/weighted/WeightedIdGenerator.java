package io.appform.ranger.discovery.bundle.id.weighted;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import io.appform.ranger.discovery.bundle.id.config.IdGeneratorConfig;
import io.appform.ranger.discovery.bundle.id.config.PartitionRange;
import io.appform.ranger.discovery.bundle.id.config.WeightedIdConfig;
import io.appform.ranger.discovery.bundle.id.constraints.PartitionValidationConstraint;
import io.appform.ranger.discovery.bundle.id.formatter.IdFormatter;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * Weighted Id Generation
 */
@Slf4j
public class WeightedIdGenerator extends DistributedIdGenerator {
    @Getter
    private int maxShardWeight;
    private final RangeMap<Integer, PartitionRange> partitionRangeMap;

    public WeightedIdGenerator(final IdGeneratorConfig idGeneratorConfig,
                               final Function<String, Integer> partitionResolverSupplier,
                               final MetricRegistry metricRegistry) {
        super(idGeneratorConfig, partitionResolverSupplier, metricRegistry);
        Preconditions.checkNotNull(idGeneratorConfig.getWeightedIdConfig());
        partitionRangeMap = createWeightRangeMap(idGeneratorConfig.getWeightedIdConfig());
    }

    public WeightedIdGenerator(final IdGeneratorConfig idGeneratorConfig,
                               final Function<String, Integer> partitionResolverSupplier,
                               final IdFormatter idFormatterInstance,
                               final MetricRegistry metricRegistry) {
        super(idGeneratorConfig, partitionResolverSupplier, idFormatterInstance, metricRegistry);
        Preconditions.checkNotNull(idGeneratorConfig.getWeightedIdConfig());
        partitionRangeMap = createWeightRangeMap(idGeneratorConfig.getWeightedIdConfig());
    }

    private RangeMap<Integer, PartitionRange> createWeightRangeMap(final WeightedIdConfig weightedIdConfig) {
        RangeMap<Integer, PartitionRange> partitionGroups = TreeRangeMap.create();
        int end = -1;
        for (val partition : weightedIdConfig.getPartitions()) {
            int start = end + 1;
            end += partition.getWeight();
            partitionGroups.put(Range.closed(start, end), partition.getPartitionRange());
        }
        maxShardWeight = end;
        return partitionGroups;
    }

    @Override
    protected int getTargetPartitionId() {
        val randomNum = SECURE_RANDOM.nextInt(maxShardWeight);
        val partitionRange = Objects.requireNonNull(partitionRangeMap.getEntry(randomNum)).getValue();
        return SECURE_RANDOM.nextInt(partitionRange.getEnd() - partitionRange.getStart() + 1) + partitionRange.getStart();
    }

    @Override
    protected Optional<Integer> getTargetPartitionId(final List<PartitionValidationConstraint> inConstraints, final boolean skipGlobal) {
        return Optional.ofNullable(
                retrier.get(() -> {
                    val partitionId = getTargetPartitionId();
                    return validateId(inConstraints, partitionId, skipGlobal) ? partitionId : null;
                }));
    }
}

