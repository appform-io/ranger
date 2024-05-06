package io.appform.ranger.discovery.bundle.id;

import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import io.appform.ranger.discovery.bundle.id.constraints.KeyValidationConstraint;
import io.appform.ranger.discovery.bundle.id.formatter.IdFormatter;
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
    private int maxShardWeight;
    private final RangeMap<Integer, PartitionRange> partitionRangeMap;

    public WeightedIdGenerator(int node,
                               int partitionSize,
                               final Function<String, Integer> partitionResolverSupplier,
                               final WeightedIdConfig weightedIdConfig) {
        super(node, partitionSize, partitionResolverSupplier);
        partitionRangeMap = createWeightRangeMap(weightedIdConfig);
    }

    public WeightedIdGenerator(int node,
                               int partitionSize,
                               final Function<String, Integer> partitionResolverSupplier,
                               final IdFormatter idFormatterInstance,
                               final WeightedIdConfig weightedIdConfig) {
        super(node, partitionSize, partitionResolverSupplier, idFormatterInstance);
        partitionRangeMap = createWeightRangeMap(weightedIdConfig);
    }

    private RangeMap<Integer, PartitionRange> createWeightRangeMap(WeightedIdConfig weightedIdConfig) {
        RangeMap<Integer, PartitionRange> shardGroups = TreeRangeMap.create();
        int start = 0;
        int end = -1;
        for (val shard : weightedIdConfig.getPartitions()) {
            end += shard.getWeight();
            shardGroups.put(Range.closed(start, end), shard.getPartitionRange());
            start = end+1;
        }
        maxShardWeight = end;
        return shardGroups;
    }

    private int getTargetPartitionId() {
        val randomNum = SECURE_RANDOM.nextInt(maxShardWeight);
        val partitionRange = Objects.requireNonNull(partitionRangeMap.getEntry(randomNum)).getValue();
        return SECURE_RANDOM.nextInt(partitionRange.getEnd() - partitionRange.getStart() + 1) + partitionRange.getStart();
    }

    private Optional<Integer> getTargetPartitionId(final List<KeyValidationConstraint> inConstraints, boolean skipGlobal) {
        return Optional.ofNullable(
                RETRIER.get(this::getTargetPartitionId))
                .filter(key -> validateId(inConstraints, key, skipGlobal));
    }
}

