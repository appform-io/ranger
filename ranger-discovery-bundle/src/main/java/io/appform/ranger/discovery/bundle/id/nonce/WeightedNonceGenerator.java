package io.appform.ranger.discovery.bundle.id.nonce;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import io.appform.ranger.discovery.bundle.id.config.IdGeneratorConfig;
import io.appform.ranger.discovery.bundle.id.config.PartitionRange;
import io.appform.ranger.discovery.bundle.id.config.WeightedIdConfig;
import io.appform.ranger.discovery.bundle.id.formatter.IdFormatter;
import io.appform.ranger.discovery.bundle.id.formatter.IdFormatters;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.time.Clock;
import java.util.Objects;
import java.util.function.Function;


@SuppressWarnings("unused")
@Slf4j
public class WeightedNonceGenerator extends PartitionAwareNonceGenerator {
    @Getter
    private int maxShardWeight;
    private final RangeMap<Integer, PartitionRange> partitionRangeMap;


    public WeightedNonceGenerator(final int nodeId,
                                     final IdGeneratorConfig idGeneratorConfig,
                                     final Function<String, Integer> partitionResolverSupplier,
                                     final IdFormatter idFormatter,
                                     final MetricRegistry metricRegistry,
                                     final Clock clock) {
        super(nodeId, idGeneratorConfig, partitionResolverSupplier, metricRegistry, clock);
        Preconditions.checkNotNull(getIdGeneratorConfig().getWeightedIdConfig());
        partitionRangeMap = createWeightRangeMap(getIdGeneratorConfig().getWeightedIdConfig());
    }

    protected WeightedNonceGenerator(final int nodeId,
                                     final IdGeneratorConfig idGeneratorConfig,
                                     final Function<String, Integer> partitionResolverSupplier,
                                     final MetricRegistry metricRegistry,
                                     final Clock clock) {
        this(nodeId, idGeneratorConfig, partitionResolverSupplier, IdFormatters.secondPrecision(), metricRegistry, clock);
    }

    private RangeMap<Integer, PartitionRange> createWeightRangeMap(final WeightedIdConfig weightedIdConfig) {
        RangeMap<Integer, PartitionRange> partitionGroups = TreeRangeMap.create();
        int endWeight = -1;
        for (val partition : weightedIdConfig.getPartitions()) {
            int startWeight = endWeight + 1;
            endWeight += partition.getWeight();
            partitionGroups.put(Range.closed(startWeight, endWeight), partition.getPartitionRange());
        }
        maxShardWeight = endWeight;
        return partitionGroups;
    }

    @Override
    protected int getTargetPartitionId() {
        val randomNum = getSECURE_RANDOM().nextInt(maxShardWeight);
        val partitionRange = Objects.requireNonNull(partitionRangeMap.getEntry(randomNum)).getValue();
        return getSECURE_RANDOM().nextInt(partitionRange.getEnd() - partitionRange.getStart() + 1) + partitionRange.getStart();
    }

}
