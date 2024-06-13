package io.appform.ranger.discovery.bundle.id.weighted;

import com.codahale.metrics.MetricRegistry;
import io.appform.ranger.discovery.bundle.id.config.IdGeneratorConfig;
import io.appform.ranger.discovery.bundle.id.constraints.PartitionValidationConstraint;
import io.appform.ranger.discovery.bundle.id.formatter.IdFormatter;
import io.appform.ranger.discovery.bundle.id.formatter.IdFormatters;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/**
 * Partition Aware Id Generation
 */
@SuppressWarnings("unused")
@Slf4j
public class PartitionAwareIdGenerator extends DistributedIdGenerator {
    public PartitionAwareIdGenerator(final IdGeneratorConfig idGeneratorConfig,
                                     final Function<String, Integer> partitionResolverSupplier,
                                     final IdFormatter idFormatterInstance,
                                     final MetricRegistry metricRegistry) {
        super(idGeneratorConfig, partitionResolverSupplier, idFormatterInstance, metricRegistry);
    }

    public PartitionAwareIdGenerator(final IdGeneratorConfig idGeneratorConfig, final Function<String, Integer> partitionResolverSupplier,
                                     final MetricRegistry metricRegistry) {
        super(idGeneratorConfig, partitionResolverSupplier, IdFormatters.partitionAware(), metricRegistry);
    }

    @Override
    protected int getTargetPartitionId() {
        return SECURE_RANDOM.nextInt(idGeneratorConfig.getPartitionCount());
    }

    @Override
    protected Optional<Integer> getTargetPartitionId(final List<PartitionValidationConstraint> inConstraints, final boolean skipGlobal) {
        return Optional.ofNullable(
                retrier.get(() -> {
                    val partitionId = SECURE_RANDOM.nextInt(idGeneratorConfig.getPartitionCount());
                    return validateId(inConstraints, partitionId, skipGlobal) ? partitionId : null;
                })
        );
    }

}
