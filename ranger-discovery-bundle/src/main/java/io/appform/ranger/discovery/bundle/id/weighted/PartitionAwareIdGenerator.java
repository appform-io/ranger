package io.appform.ranger.discovery.bundle.id.weighted;

import io.appform.ranger.discovery.bundle.id.config.IdGeneratorRetryConfig;
import io.appform.ranger.discovery.bundle.id.constraints.PartitionValidationConstraint;
import io.appform.ranger.discovery.bundle.id.formatter.IdFormatter;
import io.appform.ranger.discovery.bundle.id.formatter.IdFormatters;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/**
 * Partition Aware Id Generation
 */
@SuppressWarnings("unused")
@Slf4j
public class PartitionAwareIdGenerator extends DistributedIdGenerator {
    public PartitionAwareIdGenerator(final int partitionCount,
                                     final Function<String, Integer> partitionResolverSupplier,
                                     final IdGeneratorRetryConfig retryConfig,
                                     final IdFormatter idFormatterInstance) {
        super(partitionCount, partitionResolverSupplier, retryConfig, idFormatterInstance);
    }

    public PartitionAwareIdGenerator(final int partitionCount,
                                     final Function<String, Integer> partitionResolverSupplier,
                                     final IdGeneratorRetryConfig retryConfig) {
        super(partitionCount, partitionResolverSupplier, retryConfig, IdFormatters.partitionAware());
    }

    @Override
    protected int getTargetPartitionId() {
        return SECURE_RANDOM.nextInt(partitionCount);
    }

    @Override
    protected Optional<Integer> getTargetPartitionId(final List<PartitionValidationConstraint> inConstraints, final boolean skipGlobal) {
        return Optional.ofNullable(
                retrier.get(() -> SECURE_RANDOM.nextInt(partitionCount)))
                .filter(key -> validateId(inConstraints, key, skipGlobal));
    }

}
