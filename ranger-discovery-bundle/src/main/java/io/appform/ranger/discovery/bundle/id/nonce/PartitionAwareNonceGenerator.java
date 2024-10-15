package io.appform.ranger.discovery.bundle.id.nonce;

import com.codahale.metrics.MetricRegistry;
import dev.failsafe.event.ExecutionAttemptedEvent;
import io.appform.ranger.discovery.bundle.id.GenerationResult;
import io.appform.ranger.discovery.bundle.id.IdInfo;
import io.appform.ranger.discovery.bundle.id.IdUtils;
import io.appform.ranger.discovery.bundle.id.IdValidationState;
import io.appform.ranger.discovery.bundle.id.PartitionIdTracker;
import io.appform.ranger.discovery.bundle.id.config.IdGeneratorConfig;
import io.appform.ranger.discovery.bundle.id.config.NamespaceConfig;
import io.appform.ranger.discovery.bundle.id.formatter.IdFormatter;
import io.appform.ranger.discovery.bundle.id.formatter.IdFormatters;
import io.appform.ranger.discovery.bundle.id.generator.IdGeneratorBase;
import io.appform.ranger.discovery.bundle.id.request.IdGenerationRequest;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.time.Clock;
import java.time.Instant;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;


@SuppressWarnings("unused")
@Slf4j
@Getter
public class PartitionAwareNonceGenerator extends NonceGeneratorBase {
    private final Map<String, PartitionIdTracker[]> idStore = new ConcurrentHashMap<>();
    private final Function<String, Integer> partitionResolver;
    private final IdGeneratorConfig idGeneratorConfig;
    private final MetricRegistry metricRegistry;
    private final Clock clock;
    HashSet<Integer> timeKeys = new HashSet<>();


    /**  idStore Structure
    {
        namespace: [
            timestamp: {
                partitions: [
                    {
                        ids: [],
                        pointer: int
                    },
                    {
                        ids: [],
                        pointer: int
                    } ...
                ],
                nextIdCounter: int
            }
        ]
    }
    */

    public PartitionAwareNonceGenerator(final IdGeneratorConfig idGeneratorConfig,
                                        final Function<String, Integer> partitionResolverSupplier,
                                        final IdFormatter idFormatter,
                                        final MetricRegistry metricRegistry,
                                        final Clock clock) {
        super(idFormatter, idGeneratorConfig.getPartitionRetryCount());
        this.idGeneratorConfig = idGeneratorConfig;
        this.partitionResolver = partitionResolverSupplier;
        this.metricRegistry = metricRegistry;
        this.clock = clock;
    }

    protected PartitionAwareNonceGenerator(final IdGeneratorConfig idGeneratorConfig,
                                           final Function<String, Integer> partitionResolverSupplier,
                                           final MetricRegistry metricRegistry,
                                           final Clock clock) {
        this(idGeneratorConfig, partitionResolverSupplier, IdFormatters.secondPrecision(), metricRegistry, clock);
    }

    @Override
    public IdInfo generate(final String namespace) {
        val targetPartitionId = getTargetPartitionId();
        return generateForPartition(namespace, targetPartitionId);
    }

    public IdInfo generateForPartition(final String namespace, final int targetPartitionId) {
        val instant = clock.instant();
        val prefixIdMap = idStore.computeIfAbsent(namespace, k -> getAndInitPartitionIdTrackers(namespace, instant));
        val partitionTracker = getPartitionTracker(prefixIdMap, instant);
        val idCounter = generateAndGetId(
                partitionTracker,
                namespace,
                targetPartitionId);
        val dateTime = IdUtils.getDateTimeFromSeconds(partitionTracker.getInstant().getEpochSecond());
        val id = String.format("%s%s", namespace, getIdFormatter().format(dateTime, IdGeneratorBase.getNODE_ID(), idCounter));
        return new IdInfo(idCounter, dateTime.getMillis());
    }

    private Integer generateAndGetId(final PartitionIdTracker partitionIdTracker,
                                     final String namespace,
                                     final int targetPartitionId) {
        val idPool = partitionIdTracker.getPartition(targetPartitionId);
        Optional<Integer> idOptional = idPool.getNextId();
        while (idOptional.isEmpty()) {
            val idInfo = partitionIdTracker.getIdInfo();
            val dateTime = IdUtils.getDateTimeFromSeconds(idInfo.getTime());
            val idString = String.format("%s%s", namespace, getIdFormatter().format(dateTime, IdGeneratorBase.getNODE_ID(), idInfo.getExponent()));
            val mappedPartitionId = partitionResolver.apply(idString);
            partitionIdTracker.addId(mappedPartitionId, idInfo);
            idOptional = idPool.getNextId();
        }
        return idOptional.get();
    }

    @Override
    public Optional<IdInfo> generateWithConstraints(final IdGenerationRequest request) {
        val instant = clock.instant();
        val prefixIdMap = idStore.computeIfAbsent(request.getPrefix(), k -> getAndInitPartitionIdTrackers(request.getPrefix(), clock.instant()));
        val partitionIdTracker = getPartitionTracker(prefixIdMap, instant);
        return Optional.ofNullable(getRetryer().get(
                () -> {
                    val targetPartitionId = getTargetPartitionId();
                    val idInfo = generateForPartition(request.getPrefix(), targetPartitionId);
                    return new GenerationResult(idInfo,
                            validateId(request.getConstraints(), getIdFromIdInfo(idInfo, request.getPrefix(), getIdFormatter()), request.isSkipGlobal()),
                            request.getDomain(),
                            request.getPrefix());
                }))
                .filter(generationResult -> generationResult.getState() == IdValidationState.VALID)
                .map(GenerationResult::getIdInfo);
    }

    @Override
    protected void retryEventListener(final ExecutionAttemptedEvent<GenerationResult> event) {
        val result = event.getLastResult();
        if (null != result && !result.getState().equals(IdValidationState.VALID)) {
            val instant = clock.instant();
            val prefixIdMap = idStore.computeIfAbsent(result.getNamespace(), k -> getAndInitPartitionIdTrackers(result.getNamespace(), clock.instant()));
            val partitionIdTracker = getPartitionTracker(prefixIdMap, instant);
            val mappedPartitionId = partitionResolver.apply(getIdFromIdInfo(result.getIdInfo(), result.getNamespace(), getIdFormatter()).getId());
            partitionIdTracker.addId(mappedPartitionId, result.getIdInfo());
        }
    }

    protected int getTargetPartitionId() {
        return getSecureRandom().nextInt(idGeneratorConfig.getPartitionCount());
    }

    private int getIdPoolSize(final String namespace) {
        val idPoolSizeOptional = idGeneratorConfig.getNamespaceConfig().stream()
                .filter(namespaceConfig -> namespaceConfig.getNamespace().equals(namespace))
                .map(NamespaceConfig::getIdPoolSizePerBucket)
                .filter(Objects::nonNull)
                .findFirst();
        return idPoolSizeOptional.orElseGet(() -> idGeneratorConfig.getDefaultNamespaceConfig().getIdPoolSizePerPartition());
    }

    private PartitionIdTracker[] getAndInitPartitionIdTrackers(final String namespace, final Instant instant) {
        val partitionTrackerList = new PartitionIdTracker[idGeneratorConfig.getDataStorageLimitInSeconds()];
        for (int i = 0; i<idGeneratorConfig.getDataStorageLimitInSeconds(); i++) {
            partitionTrackerList[i] = new PartitionIdTracker(idGeneratorConfig.getPartitionCount(),
                    getIdPoolSize(namespace), instant, metricRegistry, namespace);
        }
        return partitionTrackerList;
    }

    private int getTimeKey(final Instant instant) {
        val timeInSeconds = instant.getEpochSecond();
        return (int) timeInSeconds % idGeneratorConfig.getDataStorageLimitInSeconds();
    }

    private PartitionIdTracker getPartitionTracker(final PartitionIdTracker[] partitionTrackerList, final Instant instant) {
        val timeKey = getTimeKey(instant);
        val partitionTracker = partitionTrackerList[timeKey];
        if (partitionTracker.getInstant().getEpochSecond() != instant.getEpochSecond()) {
            partitionTracker.reset(instant);
        }
        return partitionTracker;
    }

}
