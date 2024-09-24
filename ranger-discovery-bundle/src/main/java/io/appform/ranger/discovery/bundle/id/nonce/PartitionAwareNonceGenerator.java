package io.appform.ranger.discovery.bundle.id.nonce;

import com.codahale.metrics.MetricRegistry;
import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeExecutor;
import dev.failsafe.RetryPolicy;
import io.appform.ranger.discovery.bundle.id.Domain;
import io.appform.ranger.discovery.bundle.id.GenerationResult;
import io.appform.ranger.discovery.bundle.id.IdInfo;
import io.appform.ranger.discovery.bundle.id.IdUtils;
import io.appform.ranger.discovery.bundle.id.IdValidationState;
import io.appform.ranger.discovery.bundle.id.PartitionIdTracker;
import io.appform.ranger.discovery.bundle.id.config.IdGeneratorConfig;
import io.appform.ranger.discovery.bundle.id.config.NamespaceConfig;
import io.appform.ranger.discovery.bundle.id.constraints.IdValidationConstraint;
import io.appform.ranger.discovery.bundle.id.formatter.IdFormatter;
import io.appform.ranger.discovery.bundle.id.formatter.IdFormatters;
import io.appform.ranger.discovery.bundle.id.generator.IdGeneratorBase;
import io.appform.ranger.discovery.bundle.id.request.IdGenerationRequest;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.joda.time.DateTime;

import java.time.Clock;
import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;


@SuppressWarnings("unused")
@Slf4j
@Getter
public class PartitionAwareNonceGenerator extends NonceGeneratorBase {
    private final FailsafeExecutor<GenerationResult> RETRYER;
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
        super(idFormatter);
        this.idGeneratorConfig = idGeneratorConfig;
        this.partitionResolver = partitionResolverSupplier;
        this.metricRegistry = metricRegistry;
        this.clock = clock;
        RetryPolicy<GenerationResult> retryPolicy = RetryPolicy.<GenerationResult>builder()
                .withMaxAttempts(idGeneratorConfig.getPartitionRetryCount())
                .handleIf(throwable -> true)
                .handleResultIf(Objects::isNull)
                .handleResultIf(generationResult -> generationResult.getState() == IdValidationState.INVALID_RETRYABLE)
                .onRetry(event -> {
                    val res = event.getLastResult();
                    if (null != res && !res.getState().equals(IdValidationState.VALID)) {
                        reAddId(res.getNamespace(), res.getIdInfo());
                    }
                })
                .build();
        RETRYER = Failsafe.with(Collections.singletonList(retryPolicy));
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
        val idCounter = generateForAllPartitions(
                partitionTracker,
                namespace,
                targetPartitionId);
        val dateTime = getDateTimeFromTime(partitionTracker.getInstant().getEpochSecond());
        val id = String.format("%s%s", namespace, getIdFormatter().format(dateTime, IdGeneratorBase.getNODE_ID(), idCounter));
        return new IdInfo(idCounter, partitionTracker.getInstant().getEpochSecond());
    }

    private Integer generateForAllPartitions(final PartitionIdTracker partitionIdTracker,
                                             final String namespace,
                                             final int targetPartitionId) {
        val idPool = partitionIdTracker.getPartition(targetPartitionId);
        Optional<Integer> idOptional = idPool.getNextId();
        while (idOptional.isEmpty()) {
            val idInfo = partitionIdTracker.getIdInfo();
            val dateTime = IdUtils.getDateTimeFromSeconds(idInfo.getTime());
            val txnId = String.format("%s%s", namespace, getIdFormatter().format(dateTime, IdGeneratorBase.getNODE_ID(), idInfo.getExponent()));
            val mappedPartitionId = partitionResolver.apply(txnId);
            partitionIdTracker.addId(mappedPartitionId, idInfo);
            idOptional = idPool.getNextId();
        }
        return idOptional.get();
    }

    /**
     * Generate id that matches all passed constraints.
     * NOTE: There are performance implications for this.
     * The evaluation of constraints will take its toll on id generation rates.
     *
     * @param namespace  String namespace
     * @param domain     Domain for constraint selection
     * @param skipGlobal Skip global constrains and use only passed ones
     * @return ID if it could be generated
     */
    @Override
    public Optional<IdInfo> generateWithConstraints(final String namespace, final String domain, final boolean skipGlobal) {
        return generateWithConstraints(IdGenerationRequest.builder()
                .prefix(namespace)
                .domain(domain)
                .constraints(getREGISTERED_DOMAINS().getOrDefault(domain, Domain.DEFAULT).getConstraints())
                .skipGlobal(skipGlobal)
                .build());
    }

    @Override
    public Optional<IdInfo> generateWithConstraints(final String namespace,
                                                    final List<IdValidationConstraint> inConstraints,
                                                    final boolean skipGlobal) {
        return generateWithConstraints(IdGenerationRequest.builder()
                .prefix(namespace)
                .constraints(inConstraints)
                .skipGlobal(skipGlobal)
                .build());
    }

    @Override
    public Optional<IdInfo> generateWithConstraints(final IdGenerationRequest request) {
        val instant = clock.instant();
        val prefixIdMap = idStore.computeIfAbsent(request.getPrefix(), k -> getAndInitPartitionIdTrackers(request.getPrefix(), clock.instant()));
        val partitionIdTracker = getPartitionTracker(prefixIdMap, instant);
        return Optional.ofNullable(RETRYER.get(
                () -> {
                    val targetPartitionId = getTargetPartitionId();
                    val idInfo = generateForPartition(request.getPrefix(), targetPartitionId);
                    return GenerationResult.builder()
                            .idInfo(idInfo)
                            .state(validateId(request.getConstraints(), getIdFromIdInfo(idInfo, request.getPrefix(), getIdFormatter()), request.isSkipGlobal()))
                            .domain(request.getDomain())
                            .build();
                }))
                .filter(generationResult -> generationResult.getState() == IdValidationState.VALID)
                .map(GenerationResult::getIdInfo);
    }

    protected int getTargetPartitionId() {
        return getSECURE_RANDOM().nextInt(idGeneratorConfig.getPartitionCount());
    }

    private int getIdPoolSize(String namespace) {
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

    @Override
    public DateTime getDateTimeFromTime(final long time) {
        return IdUtils.getDateTimeFromSeconds(time);
    }

    private void reAddId(final String namespace, final IdInfo idInfo) {
        val instant = clock.instant();
        val prefixIdMap = idStore.computeIfAbsent(namespace, k -> getAndInitPartitionIdTrackers(namespace, clock.instant()));
        val partitionIdTracker = getPartitionTracker(prefixIdMap, instant);
        val mappedPartitionId = partitionResolver.apply(getIdFromIdInfo(idInfo, namespace, getIdFormatter()).getId());
        partitionIdTracker.addId(mappedPartitionId, idInfo);
    }

}
