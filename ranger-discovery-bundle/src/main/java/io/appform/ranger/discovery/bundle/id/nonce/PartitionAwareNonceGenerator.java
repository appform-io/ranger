package io.appform.ranger.discovery.bundle.id.nonce;

import com.codahale.metrics.MetricRegistry;
import dev.failsafe.event.ExecutionAttemptedEvent;
import io.appform.ranger.discovery.bundle.id.GenerationResult;
import io.appform.ranger.discovery.bundle.id.NonceInfo;
import io.appform.ranger.discovery.bundle.id.IdUtils;
import io.appform.ranger.discovery.bundle.id.IdValidationState;
import io.appform.ranger.discovery.bundle.id.PartitionIdTracker;
import io.appform.ranger.discovery.bundle.id.config.IdGeneratorConfig;
import io.appform.ranger.discovery.bundle.id.config.NamespaceConfig;
import io.appform.ranger.discovery.bundle.id.formatter.IdFormatter;
import io.appform.ranger.discovery.bundle.id.formatter.IdFormatters;
import io.appform.ranger.discovery.bundle.id.request.IdGenerationInput;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.security.SecureRandom;
import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;


@SuppressWarnings("unused")
@Slf4j
@Getter
public class PartitionAwareNonceGenerator extends NonceGenerator {
    private final SecureRandom secureRandom = new SecureRandom(Long.toBinaryString(System.currentTimeMillis()).getBytes());
    private final Map<String, PartitionIdTracker[]> idStore = new ConcurrentHashMap<>();
    private final Function<String, Integer> partitionResolver;
    private final IdGeneratorConfig idGeneratorConfig;
    private final MetricRegistry metricRegistry;
    private final Clock clock;

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
    }

    protected PartitionAwareNonceGenerator(final IdGeneratorConfig idGeneratorConfig,
                                           final Function<String, Integer> partitionResolverSupplier,
                                           final MetricRegistry metricRegistry,
                                           final Clock clock) {
        this(idGeneratorConfig, partitionResolverSupplier, IdFormatters.secondPrecision(), metricRegistry, clock);
    }

    @Override
    public NonceInfo generate(final String namespace) {
        val targetPartitionId = getTargetPartitionId();
        return generateForPartition(namespace, targetPartitionId);
    }

    public NonceInfo generateForPartition(final String namespace, final int targetPartitionId) {
        val instant = clock.instant();
        val prefixIdMap = idStore.computeIfAbsent(namespace, k -> getAndInitPartitionIdTrackers(namespace, instant));
        val partitionTracker = getPartitionTracker(prefixIdMap, instant);
        val idCounter = generateAndGetId(
                partitionTracker,
                namespace,
                targetPartitionId);
        return new NonceInfo(idCounter, partitionTracker.getInstant().getEpochSecond() * 1000L);
    }

    @Override
    public NonceInfo generateWithConstraints(final IdGenerationInput request) {
        val instant = clock.instant();
        val prefixIdMap = idStore.computeIfAbsent(request.getPrefix(), k -> getAndInitPartitionIdTrackers(request.getPrefix(), clock.instant()));
        val partitionIdTracker = getPartitionTracker(prefixIdMap, instant);
        val targetPartitionId = getTargetPartitionId();
        return generateForPartition(request.getPrefix(), targetPartitionId);
    }

    @Override
    public void retryEventListener(final ExecutionAttemptedEvent<GenerationResult> event) {
        val result = event.getLastResult();
        if (null != result && !result.getState().equals(IdValidationState.VALID)) {
            val instant = clock.instant();
            val prefixIdMap = idStore.computeIfAbsent(result.getNamespace(), k -> getAndInitPartitionIdTrackers(result.getNamespace(), clock.instant()));
            val partitionIdTracker = getPartitionTracker(prefixIdMap, instant);
            val mappedPartitionId = resolvePartitionId(result.getNamespace(), result.getNonceInfo());
            partitionIdTracker.addId(mappedPartitionId, result.getNonceInfo());
        }
    }

    protected int getTargetPartitionId() {
        return getSecureRandom().nextInt(idGeneratorConfig.getPartitionCount());
    }

    private Integer generateAndGetId(final PartitionIdTracker partitionIdTracker,
                                     final String namespace,
                                     final int targetPartitionId) {
        val idPool = partitionIdTracker.getPartition(targetPartitionId);
        Optional<Integer> idOptional = idPool.getNextId();
        while (idOptional.isEmpty()) {
            val nonceInfo = partitionIdTracker.getNonceInfo();
            val mappedPartitionId = resolvePartitionId(namespace, nonceInfo);
            partitionIdTracker.addId(mappedPartitionId, nonceInfo);
            idOptional = idPool.getNextId();
        }
        return idOptional.get();
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

    private int resolvePartitionId(final String namespace, final NonceInfo nonceInfo) {
        val id = IdUtils.getIdFromNonceInfo(nonceInfo, namespace, getIdFormatter());
        val idString = id.getId();
        return partitionResolver.apply(idString);
    }

}
