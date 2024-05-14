package io.appform.ranger.discovery.bundle.id;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeExecutor;
import dev.failsafe.RetryPolicy;
import io.appform.ranger.discovery.bundle.id.constraints.PartitionValidationConstraint;
import io.appform.ranger.discovery.bundle.id.formatter.IdFormatter;
import io.appform.ranger.discovery.bundle.id.formatter.IdFormatters;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Pattern;

/**
 * Distributed Id Generation
 */
@SuppressWarnings("unused")
@Slf4j
public class PartitionAwareIdGenerator {

    private static final int MINIMUM_ID_LENGTH = 22;
    protected static final SecureRandom SECURE_RANDOM = new SecureRandom(Long.toBinaryString(System.currentTimeMillis()).getBytes());
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormat.forPattern("yyMMddHHmmss");
    private final RetryPolicy<Integer> RETRY_POLICY = RetryPolicy.<Integer>builder()
            .withMaxAttempts(readRetryCount())
            .handleIf(throwable -> true)
            .handleResultIf(Objects::isNull)
            .build();
    protected final FailsafeExecutor<Integer> RETRIER = Failsafe.with(Collections.singletonList(RETRY_POLICY));
    private static final Pattern PATTERN = Pattern.compile("(.*)([0-9]{12})([0-9]{4})([0-9]{6})");
    private static final List<PartitionValidationConstraint> GLOBAL_CONSTRAINTS = new ArrayList<>();
    private static final Map<String, List<PartitionValidationConstraint>> DOMAIN_SPECIFIC_CONSTRAINTS = new HashMap<>();
    protected static final int NODE_ID = IdGenerator.getNodeId();
    @Getter
    private final Map<String, Map<Long, PartitionIdTracker>> idStore = new ConcurrentHashMap<>();
    protected final IdFormatter idFormatter;
    protected final Function<String, Integer> partitionResolver;
    protected final int partitionCount;

/*  dataStore Structure
    {
        prefix: {
            timestamp: {
                partitions: [
                {
                    ids: [],
                    pointer: <int>
                },
                {
                    ids: [],
                    pointer: <int>
                } ...
            ],
                counter: <int>
            }
        }
    }
 */

    public PartitionAwareIdGenerator(final int partitionSize,
                                     final Function<String, Integer> partitionResolverSupplier) {
        partitionCount = partitionSize;
        partitionResolver = partitionResolverSupplier;
        idFormatter = IdFormatters.distributed();
        val executorService = Executors.newScheduledThreadPool(1);
        executorService.scheduleWithFixedDelay(
                this::deleteExpiredKeys,
                Constants.ID_DELETION_DELAY_IN_SECONDS,
                Constants.ID_DELETION_DELAY_IN_SECONDS,
                TimeUnit.SECONDS);
    }

    public PartitionAwareIdGenerator(final int partitionSize,
                                     final Function<String, Integer> partitionResolverSupplier,
                                     final IdFormatter idFormatterInstance) {
        partitionCount = partitionSize;
        partitionResolver = partitionResolverSupplier;
        idFormatter = idFormatterInstance;
    }

    public synchronized void registerGlobalConstraints(final PartitionValidationConstraint... constraints) {
        registerGlobalConstraints(ImmutableList.copyOf(constraints));
    }

    public synchronized void registerGlobalConstraints(final List<PartitionValidationConstraint> constraints) {
        Preconditions.checkArgument(null != constraints && !constraints.isEmpty());
        GLOBAL_CONSTRAINTS.addAll(constraints);
    }

    public synchronized void registerDomainSpecificConstraints(
            final String domain,
            final PartitionValidationConstraint... validationConstraints) {
        registerDomainSpecificConstraints(domain, ImmutableList.copyOf(validationConstraints));
    }

    public synchronized void registerDomainSpecificConstraints(
            final String domain,
            final List<PartitionValidationConstraint> validationConstraints) {
        Preconditions.checkArgument(null != validationConstraints && !validationConstraints.isEmpty());
        DOMAIN_SPECIFIC_CONSTRAINTS.computeIfAbsent(domain, key -> new ArrayList<>())
                .addAll(validationConstraints);
    }

    /**
     * Generate id with given prefix
     *
     * @param prefix String prefix for ID to be generated
     * @return Generated Id
     */
    public Id generate(final String prefix) {
        val targetPartitionId = getTargetPartitionId();
        return generateForPartition(prefix, targetPartitionId);
    }

    public Id generateForPartition(final String prefix, final int targetPartitionId) {
        val prefixIdMap = idStore.computeIfAbsent(prefix, k -> new ConcurrentHashMap<>());
        val currentTimestamp = new DateTime();
        val timeKey = currentTimestamp.getMillis() / 1000;
        val idCounter = generateForAllPartitions(
                prefixIdMap.computeIfAbsent(timeKey, key -> new PartitionIdTracker(partitionCount)),
                prefix,
                currentTimestamp,
                targetPartitionId);
        val id = String.format("%s%s", prefix, idFormatter.format(currentTimestamp, NODE_ID, idCounter));
        return Id.builder()
                .id(id)
                .exponent(idCounter)
                .generatedDate(currentTimestamp.toDate())
                .node(NODE_ID)
                .build();
    }

    private int generateForAllPartitions(final PartitionIdTracker partitionIdTracker,
                                         final String prefix,
                                         final DateTime timestamp,
                                         final int targetPartitionId) {
        val idPool = partitionIdTracker.getPartition(targetPartitionId);
        int idIdx = idPool.getPointer().getAndIncrement();
//            ToDo: Add Retry Limit
        while (idPool.getIdList().size() <= idIdx) {
            val counterValue = partitionIdTracker.getNextIdCounter().getAndIncrement();
            val txnId = String.format("%s%s", prefix, idFormatter.format(timestamp, NODE_ID, counterValue));
            val mappedPartitionId = partitionResolver.apply(txnId);
            partitionIdTracker.getPartition(mappedPartitionId).getIdList().add(counterValue);
        }
        return idPool.getId(idIdx);
    }

    /**
     * Generate id that matches all passed constraints.
     * NOTE: There are performance implications for this.
     * The evaluation of constraints will take it's toll on ID generation rates.
     *
     * @param prefix String prefix
     * @param domain Domain for constraint selection
     * @return Return generated id or empty if it was impossible to satisfy constraints and generate
     */
    public Optional<Id> generateWithConstraints(final String prefix, final String domain) {
        return generateWithConstraints(prefix, domain, true);
    }

    /**
     * Generate id that matches all passed constraints.
     * NOTE: There are performance implications for this.
     * The evaluation of constraints will take it's toll on id generation rates.
     *
     * @param prefix     String prefix
     * @param domain     Domain for constraint selection
     * @param skipGlobal Skip global constrains and use only passed ones
     * @return Id if it could be generated
     */
    public Optional<Id> generateWithConstraints(final String prefix, final String domain, final boolean skipGlobal) {
        val targetPartitionId = getTargetPartitionId(DOMAIN_SPECIFIC_CONSTRAINTS.getOrDefault(domain, Collections.emptyList()), skipGlobal);
        return targetPartitionId.map(id -> generateForPartition(prefix, id));
    }

    public Optional<Id> generateWithConstraints(final String prefix,
                                                final List<PartitionValidationConstraint> inConstraints,
                                                final boolean skipGlobal) {
        val targetPartitionId = getTargetPartitionId(inConstraints, skipGlobal);
        return targetPartitionId.map(id -> generateForPartition(prefix, id));
    }

    /**
     * Generate id by parsing given string
     *
     * @param idString String idString
     * @return Id if it could be generated
     */
    public Optional<Id> parse(final String idString) {
        if (idString == null
                || idString.length() < MINIMUM_ID_LENGTH) {
            return Optional.empty();
        }
        try {
            val matcher = PATTERN.matcher(idString);
            if (matcher.find()) {
                return Optional.of(Id.builder()
                        .id(idString)
                        .node(Integer.parseInt(matcher.group(3)))
                        .exponent(Integer.parseInt(matcher.group(4)))
                        .generatedDate(DATE_TIME_FORMATTER.parseDateTime(matcher.group(2)).toDate())
                        .build());
            }
            return Optional.empty();
        }
        catch (Exception e) {
            log.warn("Could not parse idString {}", e.getMessage());
            return Optional.empty();
        }
    }

    private int getTargetPartitionId() {
        return SECURE_RANDOM.nextInt(partitionCount);
    }

    private Optional<Integer> getTargetPartitionId(final List<PartitionValidationConstraint> inConstraints, final boolean skipGlobal) {
        return Optional.ofNullable(
                RETRIER.get(() -> SECURE_RANDOM.nextInt(partitionCount)))
                .filter(key -> validateId(inConstraints, key, skipGlobal));
    }

    protected boolean validateId(final List<PartitionValidationConstraint> inConstraints,
                                 final int partitionId,
                                 final boolean skipGlobal) {
        //First evaluate global constraints
        val failedGlobalConstraint
                = skipGlobal
                  ? null
                  : GLOBAL_CONSTRAINTS.stream()
                          .filter(constraint -> !constraint.isValid(partitionId))
                          .findFirst()
                          .orElse(null);
        if (null != failedGlobalConstraint) {
            return false;
        }
        //Evaluate param constraints
        val failedLocalConstraint
                = null == inConstraints
                  ? null
                  : inConstraints.stream()
                          .filter(constraint -> !constraint.isValid(partitionId))
                          .findFirst()
                          .orElse(null);
        return null == failedLocalConstraint;
    }

    protected int readRetryCount() {
        try {
//            Make it config
            val count = Integer.parseInt(System.getenv().getOrDefault("NUM_ID_GENERATION_RETRIES", "512"));
            if (count <= 0) {
                throw new IllegalArgumentException(
                        "Negative number of retries does not make sense. Please set a proper value for " +
                                "NUM_ID_GENERATION_RETRIES");
            }
            return count;
        }
        catch (NumberFormatException e) {
            throw new IllegalArgumentException("Please provide a valid positive integer for NUM_ID_GENERATION_RETRIES");
        }
    }

    private void deleteExpiredKeys() {
        val timeThreshold = DateTime.now().getMillis() / 1000 - Constants.DELETION_THRESHOLD_IN_SECONDS;
        for (val entry : idStore.entrySet()) {
            entry.getValue().entrySet().removeIf(partitionIdTrackerEntry -> partitionIdTrackerEntry.getKey() < timeThreshold);
        }
    }

}
