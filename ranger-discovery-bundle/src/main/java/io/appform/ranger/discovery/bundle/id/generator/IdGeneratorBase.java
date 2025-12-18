package io.appform.ranger.discovery.bundle.id.generator;

import com.google.common.base.Preconditions;
import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeExecutor;
import dev.failsafe.RetryPolicy;
import io.appform.ranger.discovery.bundle.id.Domain;
import io.appform.ranger.discovery.bundle.id.GenerationResult;
import io.appform.ranger.discovery.bundle.id.Id;
import io.appform.ranger.discovery.bundle.id.IdGenerationType;
import io.appform.ranger.discovery.bundle.id.NonceInfo;
import io.appform.ranger.discovery.bundle.id.IdValidationState;
import io.appform.ranger.discovery.bundle.id.constraints.IdValidationConstraint;
import io.appform.ranger.discovery.bundle.id.decorators.IdDecorator;
import io.appform.ranger.discovery.bundle.id.formatter.IdFormatter;
import io.appform.ranger.discovery.bundle.id.formatter.IdFormatters;
import io.appform.ranger.discovery.bundle.id.nonce.NonceGenerator;
import io.appform.ranger.discovery.bundle.id.nonce.RandomNonceGenerator;
import io.appform.ranger.discovery.bundle.id.request.IdGenerationInput;
import io.appform.ranger.discovery.bundle.id.request.IdGenerationRequest;
import lombok.Getter;
import lombok.val;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Base Id Generator
 */
public class IdGeneratorBase {

    @Getter
    private int nodeId;
    private final List<IdValidationConstraint> globalConstraints = new ArrayList<>();
    private final Map<String, Domain> registeredDomains = new ConcurrentHashMap<>(Map.of(Domain.DEFAULT_DOMAIN_NAME, Domain.DEFAULT));
    private final FailsafeExecutor<GenerationResult> randomNonceRetryer;

    protected final NonceGenerator randomNonceGenerator;

    public IdGeneratorBase() {
        this.randomNonceGenerator = new RandomNonceGenerator();
        val retryPolicy = RetryPolicy.<GenerationResult>builder()
                .withMaxAttempts(randomNonceGenerator.readRetryCount())
                .handleIf(throwable -> true)
                .handleResultIf(Objects::isNull)
                .handleResultIf(generationResult -> generationResult.getState() == IdValidationState.INVALID_RETRYABLE)
                .onRetry(randomNonceGenerator::retryEventListener)
                .build();
        this.randomNonceRetryer = Failsafe.with(Collections.singletonList(retryPolicy));
    }

    public final synchronized void cleanUp() {
        globalConstraints.clear();
        registeredDomains.clear();
        nodeId = 0;
    }

    public final void registerDomain(final Domain domain) {
        registeredDomains.put(domain.getDomain(), domain);
    }

    public final synchronized void registerGlobalConstraints(final List<IdValidationConstraint> constraints) {
        Preconditions.checkArgument(null != constraints && !constraints.isEmpty());
        globalConstraints.addAll(constraints);
    }

    public final synchronized void registerDomainSpecificConstraints(
            final String domain,
            final List<IdValidationConstraint> validationConstraints) {
        Preconditions.checkArgument(null != validationConstraints && !validationConstraints.isEmpty());
        registeredDomains.computeIfAbsent(domain, key -> Domain.builder()
                .domain(domain)
                .constraints(validationConstraints)
                .resolution(TimeUnit.MILLISECONDS)
                .build());
    }

    public final Id getIdFromIdInfo(final NonceInfo nonceInfo, final String namespace, final String suffix, final int idGenerators) {
        val dateTime = new DateTime(nonceInfo.getTime());
        val idFormatter = getIdFormatter(idGenerators);
        val id = String.format("%s%s", namespace, idFormatter.format(dateTime, getNodeId(), nonceInfo.getExponent(), suffix, idGenerators));
        return Id.builder()
                .id(id)
                .exponent(nonceInfo.getExponent())
                .generatedDate(dateTime.toDate())
                .node(getNodeId())
                .build();
    }

    public final IdValidationState validateId(final List<IdValidationConstraint> inConstraints, final Id id, final boolean skipGlobal) {
        // First evaluate global constraints
        val failedGlobalConstraint
                = skipGlobal
                ? null
                : globalConstraints.stream()
                .filter(constraint -> !constraint.isValid(id))
                .findFirst()
                .orElse(null);
        if (null != failedGlobalConstraint) {
            return failedGlobalConstraint.failFast()
                    ? IdValidationState.INVALID_NON_RETRYABLE
                    : IdValidationState.INVALID_RETRYABLE;
        }
        // Evaluate param constraints
        val failedLocalConstraint
                = null == inConstraints
                ? null
                : inConstraints.stream()
                .filter(constraint -> !constraint.isValid(id))
                .findFirst()
                .orElse(null);
        if (null != failedLocalConstraint) {
            return failedLocalConstraint.failFast()
                    ? IdValidationState.INVALID_NON_RETRYABLE
                    : IdValidationState.INVALID_RETRYABLE;
        }
        return IdValidationState.VALID;
    }

    /**
     * Generate id with given namespace
     *
     * @param namespace String namespace for ID to be generated
     * @return Generated Id
     */
    public final Id generate(final String namespace, final String suffix, final int idGenerators) {
        val idInfo = getNonceGenerator(idGenerators).generate(namespace);
        return getIdFromIdInfo(idInfo, namespace, suffix, idGenerators);
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
    public final Optional<Id> generateWithConstraints(final String namespace,
                                                      final String suffix,
                                                      final String domain,
                                                      final boolean skipGlobal,
                                                      final int idGenerators) {
        val registeredDomain = registeredDomains.getOrDefault(domain, Domain.DEFAULT);
        val request = IdGenerationRequest.builder()
                .prefix(namespace)
                .suffix(suffix)
                .constraints(registeredDomain.getConstraints())
                .skipGlobal(skipGlobal)
                .domain(registeredDomain.getDomain())
                .idGenerationType(idGenerators)
                .build();
        return generateWithConstraints(request);
    }

    public final Optional<Id> generateWithConstraints(final IdGenerationRequest request) {
        val idGenerators = request.getIdGenerationType();
        val nonceGenerator = getNonceGenerator(idGenerators);
        val failSafeRetryer = getFailSafeRetryer(idGenerators);
        val domain = request.getDomain() != null ? registeredDomains.getOrDefault(request.getDomain(), Domain.DEFAULT) : Domain.DEFAULT;
        val idGenerationInput = IdGenerationInput.builder()
                .prefix(request.getPrefix())
                .suffix(request.getSuffix())
                .domain(domain)
                .build();
        return Optional.ofNullable(failSafeRetryer.get(
                        () -> {
                            val idInfoOptional = nonceGenerator.generateWithConstraints(idGenerationInput);
                            val id = getIdFromIdInfo(idInfoOptional, request.getPrefix(), request.getSuffix(), idGenerators);
                            return new GenerationResult(
                                    idInfoOptional,
                                    validateId(request.getConstraints(), id, request.isSkipGlobal()),
                                    domain);
                        }))
                .filter(generationResult -> generationResult.getState() == IdValidationState.VALID)
                .map(generationResult -> this.getIdFromIdInfo(generationResult.getNonceInfo(), request.getPrefix(), request.getSuffix(), idGenerators));
    }

    public final void setNodeId(int nodeId) {
        if (this.nodeId > 0) {
            throw new RuntimeException("Node ID already set");
        }
        this.nodeId = nodeId;
    }
    
    private IdFormatter getIdFormatter(final int idGenerators) {
        return IdGenerationType.FORMATTER_VALUE_MAP.get(idGenerators);
    }
    
    private NonceGenerator getNonceGenerator(final int idGenerators) {
        if (IdGenerationType.FORMATTER_VALUE_MAP.containsKey(idGenerators) && IdGenerationType.FORMATTER_VALUE_MAP
                .get(idGenerators) == IdFormatters.randomNonce()) {
            return randomNonceGenerator;
        }
        return randomNonceGenerator;
    }
    
    private FailsafeExecutor<GenerationResult> getFailSafeRetryer(final int idGenerators) {
        if (IdGenerationType.FORMATTER_VALUE_MAP.containsKey(idGenerators) && IdGenerationType.FORMATTER_VALUE_MAP
                .get(idGenerators) == IdFormatters.randomNonce()) {
            return randomNonceRetryer;
        }
        return randomNonceRetryer;
    }
}
