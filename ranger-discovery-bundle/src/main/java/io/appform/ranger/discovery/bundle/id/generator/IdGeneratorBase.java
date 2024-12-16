package io.appform.ranger.discovery.bundle.id.generator;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeExecutor;
import dev.failsafe.RetryPolicy;
import io.appform.ranger.discovery.bundle.id.Domain;
import io.appform.ranger.discovery.bundle.id.GenerationResult;
import io.appform.ranger.discovery.bundle.id.Id;
import io.appform.ranger.discovery.bundle.id.IdUtils;
import io.appform.ranger.discovery.bundle.id.IdValidationState;
import io.appform.ranger.discovery.bundle.id.constraints.IdValidationConstraint;
import io.appform.ranger.discovery.bundle.id.formatter.IdFormatter;
import io.appform.ranger.discovery.bundle.id.formatter.IdFormatters;
import io.appform.ranger.discovery.bundle.id.nonce.NonceGeneratorBase;
import io.appform.ranger.discovery.bundle.id.request.IdGenerationRequest;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Base Id Generator
 */
@SuppressWarnings("unused")
@Slf4j
public class IdGeneratorBase {

    @Getter
    private static int NODE_ID;
    private final List<IdValidationConstraint> globalConstraints = new ArrayList<>();
    private final Map<String, Domain> registeredDomains = new ConcurrentHashMap<>(Map.of(Domain.DEFAULT_DOMAIN_NAME, Domain.DEFAULT));
    private final FailsafeExecutor<GenerationResult> retryer;

    protected final IdFormatter idFormatter;
    protected final NonceGeneratorBase nonceGenerator;

    public static void initialize(int node) {
        NODE_ID = node;
    }

    public IdGeneratorBase(final IdFormatter idFormatter,
                           final NonceGeneratorBase nonceGenerator) {
        this.idFormatter = idFormatter;
        this.nonceGenerator = nonceGenerator;
        val retryPolicy = RetryPolicy.<GenerationResult>builder()
                .withMaxAttempts(nonceGenerator.readRetryCount())
                .handleIf(throwable -> true)
                .handleResultIf(Objects::isNull)
                .handleResultIf(generationResult -> generationResult.getState() == IdValidationState.INVALID_RETRYABLE)
                .onRetry(nonceGenerator::retryEventListener)
                .build();
        this.retryer = Failsafe.with(Collections.singletonList(retryPolicy));
    }

    public final synchronized void cleanUp() {
        globalConstraints.clear();
        registeredDomains.clear();
    }

    public final void registerDomain(final Domain domain) {
        registeredDomains.put(domain.getDomain(), domain);
    }

    public final synchronized void registerGlobalConstraints(final IdValidationConstraint... constraints) {
        registerGlobalConstraints(ImmutableList.copyOf(constraints));
    }

    public final synchronized void registerGlobalConstraints(final List<IdValidationConstraint> constraints) {
        Preconditions.checkArgument(null != constraints && !constraints.isEmpty());
        globalConstraints.addAll(constraints);
    }

    public final synchronized void registerDomainSpecificConstraints(
            final String domain,
            final IdValidationConstraint... validationConstraints) {
        registerDomainSpecificConstraints(domain, ImmutableList.copyOf(validationConstraints));
    }

    public final synchronized void registerDomainSpecificConstraints(
            final String domain,
            final List<IdValidationConstraint> validationConstraints) {
        Preconditions.checkArgument(null != validationConstraints && !validationConstraints.isEmpty());
        registeredDomains.computeIfAbsent(domain, key -> Domain.builder()
                .domain(domain)
                .constraints(validationConstraints)
                .idFormatter(IdFormatters.original())
                .resolution(TimeUnit.MILLISECONDS)
                .build());
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
    public final Id generate(final String namespace) {
        val idInfo = nonceGenerator.generate(namespace);
        return IdUtils.getIdFromIdInfo(idInfo, namespace, idFormatter);
    }

//    Should we support this?
    public final Id generate(final String namespace, final IdFormatter idFormatter) {
        val idInfo = nonceGenerator.generate(namespace);
        return IdUtils.getIdFromIdInfo(idInfo, namespace, idFormatter);
    }

    /**
     * Generate id that matches all passed constraints.
     * NOTE: There are performance implications for this.
     * The evaluation of constraints will take its toll on id generation rates.
     *
     * @param namespace     String namespace
     * @param domain     Domain for constraint selection
     * @param skipGlobal Skip global constrains and use only passed ones
     * @return ID if it could be generated
     */
    public final Optional<Id> generateWithConstraints(final String namespace, final String domain, final boolean skipGlobal) {
        val registeredDomain = registeredDomains.getOrDefault(domain, Domain.DEFAULT);
        val request = IdGenerationRequest.builder()
                .prefix(namespace)
                .constraints(registeredDomain.getConstraints())
                .skipGlobal(skipGlobal)
                .domain(registeredDomain.getDomain())
                .idFormatter(registeredDomain.getIdFormatter())
                .build();
        return generateWithConstraints(request);
    }

    public final Optional<Id> generateWithConstraints(final String namespace,
                                                      final List<IdValidationConstraint> inConstraints,
                                                      final boolean skipGlobal) {
        val request = IdGenerationRequest.builder()
                .prefix(namespace)
                .constraints(inConstraints)
                .skipGlobal(skipGlobal)
                .idFormatter(nonceGenerator.getIdFormatter())
                .build();
        return generateWithConstraints(request);
    }

    public final Optional<Id> generateWithConstraints(final IdGenerationRequest request) {
        return Optional.ofNullable(retryer.get(
                        () -> {
                            val idInfoOptional = nonceGenerator.generateWithConstraints(request);
                            val id = IdUtils.getIdFromIdInfo(idInfoOptional, request.getPrefix(), request.getIdFormatter());
                            return new GenerationResult(
                                    idInfoOptional,
                                    validateId(request.getConstraints(), id, request.isSkipGlobal()),
                                    request.getDomainInstance(),
                                    request.getPrefix());
                        }))
                .filter(generationResult -> generationResult.getState() == IdValidationState.VALID)
                .map(generationResult -> IdUtils.getIdFromIdInfo(generationResult.getIdInfo(), request.getPrefix(), request.getIdFormatter()));
    }

}
