package io.appform.ranger.discovery.bundle.id.v2.generator;

import com.google.common.base.Preconditions;
import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeExecutor;
import dev.failsafe.RetryPolicy;
import io.appform.ranger.discovery.bundle.id.formatter.IdFormatter;
import io.appform.ranger.discovery.bundle.id.v2.Domain;
import io.appform.ranger.discovery.bundle.id.Id;
import io.appform.ranger.discovery.bundle.id.IdValidationState;
import io.appform.ranger.discovery.bundle.id.NonceInfo;
import io.appform.ranger.discovery.bundle.id.constraints.IdValidationConstraint;
import io.appform.ranger.discovery.bundle.id.nonce.NonceGenerator;
import io.appform.ranger.discovery.bundle.id.v2.GenerationResult;
import io.appform.ranger.discovery.bundle.id.v2.request.IdGenerationInput;
import io.appform.ranger.discovery.bundle.id.v2.request.IdGenerationRequest;
import io.appform.ranger.discovery.bundle.util.NodeUtils;
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

/**
 * Base Id Generator
 */
public class IdGeneratorBase {

    @Getter
    private int nodeId;
    private final List<IdValidationConstraint> globalConstraints = new ArrayList<>();
    private final Map<String, Domain> registeredDomains = new ConcurrentHashMap<>(Map.of(Domain.DEFAULT_DOMAIN_NAME, Domain.DEFAULT));
    private final FailsafeExecutor<GenerationResult> retryer;
    private final String ID_REGEX = "^[a-zA-Z]+$";
    
    protected final NonceGenerator nonceGenerator;

    protected IdGeneratorBase(final NonceGenerator nonceGenerator) {
        this.nonceGenerator = nonceGenerator;
        val retryPolicy = RetryPolicy.<GenerationResult>builder()
                .withMaxAttempts(nonceGenerator.readRetryCount())
                .handleIf(throwable -> true)
                .handleResultIf(Objects::isNull)
                .handleResultIf(generationResult -> generationResult.getState() == IdValidationState.INVALID_RETRYABLE)
                .onRetry(nonceGenerator::retryEventListenerV2)
                .build();
        this.nodeId = NodeUtils.getNode();
        this.retryer = Failsafe.with(Collections.singletonList(retryPolicy));
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

    public final Id getIdFromIdInfo(final NonceInfo nonceInfo, final String namespace, final String suffix, final IdFormatter idFormatter) {
        validateIdRegex(namespace);
        val dateTime = new DateTime(nonceInfo.getTime());
        val id = String.format("%s%s", namespace, idFormatter.format(dateTime, getNodeId(), nonceInfo.getExponent(), suffix != null ? suffix : ""));
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
     * @param suffix String suffix to be appended at end
     * @param idFormatter Formatter to use for id generation
     * @return Generated Id
     */
    public final Id generate(final String namespace,
                             final String suffix,
                             final IdFormatter idFormatter) {
        val idInfo = nonceGenerator.generate(namespace);
        return getIdFromIdInfo(idInfo, namespace, suffix, idFormatter);
    }
    
    /**
     * Generate id that matches all passed constraints.
     * NOTE: There are performance implications for this.
     * The evaluation of constraints will take its toll on id generation rates.
     *
     * @param namespace   String namespace
     * @param suffix      String suffix
     * @param domain      Domain for constraint selection
     * @param idFormatter Formatter to use for id generation
     * @param skipGlobal  Skip global constrains and use only passed ones
     * @return ID if it could be generated
     */
    public final Optional<Id> generateWithConstraints(final String namespace,
                                                      final String suffix,
                                                      final String domain,
                                                      final IdFormatter idFormatter,
                                                      final boolean skipGlobal) {
        val registeredDomain = registeredDomains.getOrDefault(domain, Domain.DEFAULT);
        val request = IdGenerationRequest.builder()
                .prefix(namespace)
                .suffix(suffix)
                .constraints(registeredDomain.getConstraints())
                .skipGlobal(skipGlobal)
                .domain(registeredDomain.getDomain())
                .idFormatter(idFormatter)
                .build();
        return generateWithConstraints(request);
    }

    public final Optional<Id> generateWithConstraints(final IdGenerationRequest request) {
        val domain = request.getDomain() != null ? registeredDomains.getOrDefault(request.getDomain(), Domain.DEFAULT) : Domain.DEFAULT;
        val idGenerationInput = IdGenerationInput.builder()
                .prefix(request.getPrefix())
                .suffix(request.getSuffix())
                .domain(domain)
                .build();
        return Optional.ofNullable(retryer.get(
                        () -> {
                            val idInfoOptional = nonceGenerator.generateWithConstraints(idGenerationInput);
                            val id = getIdFromIdInfo(idInfoOptional, request.getPrefix(), request.getSuffix(), request.getIdFormatter());
                            return new GenerationResult(
                                    idInfoOptional,
                                    validateId(request.getConstraints(), id, request.isSkipGlobal()),
                                    domain);
                        }))
                .filter(generationResult -> generationResult.getState() == IdValidationState.VALID)
                .map(generationResult -> this.getIdFromIdInfo(generationResult.getNonceInfo(), request.getPrefix(), request.getSuffix(), request.getIdFormatter()));
    }

    public final void setNodeId(int nodeId) {
        if (this.nodeId > 0) {
            throw new RuntimeException("Node ID already set");
        }
        this.nodeId = nodeId;
    }
    
    private void validateIdRegex(final String namespace) {
        Preconditions.checkArgument(
                namespace.matches(ID_REGEX),
                "Prefix does not match the required regex: " + ID_REGEX);
    }
}
