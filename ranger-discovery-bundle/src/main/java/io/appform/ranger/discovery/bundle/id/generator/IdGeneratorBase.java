package io.appform.ranger.discovery.bundle.id.generator;

import com.google.common.base.Preconditions;
import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeExecutor;
import dev.failsafe.RetryPolicy;
import io.appform.ranger.discovery.bundle.id.Domain;
import io.appform.ranger.discovery.bundle.id.GenerationResult;
import io.appform.ranger.discovery.bundle.id.Id;
import io.appform.ranger.discovery.bundle.id.IdValidationState;
import io.appform.ranger.discovery.bundle.id.constraints.IdValidationConstraint;
import io.appform.ranger.discovery.bundle.id.formatter.FormattedId;
import io.appform.ranger.discovery.bundle.id.formatter.IdFormatters;
import io.appform.ranger.discovery.bundle.id.nonce.NonceUtils;
import lombok.Getter;
import lombok.val;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Base Id Generator
 */
public class IdGeneratorBase {

    @Getter
    private int nodeId;
    private final List<IdValidationConstraint> globalConstraints = new ArrayList<>();
    @Getter
    private final Map<String, Domain> registeredDomains = new ConcurrentHashMap<>(Map.of(Domain.DEFAULT_DOMAIN_NAME, Domain.DEFAULT));
    @Getter
    private final FailsafeExecutor<GenerationResult> retryer;

    protected IdGeneratorBase() {
        val retryPolicy = RetryPolicy.<GenerationResult>builder()
                .withMaxAttempts(NonceUtils.readRetryCount())
                .handleIf(throwable -> true)
                .handleResultIf(Objects::isNull)
                .handleResultIf(generationResult -> generationResult.getState() == IdValidationState.INVALID_RETRYABLE)
                .onRetry(NonceUtils::retryEventListener)
                .build();
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
                .idFormatter(IdFormatters.original())
                .resolution(TimeUnit.MILLISECONDS)
                .build());
    }

    public final Id getIdFromIdInfo(final String id,
                                    final FormattedId formattedId) {
        return Id.builder()
                .id(id)
                .exponent(formattedId.getExponent())
                .time(formattedId.getTime())
                .generatedDate(formattedId.getDateTime().toDate())
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

    public final void setNodeId(int nodeId) {
        if (this.nodeId > 0) {
            throw new RuntimeException("Node ID already set");
        }
        this.nodeId = nodeId;
    }
}
