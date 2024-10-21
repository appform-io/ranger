package io.appform.ranger.discovery.bundle.id.nonce;

import com.google.common.base.Preconditions;
import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeExecutor;
import dev.failsafe.RetryPolicy;
import dev.failsafe.event.ExecutionAttemptedEvent;
import io.appform.ranger.discovery.bundle.id.Domain;
import io.appform.ranger.discovery.bundle.id.GenerationResult;
import io.appform.ranger.discovery.bundle.id.Id;
import io.appform.ranger.discovery.bundle.id.IdInfo;
import io.appform.ranger.discovery.bundle.id.IdUtils;
import io.appform.ranger.discovery.bundle.id.IdValidationState;
import io.appform.ranger.discovery.bundle.id.constraints.IdValidationConstraint;
import io.appform.ranger.discovery.bundle.id.formatter.IdFormatter;
import io.appform.ranger.discovery.bundle.id.formatter.IdFormatters;
import io.appform.ranger.discovery.bundle.id.generator.IdGeneratorBase;
import io.appform.ranger.discovery.bundle.id.request.IdGenerationRequest;
import lombok.Getter;
import lombok.val;
import org.joda.time.DateTime;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;


@Getter
public abstract class NonceGeneratorBase {

    private final SecureRandom secureRandom = new SecureRandom(Long.toBinaryString(System.currentTimeMillis()).getBytes());
    private final List<IdValidationConstraint> globalConstraints = new ArrayList<>();
    private final Map<String, Domain> registeredDomains = new ConcurrentHashMap<>(Map.of(Domain.DEFAULT_DOMAIN_NAME, Domain.DEFAULT));
    private final IdFormatter idFormatter;
    private final FailsafeExecutor<GenerationResult> retryer;

    protected NonceGeneratorBase(final IdFormatter idFormatter) {
        this.idFormatter = idFormatter;
        val retryPolicy = RetryPolicy.<GenerationResult>builder()
                .withMaxAttempts(IdUtils.readRetryCountFromEnv())
                .handleIf(throwable -> true)
                .handleResultIf(Objects::isNull)
                .handleResultIf(generationResult -> generationResult.getState() == IdValidationState.INVALID_RETRYABLE)
                .onRetry(this::retryEventListener)
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

    protected final IdValidationState validateId(final List<IdValidationConstraint> inConstraints, final Id id, final boolean skipGlobal) {
        // First evaluate global constraints
        val failedGlobalConstraint
                = skipGlobal
                ? null
                : getGlobalConstraints().stream()
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

    public final Id getIdFromIdInfo(final IdInfo idInfo, final String namespace, final IdFormatter idFormatter) {
        val dateTime = new DateTime(idInfo.getTime());
        val id = String.format("%s%s", namespace, idFormatter.format(dateTime, IdGeneratorBase.getNODE_ID(), idInfo.getExponent()));
        return Id.builder()
                .id(id)
                .exponent(idInfo.getExponent())
                .generatedDate(dateTime.toDate())
                .node(IdGeneratorBase.getNODE_ID())
                .build();
    }

    public final Id getIdFromIdInfo(final IdInfo idInfo, final String namespace) {
        return getIdFromIdInfo(idInfo, namespace, idFormatter);
    }

    /**
     * Generate id with given namespace
     *
     * @param namespace String namespace for ID to be generated
     * @return Generated IdInfo
     */
    public abstract IdInfo generate(final String namespace);

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
    public final Optional<IdInfo> generateWithConstraints(final String namespace, final String domain, final boolean skipGlobal) {
        val registeredDomain = getRegisteredDomains().getOrDefault(domain, Domain.DEFAULT);
        return generateWithConstraints(IdGenerationRequest.builder()
                .prefix(namespace)
                .constraints(registeredDomain.getConstraints())
                .skipGlobal(skipGlobal)
                .domain(registeredDomain.getDomain())
                .idFormatter(registeredDomain.getIdFormatter())
                .build());
    }

    public abstract Optional<IdInfo> generateWithConstraints(final IdGenerationRequest request);

    public abstract IdInfo generateForPartition(final String namespace, final int targetPartitionId) ;

    protected abstract void retryEventListener(ExecutionAttemptedEvent<GenerationResult> event);

}
