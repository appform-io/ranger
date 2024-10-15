package io.appform.ranger.discovery.bundle.id.nonce;

import com.google.common.base.Preconditions;
import io.appform.ranger.discovery.bundle.id.Domain;
import io.appform.ranger.discovery.bundle.id.Id;
import io.appform.ranger.discovery.bundle.id.IdInfo;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;


@Getter
public abstract class NonceGeneratorBase {

    private final SecureRandom SECURE_RANDOM = new SecureRandom(Long.toBinaryString(System.currentTimeMillis()).getBytes());
    private final List<IdValidationConstraint> GLOBAL_CONSTRAINTS = new ArrayList<>();
    private final Map<String, Domain> REGISTERED_DOMAINS = new ConcurrentHashMap<>(Map.of(Domain.DEFAULT_DOMAIN_NAME, Domain.DEFAULT));
    private final IdFormatter idFormatter;

    protected NonceGeneratorBase(final IdFormatter idFormatter) {
        this.idFormatter = idFormatter;
    }

    public synchronized void cleanUp() {
        GLOBAL_CONSTRAINTS.clear();
        REGISTERED_DOMAINS.clear();
    }

    public void registerDomain(final Domain domain) {
        REGISTERED_DOMAINS.put(domain.getDomain(), domain);
    }

    public synchronized void registerGlobalConstraints(final List<IdValidationConstraint> constraints) {
        Preconditions.checkArgument(null != constraints && !constraints.isEmpty());
        GLOBAL_CONSTRAINTS.addAll(constraints);
    }

    public synchronized void registerDomainSpecificConstraints(
            final String domain,
            final List<IdValidationConstraint> validationConstraints) {
        Preconditions.checkArgument(null != validationConstraints && !validationConstraints.isEmpty());
        REGISTERED_DOMAINS.computeIfAbsent(domain, key -> Domain.builder()
                .domain(domain)
                .constraints(validationConstraints)
                .idFormatter(IdFormatters.original())
                .resolution(TimeUnit.MILLISECONDS)
                .build());
    }

    protected IdValidationState validateId(final List<IdValidationConstraint> inConstraints, final Id id, final boolean skipGlobal) {
        // First evaluate global constraints
        val failedGlobalConstraint
                = skipGlobal
                ? null
                : getGLOBAL_CONSTRAINTS().stream()
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

    public Id getIdFromIdInfo(final IdInfo idInfo, final String namespace, final IdFormatter idFormatter) {
        val dateTime = getDateTimeFromTime(idInfo.getTime());
        val id = String.format("%s%s", namespace, idFormatter.format(dateTime, IdGeneratorBase.getNODE_ID(), idInfo.getExponent()));
        return Id.builder()
                .id(id)
                .exponent(idInfo.getExponent())
                .generatedDate(dateTime.toDate())
                .node(IdGeneratorBase.getNODE_ID())
                .build();
    }

    public Id getIdFromIdInfo(final IdInfo idInfo, final String namespace) {
        return getIdFromIdInfo(idInfo, namespace, idFormatter);
    }

    /**
     * Generate id with given namespace
     *
     * @param namespace String namespace for ID to be generated
     * @return Generated Id
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
    public abstract Optional<IdInfo> generateWithConstraints(final String namespace, final String domain, final boolean skipGlobal);

    public abstract Optional<IdInfo> generateWithConstraints(final IdGenerationRequest request);

    public abstract Optional<IdInfo> generateWithConstraints(final String namespace,
                                                             final List<IdValidationConstraint> inConstraints,
                                                             final boolean skipGlobal);

    public abstract IdInfo generateForPartition(final String namespace, final int targetPartitionId) ;
}
