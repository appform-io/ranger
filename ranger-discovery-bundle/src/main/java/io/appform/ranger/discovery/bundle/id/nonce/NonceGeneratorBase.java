package io.appform.ranger.discovery.bundle.id.nonce;

import com.google.common.base.Preconditions;
import io.appform.ranger.discovery.bundle.id.Domain;
import io.appform.ranger.discovery.bundle.id.Id;
import io.appform.ranger.discovery.bundle.id.IdInfo;
import io.appform.ranger.discovery.bundle.id.constraints.IdValidationConstraint;
import io.appform.ranger.discovery.bundle.id.formatter.IdFormatter;
import io.appform.ranger.discovery.bundle.id.formatter.IdFormatters;
import io.appform.ranger.discovery.bundle.id.request.IdGenerationRequest;
import lombok.Getter;
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
    protected List<IdValidationConstraint> GLOBAL_CONSTRAINTS = new ArrayList<>();
    protected final Map<String, Domain> REGISTERED_DOMAINS = new ConcurrentHashMap<>(Map.of(Domain.DEFAULT_DOMAIN_NAME, Domain.DEFAULT));
    private final int nodeId;
    private final IdFormatter idFormatter;

    protected NonceGeneratorBase(final int nodeId, final IdFormatter idFormatter) {
        this.nodeId = nodeId;
        this.idFormatter = idFormatter;
    }

    public synchronized void cleanUp() {
        GLOBAL_CONSTRAINTS.clear();
        REGISTERED_DOMAINS.clear();
    }

    public void registerDomain(Domain domain) {
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

    public abstract Id getIdFromIdInfo(IdInfo idInfo, final String namespace, final IdFormatter idFormatter);

    public abstract DateTime getDateTimeFromTime(final long time);
}
