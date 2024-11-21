package io.appform.ranger.discovery.bundle.id.generator;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.appform.ranger.discovery.bundle.id.Domain;
import io.appform.ranger.discovery.bundle.id.Id;
import io.appform.ranger.discovery.bundle.id.constraints.IdValidationConstraint;
import io.appform.ranger.discovery.bundle.id.formatter.IdFormatter;
import io.appform.ranger.discovery.bundle.id.nonce.NonceGeneratorBase;
import io.appform.ranger.discovery.bundle.id.request.IdGenerationRequest;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.joda.time.format.DateTimeFormatter;

import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * Base Id Generator
 */
@SuppressWarnings("unused")
@Slf4j
public class IdGeneratorBase {

    @Getter
    private static int NODE_ID;
    private final int minimumIdLength;
    private final Pattern pattern;
    protected final DateTimeFormatter dateTimeFormatter;
    protected final NonceGeneratorBase nonceGenerator;

    public static void initialize(int node) {
        NODE_ID = node;
    }

    public IdGeneratorBase(final int minimumIdLength,
                           final DateTimeFormatter dateTimeFormatter,
                           final Pattern pattern,
                           final NonceGeneratorBase nonceGenerator) {
        this.nonceGenerator = nonceGenerator;
        this.minimumIdLength = minimumIdLength;
        this.dateTimeFormatter = dateTimeFormatter;
        this.pattern = pattern;
    }

    public final synchronized void cleanUp() {
        nonceGenerator.cleanUp();
    }

    public final void registerDomain(final Domain domain) {
        nonceGenerator.registerDomain(domain);
    }

    public final synchronized void registerGlobalConstraints(final IdValidationConstraint... constraints) {
        registerGlobalConstraints(ImmutableList.copyOf(constraints));
    }

    public final synchronized void registerGlobalConstraints(final List<IdValidationConstraint> constraints) {
        Preconditions.checkArgument(null != constraints && !constraints.isEmpty());
        nonceGenerator.registerGlobalConstraints(constraints);
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
        nonceGenerator.registerDomainSpecificConstraints(domain, validationConstraints);
    }

    /**
     * Generate id with given namespace
     *
     * @param namespace String namespace for ID to be generated
     * @return Generated Id
     */
    public final Id generate(final String namespace) {
        val idInfo = nonceGenerator.generate(namespace);
        return nonceGenerator.getIdFromIdInfo(idInfo, namespace);
    }

    public final Id generate(final String namespace, final IdFormatter idFormatter) {
        val idInfo = nonceGenerator.generate(namespace);
        return nonceGenerator.getIdFromIdInfo(idInfo, namespace, idFormatter);
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
        val registeredDomain = nonceGenerator.getRegisteredDomains().getOrDefault(domain, Domain.DEFAULT);
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
        val idInfoOptional = nonceGenerator.generateWithConstraints(request);
        return idInfoOptional.map(idInfo -> nonceGenerator.getIdFromIdInfo(idInfo, request.getPrefix(), request.getIdFormatter()));
    }

    /**
     * Generate id by parsing given string
     *
     * @param idString String idString
     * @return ID if it could be generated
     */
    public final Optional<Id> parse(final String idString) {
        if (idString == null
                || idString.length() < minimumIdLength) {
            return Optional.empty();
        }
        try {
            val matcher = pattern.matcher(idString);
            if (!matcher.find()) {
                return Optional.empty();
            }
            return Optional.of(Id.builder()
                    .id(idString)
                    .node(Integer.parseInt(matcher.group(3)))
                    .exponent(Integer.parseInt(matcher.group(4)))
                    .generatedDate(dateTimeFormatter.parseDateTime(matcher.group(2)).toDate())
                    .build());
        } catch (Exception e) {
            log.warn("Could not parse idString {}", e.getMessage());
            return Optional.empty();
        }
    }
}
